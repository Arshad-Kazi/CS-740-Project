/*
 * Warm-up source-selection policies.
 *
 * Warming a GPU node means getting `stateBytes` of model weights onto it
 * before it can serve. Where those bytes come from is a scheduling decision,
 * and it is the decision this project exists to study. Policies are therefore
 * pluggable: the simulator executes whatever transfers a policy asks for and
 * never needs to know which strategy is running.
 *
 * A policy returns PullTasks -- (source, byte range) pairs. The manager opens
 * one TCP flow per task, and the node becomes ready when every byte of the
 * state has arrived. onTaskComplete lets adaptive policies re-plan as
 * transfers finish, which is what chunked and tree-structured strategies need.
 *
 * Three source tiers, with genuinely different costs:
 *
 *   central storage   slow (read-rate limited), but costs the serving
 *                     fleet nothing
 *   same-rack peer    fast (NVLink-class interconnect) and nearly free,
 *                     but only if some node in that rack already has
 *                     the weights
 *   cross-rack peer   fast-ish, but crosses the normal fabric and consumes
 *                     the donor's egress NIC, competing with its serving
 *                     traffic
 *
 * Which is why "pull from a peer" is not obviously the right answer.
 */
#ifndef WARMUP_POLICY_H
#define WARMUP_POLICY_H

#include <cstdint>
#include <string>
#include <vector>

/* One transfer: `length` bytes starting at `offset`, from `source`. */
struct PullTask
{
    static const int STORAGE = -1;   // central storage rather than a peer

    int source;          // STORAGE, or a node id
    uint64_t offset;     // byte offset into the model state
    uint64_t length;     // bytes to move

    PullTask() : source(STORAGE), offset(0), length(0) {}
    PullTask(int src, uint64_t off, uint64_t len)
        : source(src), offset(off), length(len) {}

    bool fromStorage() const { return source == STORAGE; }
};

/*
 * Read-only view of the fleet, handed to a policy when it plans. Implemented
 * by GpuFleet. Policies see only what a real scheduler could see.
 */
class FleetView
{
    public:
        virtual ~FleetView() {}

        virtual uint32_t nNodes() const = 0;
        virtual uint32_t nRacks() const = 0;
        virtual uint32_t nStorageNodes() const = 0;
        virtual uint64_t stateBytes() const = 0;

        /* Does this node hold a complete copy of the weights? */
        virtual bool hasWeights(uint32_t node) const = 0;

        /* Which rack a node sits in. Same-rack peers share the fast path. */
        virtual uint32_t rackOf(uint32_t node) const = 0;

        /* Serving load of a node, 0 .. ~1. Pulling from a busy node taxes
         * its egress link, so a load-aware policy should consult this. */
        virtual double loadOf(uint32_t node) = 0;

        /* How many warm-up transfers this node is currently sourcing.
         * Lets a policy spread donation rather than hammering one peer. */
        virtual uint32_t donationsInFlight(uint32_t node) const = 0;

        /* Every node that currently holds the weights. */
        virtual const std::vector<uint32_t>& residentNodes() const = 0;
};

class WarmupPolicy
{
    public:
        virtual ~WarmupPolicy() {}

        virtual const char *name() const = 0;

        /* Transfers to start in order to warm `node`. Returning an empty
         * vector means "cannot warm this node right now"; the manager will
         * ask again at the next control tick. */
        virtual std::vector<PullTask> plan(uint32_t node, FleetView &view) = 0;

        /* One of this node's transfers finished. Return any follow-up
         * transfers. The default (no follow-up) suits single-shot policies. */
        virtual std::vector<PullTask> onTaskComplete(uint32_t node,
                                                     const PullTask &done,
                                                     FleetView &view)
        {
            (void)node; (void)done; (void)view;
            return std::vector<PullTask>();
        }
};

/*
 * Baseline: every node pulls the whole state from central storage.
 * Storage bandwidth is shared, so k concurrent warm-ups each take k times
 * as long -- scale-up latency grows with scale-up size.
 */
class StorageOnlyPolicy : public WarmupPolicy
{
    public:
        const char *name() const { return "storage"; }

        std::vector<PullTask> plan(uint32_t node, FleetView &view)
        {
            (void)node;
            std::vector<PullTask> tasks;
            tasks.push_back(PullTask(PullTask::STORAGE, 0, view.stateBytes()));
            return tasks;
        }
};

/*
 * Peer-assisted: prefer a donor in the same rack (fast interconnect, and it
 * does not touch the fabric at all), then the least-loaded donor elsewhere,
 * and fall back to storage only when nobody holds the weights yet.
 *
 * The fallback matters more than it looks: at the start of a surge *no* node
 * has the weights, so the first pull in each rack must come from storage.
 * Peer distribution is an epidemic, and seeding is its rate limit.
 */
class NearestPeerPolicy : public WarmupPolicy
{
    public:
        NearestPeerPolicy(bool allowCrossRack = true)
            : _allowCrossRack(allowCrossRack) {}

        const char *name() const
        {
            return _allowCrossRack ? "peer" : "peer-racklocal";
        }

        std::vector<PullTask> plan(uint32_t node, FleetView &view)
        {
            std::vector<PullTask> tasks;
            int donor = pickDonor(node, view);
            tasks.push_back(PullTask(donor, 0, view.stateBytes()));
            return tasks;
        }

    protected:
        /* Returns a node id, or PullTask::STORAGE if no peer is suitable. */
        int pickDonor(uint32_t node, FleetView &view)
        {
            const std::vector<uint32_t> &resident = view.residentNodes();
            uint32_t myRack = view.rackOf(node);

            int bestLocal = PullTask::STORAGE, bestRemote = PullTask::STORAGE;
            double bestLocalCost = 1e9, bestRemoteCost = 1e9;

            for (size_t i = 0; i < resident.size(); i++) {
                uint32_t d = resident[i];
                if (d == node) {
                    continue;
                }
                /* Prefer idle donors: serving load plus transfers already
                 * being sourced, since both consume the donor's egress. */
                double cost = view.loadOf(d) + view.donationsInFlight(d);

                if (view.rackOf(d) == myRack) {
                    if (cost < bestLocalCost) {
                        bestLocalCost = cost;
                        bestLocal = (int)d;
                    }
                } else if (cost < bestRemoteCost) {
                    bestRemoteCost = cost;
                    bestRemote = (int)d;
                }
            }

            if (bestLocal != PullTask::STORAGE) {
                return bestLocal;   // same rack: fast path, no fabric cost
            }
            if (_allowCrossRack && bestRemote != PullTask::STORAGE) {
                return bestRemote;
            }
            return PullTask::STORAGE;   // nobody has it yet: seed from storage
        }

    private:
        bool _allowCrossRack;
};

inline WarmupPolicy *makeWarmupPolicy(const std::string &name)
{
    if (name == "storage")        return new StorageOnlyPolicy();
    if (name == "peer")           return new NearestPeerPolicy(true);
    if (name == "peer-racklocal") return new NearestPeerPolicy(false);
    return NULL;
}

#endif /* WARMUP_POLICY_H */
