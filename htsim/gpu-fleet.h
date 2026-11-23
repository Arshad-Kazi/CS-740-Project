/*
 * Elastic fleet of GPU inference nodes.
 *
 * A node can serve only once the model weights are resident. Getting them
 * there is a network transfer, not a timer, so "boot time" is a measured
 * outcome that depends on where the bytes came from and what else was using
 * the network at the time.
 *
 * Node states:
 *   OFF      allocated in the topology but idle; holds no weights
 *   WARMING  weights are in transit; consuming network, serving nothing
 *   ACTIVE   weights resident; in the round-robin dispatch set
 *
 * A node that is scaled down returns to OFF and *loses* its weights, which
 * is the conservative assumption: every scale-up pays the full warm-up cost.
 * (Retaining them would model a keep-warm pool, a separate experiment.)
 *
 * Capacity is the node's compute queue, not its link -- GPU serving
 * throughput is below NIC line rate, so compute is what saturates and what
 * the autoscaler measures.
 */
#ifndef GPU_FLEET_H
#define GPU_FLEET_H

#include "eventlist.h"
#include "dre-queue.h"
#include "elastic-fleet.h"
#include "warmup-policy.h"

#include <string>
#include <vector>

/* Implemented by WarmupManager; lets the fleet ask for a transfer to begin
 * without knowing how transfers work. */
class WarmupStarter
{
    public:
        virtual ~WarmupStarter() {}
        virtual void startWarmup(uint32_t node) = 0;
        virtual void abortWarmup(uint32_t node) = 0;
};

class GpuFleet : public ElasticFleet, public FleetView
{
    public:
        enum State { OFF, WARMING, ACTIVE };
        enum Placement {
            PACKED,      // fill a rack before moving to the next
            ROUNDROBIN   // spread across racks
        };

        GpuFleet(uint32_t nRacks, uint32_t nodesPerRack, uint32_t initialActive,
                 uint64_t stateBytes, uint32_t nStorageNodes, Placement placement);

        static bool parsePlacement(const std::string &s, Placement &out);

        void setWarmupStarter(WarmupStarter *s) { _starter = s; }

        /* Compute queue of node i: its capacity, and the load signal. */
        void setComputeQueue(uint32_t i, DreQueue *q);

        /* ---- ElasticFleet ---- */
        double meanLoad();
        double maxLoad();
        uint32_t nActive() const { return (uint32_t)_active.size(); }
        uint32_t nPending() const { return _nWarming; }
        uint32_t scaleUp(uint32_t n);
        uint32_t scaleDown(uint32_t n);

        /* ---- FleetView (what a policy is allowed to see) ---- */
        uint32_t nNodes() const { return _maxNodes; }
        uint32_t nRacks() const { return _nRacks; }
        uint32_t nStorageNodes() const { return _nStorage; }
        uint64_t stateBytes() const { return _stateBytes; }
        bool hasWeights(uint32_t node) const { return _state[node] == ACTIVE; }
        uint32_t rackOf(uint32_t node) const { return _rackOf[node]; }
        double loadOf(uint32_t node);
        uint32_t donationsInFlight(uint32_t node) const { return _donations[node]; }
        const std::vector<uint32_t>& residentNodes() const { return _active; }

        /* ---- Topology helpers ---- */
        uint32_t slotOf(uint32_t node) const { return _slotOf[node]; }
        State stateOf(uint32_t node) const { return _state[node]; }

        /* ---- Called by WarmupManager ---- */
        void onWarmupComplete(uint32_t node);
        void noteDonationStart(uint32_t node);
        void noteDonationEnd(uint32_t node);

        /* ---- Round-robin dispatch over ACTIVE nodes ---- */
        int select();

        /* Node-seconds of serving capacity consumed (accrues when read). */
        double nodeSeconds();

        /* Nodes still WARMING, i.e. capacity ordered that never arrived. */
        uint32_t nStuck() const { return _nWarming; }

        uint64_t _dispatches;
        uint64_t _starts;
        uint64_t _stops;

    private:
        void accrue();

        uint32_t _maxNodes, _nRacks, _nodesPerRack, _nStorage;
        uint64_t _stateBytes;
        Placement _placement;
        WarmupStarter *_starter;

        std::vector<State> _state;
        std::vector<uint32_t> _rackOf, _slotOf;
        std::vector<DreQueue*> _compute;
        std::vector<uint32_t> _donations;   // transfers each node is sourcing

        std::vector<uint32_t> _active;      // dispatchable, and weight donors
        uint32_t _nWarming;
        uint32_t _rrNext;

        uint64_t _nodeTimePs;
        simtime_picosec _lastAccrual;
};

#endif /* GPU_FLEET_H */
