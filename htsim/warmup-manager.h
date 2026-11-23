/*
 * Executes warm-up transfers.
 *
 * Asks the active WarmupPolicy where a node's weights should come from, then
 * opens one real TCP flow per PullTask. Those flows traverse the same queues,
 * drop in the same buffers and back off on the same ECN marks as user
 * traffic, so warm-up time is produced by the network rather than declared
 * by a parameter.
 *
 * A node is handed back to the fleet as ACTIVE only when every byte of the
 * model state has landed.
 *
 * It subclasses FlowGenerator purely to intercept flow completion -- htsim
 * reports a finished flow to its DataSource's FlowGenerator. It is never
 * scheduled for flow generation (setTimeLimits is never called), so the base
 * class's arrival process stays dormant; doNextEvent is overridden to retry
 * nodes whose policy declined to plan.
 */
#ifndef WARMUP_MANAGER_H
#define WARMUP_MANAGER_H

#include "flow-generator.h"
#include "gpu-fleet.h"
#include "warmup-policy.h"

#include <functional>
#include <map>
#include <vector>

/* Builds the forward (data) and reverse (ACK) routes for one pull.
 * Implemented by the testbed, which owns the topology. */
typedef std::function<void(const PullTask &task, uint32_t dstNode,
                           route_t *&fwd, route_t *&rev)> warmup_route_gen_t;

class WarmupManager : public FlowGenerator, public WarmupStarter
{
    public:
        WarmupManager(GpuFleet *fleet, WarmupPolicy *policy,
                      warmup_route_gen_t routeGen, DataSource::EndHost endhost);

        /* Retry interval for nodes whose policy declined to plan. */
        void setRetryInterval(simtime_picosec t);

        /* ---- WarmupStarter ---- */
        void startWarmup(uint32_t node);
        void abortWarmup(uint32_t node);

        /* ---- FlowGenerator ---- */
        void finishFlow(uint32_t flow_id);   // a warm-up transfer completed
        void doNextEvent();                  // retry pass

        /* ---- Statistics ---- */
        struct Record {
            uint32_t node;
            uint32_t rack;
            simtime_picosec started;
            simtime_picosec finished;   // 0 if still in flight
            uint64_t bytesFromStorage;
            uint64_t bytesFromLocalPeer;
            uint64_t bytesFromRemotePeer;
            uint32_t tasks;
        };

        const std::vector<Record>& records() const { return _records; }
        uint64_t storageBytes() const { return _storageBytes; }
        uint64_t localPeerBytes() const { return _localPeerBytes; }
        uint64_t remotePeerBytes() const { return _remotePeerBytes; }
        uint32_t completed() const { return _completed; }

        /* Warm-up durations in picoseconds, for completed nodes only. */
        std::vector<simtime_picosec> durations() const;

    private:
        void issue(uint32_t node, const std::vector<PullTask> &tasks);
        void startTask(uint32_t node, const PullTask &task);

        struct InFlight {
            uint32_t node;
            PullTask task;
        };

        /* Per-node progress while WARMING. */
        struct Progress {
            uint64_t bytesDone;
            uint32_t outstanding;
            size_t recordIdx;
        };

        GpuFleet *_fleet;
        WarmupPolicy *_policy;
        warmup_route_gen_t _routeGen;
        DataSource::EndHost _endhost;

        std::map<uint32_t, InFlight> _inFlight;   // flow id -> what it is
        std::map<uint32_t, Progress> _progress;   // node    -> how far along
        std::vector<uint32_t> _unplanned;         // policy declined; retry
        std::vector<Record> _records;

        uint64_t _storageBytes, _localPeerBytes, _remotePeerBytes;
        uint32_t _completed;
        uint32_t _flowSeq;
        simtime_picosec _retry;
        bool _retryScheduled;
};

#endif /* WARMUP_MANAGER_H */
