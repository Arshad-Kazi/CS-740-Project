/*
 * Elastic server pool with model weight-transfer autoscaling.
 *
 * Instead of modelling boot delay as a fixed timer, when scale-up is triggered:
 * 1. Chooses k active nodes that have available compute capacity from the pool.
 * 2. Uses a Weighted Round Robin scheme to request chunks of model parameter
 *    data from each chosen node based on its available capacity score
 *    (score = 1.0 - utilization).
 * 3. For each sending node N that sends x chunks, distributes those chunks
 *    across fabric core switches using a Weighted Round Robin scheme based on
 *    the CSIG congestion metric (available bandwidth) of the leaf->core uplinks.
 * 4. The booting node becomes ACTIVE only once all parameter chunks have
 *    finished transferring over the network.
 */
#ifndef WEIGHT_TRANSFER_POOL_H
#define WEIGHT_TRANSFER_POOL_H

#include "eventlist.h"
#include "dre-queue.h"
#include "csig-queue.h"
#include "elastic-fleet.h"
#include "flow-generator.h"
#include "server-pool.h"
#include "tcp.h"

#include <vector>
#include <map>
#include <random>
#include <functional>
#include <string>

// Route generator for transfer flows:
// given (srcBackend, dstBackend, core), returns forward and reverse route_t*
typedef std::function<void(uint32_t srcNode, uint32_t dstNode, uint32_t core,
                           route_t *&fwd, route_t *&rev)> transfer_route_gen_t;

// CSIG metric query: returns current available bandwidth (bps) on uplink (srcLeaf -> core)
typedef std::function<uint64_t(uint32_t srcLeaf, uint32_t core)> csig_metric_fn_t;

class WeightTransferPool : public EventSource, public ElasticFleet
{
    public:
        enum State { OFF, BOOTING, ACTIVE };

        WeightTransferPool(uint32_t maxServers,
                           uint32_t initialActive,
                           uint32_t nLeaf,
                           uint32_t nCore,
                           uint64_t modelBytes,
                           double modelFraction,
                           uint32_t transferK,
                           uint64_t chunkSize,
                           DataSource::EndHost endhost,
                           transfer_route_gen_t routeGen,
                           csig_metric_fn_t csigMetricFn);
        virtual ~WeightTransferPool();

        void setPolicy(ServerPool::LbPolicy p) { _policy = p; }
        void setEgressQueue(uint32_t i, DreQueue *q);

        /* ---- ElasticFleet Interface ---- */
        virtual double meanLoad();
        virtual double maxLoad();
        virtual uint32_t nActive() const { return (uint32_t)_active.size(); }
        virtual uint32_t nPending() const { return _nBooting; }
        virtual uint32_t scaleUp(uint32_t n);
        virtual uint32_t scaleDown(uint32_t n);

        /* ---- EventSource ---- */
        virtual void doNextEvent();

        /* Pick a server for a new user request */
        int select(std::mt19937 &rng);

        /* Current load of server i */
        double load(uint32_t i);

        /* Server-seconds consumed over the run */
        double serverSeconds();
        uint32_t maxServers() const { return _maxServers; }

        /* Notification from helper FlowGenerator when a chunk transfer completes */
        void onFlowFinished(uint32_t flowId);

        /* Statistics */
        uint64_t _dispatches;
        uint64_t _starts;
        uint64_t _stops;
        uint64_t _chunksTransferred;

    private:
        void accrueServerTime();
        void startTransfersForNode(uint32_t dstNode);
        inline uint32_t backendLeaf(uint32_t b) const { return b % _nLeaf; }

        uint32_t _maxServers;
        uint32_t _nLeaf;
        uint32_t _nCore;
        uint64_t _modelBytes;
        double _modelFraction;
        uint64_t _transferBytes;
        uint32_t _transferK;
        uint64_t _chunkSize;
        DataSource::EndHost _endhost;
        transfer_route_gen_t _routeGen;
        csig_metric_fn_t _csigMetricFn;

        ServerPool::LbPolicy _policy;
        std::vector<State> _state;
        std::vector<DreQueue*> _egress;
        std::vector<uint32_t> _active;
        uint32_t _nBooting;
        uint32_t _rrNext;

        uint64_t _serverTimePs;
        simtime_picosec _lastAccrual;

        struct BootingRecord {
            uint32_t chunksRemaining;
            simtime_picosec startTime;
        };
        std::map<uint32_t, BootingRecord> _booting; // dstNode -> record

        struct ChunkInfo {
            uint32_t dstNode;
            uint32_t srcNode;
            uint32_t core;
            uint64_t size;
        };
        std::map<uint32_t, ChunkInfo> _inFlightChunks; // flow_id -> ChunkInfo

        // Helper FlowGenerator to capture finishFlow callbacks from TcpSrc
        class TransferFlowGenerator : public FlowGenerator
        {
            public:
                TransferFlowGenerator(WeightTransferPool *pool, DataSource::EndHost eh);
                virtual void finishFlow(uint32_t flow_id);
            private:
                WeightTransferPool *_pool;
        };

        TransferFlowGenerator *_flowGen;
        uint32_t _flowSeq;
};

/* Smooth Weighted Round Robin helper */
std::vector<uint32_t> weightedRoundRobinSchedule(uint32_t numItems,
                                                const std::vector<double> &weights);

#endif /* WEIGHT_TRANSFER_POOL_H */
