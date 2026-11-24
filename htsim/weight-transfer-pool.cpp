/*
 * Elastic server pool with model weight-transfer autoscaling.
 */
#include "weight-transfer-pool.h"

#include <algorithm>
#include <iostream>

using namespace std;

namespace {
    void nullRouteGen(route_t *&fwd, route_t *&rev, uint32_t &src, uint32_t &dst)
    {
        fwd = NULL; rev = NULL; src = 0; dst = 0;
    }
}

/*
 * Smooth Weighted Round Robin (Nginx / WRR algorithm).
 * Given N items and bucket weights, returns an assignment vector of size N
 * where element i is the chosen bucket index for item i.
 */
vector<uint32_t>
weightedRoundRobinSchedule(uint32_t numItems, const vector<double> &weights)
{
    vector<uint32_t> schedule;
    schedule.reserve(numItems);
    if (weights.empty() || numItems == 0) {
        return schedule;
    }

    uint32_t n = weights.size();
    vector<double> current = weights;
    double sumWeight = 0.0;
    for (double w : weights) {
        sumWeight += (w > 0.0 ? w : 1e-6);
    }

    for (uint32_t i = 0; i < numItems; i++) {
        uint32_t best = 0;
        double maxVal = current[0];
        for (uint32_t b = 1; b < n; b++) {
            if (current[b] > maxVal) {
                maxVal = current[b];
                best = b;
            }
        }
        schedule.push_back(best);
        current[best] -= sumWeight;
        for (uint32_t b = 0; b < n; b++) {
            current[b] += (weights[b] > 0.0 ? weights[b] : 1e-6);
        }
    }
    return schedule;
}

WeightTransferPool::TransferFlowGenerator::TransferFlowGenerator(WeightTransferPool *pool,
                                                                 DataSource::EndHost eh)
    : FlowGenerator(eh, nullRouteGen, speedFromGbps(1), 100000, Workloads::UNIFORM),
      _pool(pool)
{
    setPrefix("wt-");
}

void
WeightTransferPool::TransferFlowGenerator::finishFlow(uint32_t flow_id)
{
    _pool->onFlowFinished(flow_id);
}

WeightTransferPool::WeightTransferPool(uint32_t maxServers,
                                       uint32_t initialActive,
                                       uint32_t nLeaf,
                                       uint32_t nCore,
                                       uint64_t modelBytes,
                                       double modelFraction,
                                       uint32_t transferK,
                                       uint64_t chunkSize,
                                       DataSource::EndHost endhost,
                                       transfer_route_gen_t routeGen,
                                       csig_metric_fn_t csigMetricFn)
    : EventSource("WeightTransferPool"),
      _dispatches(0),
      _starts(0),
      _stops(0),
      _chunksTransferred(0),
      _maxServers(maxServers),
      _nLeaf(nLeaf),
      _nCore(nCore),
      _modelBytes(modelBytes),
      _modelFraction(modelFraction),
      _transferBytes((uint64_t)(modelBytes * modelFraction)),
      _transferK(transferK > 0 ? transferK : 1),
      _chunkSize(chunkSize > 0 ? chunkSize : 100000),
      _endhost(endhost),
      _routeGen(routeGen),
      _csigMetricFn(csigMetricFn),
      _policy(ServerPool::ROUND_ROBIN),
      _state(maxServers, OFF),
      _egress(maxServers, NULL),
      _nBooting(0),
      _rrNext(0),
      _serverTimePs(0),
      _lastAccrual(0),
      _flowSeq(0)
{
    _flowGen = new TransferFlowGenerator(this, _endhost);

    /* Initial active servers start immediately without transfer delay. */
    uint32_t n = min(initialActive, maxServers);
    for (uint32_t i = 0; i < n; i++) {
        _state[i] = ACTIVE;
        _active.push_back(i);
    }
    _starts = n;
}

WeightTransferPool::~WeightTransferPool()
{
    delete _flowGen;
}

void
WeightTransferPool::setEgressQueue(uint32_t i, DreQueue *q)
{
    if (i < _maxServers) {
        _egress[i] = q;
    }
}

void
WeightTransferPool::accrueServerTime()
{
    simtime_picosec now = EventList::Get().now();
    _serverTimePs += (uint64_t)_active.size() * (now - _lastAccrual);
    _lastAccrual = now;
}

void
WeightTransferPool::doNextEvent()
{
    accrueServerTime();
}

uint32_t
WeightTransferPool::scaleUp(uint32_t n)
{
    accrueServerTime();
    uint32_t started = 0;

    for (uint32_t i = 0; i < _maxServers && started < n; i++) {
        if (_state[i] != OFF) {
            continue;
        }
        _state[i] = BOOTING;
        _nBooting++;
        started++;

        startTransfersForNode(i);
    }

    _starts += started;
    return started;
}

uint32_t
WeightTransferPool::scaleDown(uint32_t n)
{
    accrueServerTime();
    uint32_t stopped = 0;

    while (stopped < n && _active.size() > 1) {
        uint32_t victim = _active.back();
        _active.pop_back();
        _state[victim] = OFF;
        stopped++;
    }

    _stops += stopped;
    return stopped;
}

void
WeightTransferPool::startTransfersForNode(uint32_t dstNode)
{
    if (_transferBytes == 0 || _active.empty()) {
        /* No bytes to transfer or no active peers available: become active immediately */
        _state[dstNode] = ACTIVE;
        _active.push_back(dstNode);
        _nBooting--;
        return;
    }

    /* 1. Calculate chunks and chunk sizes */
    uint32_t totalChunks = (uint32_t)((_transferBytes + _chunkSize - 1) / _chunkSize);
    if (totalChunks == 0) totalChunks = 1;

    vector<uint64_t> chunkSizes(totalChunks);
    uint64_t remainingBytes = _transferBytes;
    for (uint32_t c = 0; c < totalChunks; c++) {
        uint64_t sz = min(_chunkSize, remainingBytes);
        chunkSizes[c] = sz;
        remainingBytes -= sz;
    }

    /* 2. Choose up to k active nodes that have available compute capacity */
    struct Candidate {
        uint32_t nodeId;
        double capScore;
    };
    vector<Candidate> candidates;

    for (uint32_t u : _active) {
        if (u == dstNode) continue;
        double uLoad = load(u);
        double cap = max(0.001, 1.0 - uLoad);
        candidates.push_back({u, cap});
    }

    if (candidates.empty()) {
        _state[dstNode] = ACTIVE;
        _active.push_back(dstNode);
        _nBooting--;
        return;
    }

    // Sort descending by available capacity score
    sort(candidates.begin(), candidates.end(), [](const Candidate &a, const Candidate &b) {
        return a.capScore > b.capScore;
    });

    uint32_t m = min(_transferK, (uint32_t)candidates.size());
    candidates.resize(m);

    /* 3. Weighted Round Robin: distribute totalChunks across the m chosen nodes based on capacity score */
    vector<double> nodeWeights(m);
    for (uint32_t i = 0; i < m; i++) {
        nodeWeights[i] = candidates[i].capScore;
    }
    vector<uint32_t> nodeSchedule = weightedRoundRobinSchedule(totalChunks, nodeWeights);

    // Group chunk sizes per donor node
    vector<vector<uint64_t> > donorChunks(m);
    for (uint32_t c = 0; c < totalChunks; c++) {
        uint32_t donorIdx = nodeSchedule[c];
        donorChunks[donorIdx].push_back(chunkSizes[c]);
    }

    /* 4. For each sending node N with x chunks, distribute chunks across cores using WRR based on CSIG ABW */
    uint32_t dstLeaf = backendLeaf(dstNode);
    uint32_t actualFlowsLaunched = 0;

    for (uint32_t i = 0; i < m; i++) {
        uint32_t x = donorChunks[i].size();
        if (x == 0) continue;

        uint32_t srcNode = candidates[i].nodeId;
        uint32_t srcLeaf = backendLeaf(srcNode);

        vector<uint32_t> coreSchedule;
        if (srcLeaf == dstLeaf) {
            // Same rack: local switching
            coreSchedule.assign(x, 0);
        } else {
            // Query CSIG available bandwidth on uplinks to cores
            vector<double> coreWeights(_nCore, 1.0);
            for (uint32_t c = 0; c < _nCore; c++) {
                uint64_t abw = _csigMetricFn ? _csigMetricFn(srcLeaf, c) : 0;
                coreWeights[c] = (abw > 0) ? (double)abw : 1.0;
            }
            coreSchedule = weightedRoundRobinSchedule(x, coreWeights);
        }

        // Launch chunk flows
        for (uint32_t j = 0; j < x; j++) {
            uint32_t core = coreSchedule[j];
            uint64_t sz = donorChunks[i][j];

            route_t *fwd = NULL, *rev = NULL;
            _routeGen(srcNode, dstNode, core, fwd, rev);
            if (!fwd || !rev) {
                continue;
            }

            TcpSrc *src = new TcpSrc(NULL, NULL, (uint32_t)sz);
            TcpSink *snk = new TcpSink();
            if (_endhost == DataSource::DCTCP) {
                TcpSrc::_enable_dctcp = true;
            }

            src->setName("transfer-src" + to_string(_flowSeq));
            snk->setName("transfer-snk" + to_string(_flowSeq));
            _flowSeq++;

            src->_node_id = srcNode;
            snk->_node_id = dstNode;

            fwd->push_back(snk);
            rev->push_back(src);

            src->connect(EventList::Get().now() + 1, *fwd, *rev, *snk);
            src->setFlowGenerator(_flowGen);

            ChunkInfo ci;
            ci.dstNode = dstNode;
            ci.srcNode = srcNode;
            ci.core = core;
            ci.size = sz;
            _inFlightChunks[src->id] = ci;
            actualFlowsLaunched++;
        }
    }

    if (actualFlowsLaunched == 0) {
        _state[dstNode] = ACTIVE;
        _active.push_back(dstNode);
        _nBooting--;
    } else {
        _booting[dstNode] = {actualFlowsLaunched, EventList::Get().now()};
    }
}

void
WeightTransferPool::onFlowFinished(uint32_t flowId)
{
    map<uint32_t, ChunkInfo>::iterator it = _inFlightChunks.find(flowId);
    if (it == _inFlightChunks.end()) {
        return;
    }

    uint32_t dstNode = it->second.dstNode;
    _inFlightChunks.erase(it);
    _chunksTransferred++;

    map<uint32_t, BootingRecord>::iterator bIt = _booting.find(dstNode);
    if (bIt == _booting.end()) {
        return;
    }

    bIt->second.chunksRemaining--;
    if (bIt->second.chunksRemaining == 0) {
        /* All chunks received! Promote node to ACTIVE */
        accrueServerTime();
        _state[dstNode] = ACTIVE;
        _active.push_back(dstNode);
        _nBooting--;
        _booting.erase(bIt);
    }
}

double
WeightTransferPool::load(uint32_t i)
{
    if (i >= _maxServers || _egress[i] == NULL) {
        return 0.0;
    }
    return _egress[i]->utilization();
}

double
WeightTransferPool::meanLoad()
{
    if (_active.empty()) {
        return 0.0;
    }
    double sum = 0.0;
    for (size_t k = 0; k < _active.size(); k++) {
        sum += load(_active[k]);
    }
    return sum / _active.size();
}

double
WeightTransferPool::maxLoad()
{
    double mx = 0.0;
    for (size_t k = 0; k < _active.size(); k++) {
        mx = max(mx, load(_active[k]));
    }
    return mx;
}

double
WeightTransferPool::serverSeconds()
{
    accrueServerTime();
    return timeAsSec(_serverTimePs);
}

int
WeightTransferPool::select(mt19937 &rng)
{
    if (_active.empty()) {
        return -1;
    }

    _dispatches++;
    size_t n = _active.size();

    switch (_policy) {
        case ServerPool::RANDOM:
            return (int)_active[rng() % n];

        case ServerPool::LEAST_LOADED: {
            size_t best = 0;
            double bestLoad = load(_active[0]);
            for (size_t k = 1; k < n; k++) {
                double l = load(_active[k]);
                if (l < bestLoad) {
                    bestLoad = l;
                    best = k;
                }
            }
            return (int)_active[best];
        }

        case ServerPool::P2C: {
            size_t a = rng() % n;
            size_t b = rng() % n;
            return (int)_active[load(_active[a]) <= load(_active[b]) ? a : b];
        }

        case ServerPool::ROUND_ROBIN:
        default:
            return (int)_active[_rrNext++ % n];
    }
}
