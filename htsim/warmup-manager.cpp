/*
 * Executes warm-up transfers. See warmup-manager.h.
 */
#include "warmup-manager.h"

#include <algorithm>

using namespace std;

namespace {
    /* The base FlowGenerator needs a route generator and an arrival rate it
     * will never use: this manager creates every flow itself and is never
     * scheduled to generate any. */
    void nullRouteGen(route_t *&fwd, route_t *&rev, uint32_t &src, uint32_t &dst)
    {
        fwd = NULL; rev = NULL; src = 0; dst = 0;
    }
}

WarmupManager::WarmupManager(GpuFleet *fleet,
                             WarmupPolicy *policy,
                             warmup_route_gen_t routeGen,
                             DataSource::EndHost endhost)
    : FlowGenerator(endhost, nullRouteGen, speedFromGbps(1), 100000, Workloads::UNIFORM),
    _fleet(fleet),
    _policy(policy),
    _routeGen(routeGen),
    _endhost(endhost),
    _storageBytes(0),
    _localPeerBytes(0),
    _remotePeerBytes(0),
    _completed(0),
    _flowSeq(0),
    _retry(timeFromUs(500)),
    _retryScheduled(false)
{
    setPrefix("warm-");
}

void
WarmupManager::setRetryInterval(simtime_picosec t)
{
    _retry = t;
}

void
WarmupManager::startWarmup(uint32_t node)
{
    Record r;
    r.node = node;
    r.rack = _fleet->rackOf(node);
    r.started = EventList::Get().now();
    r.finished = 0;
    r.bytesFromStorage = 0;
    r.bytesFromLocalPeer = 0;
    r.bytesFromRemotePeer = 0;
    r.tasks = 0;

    Progress p;
    p.bytesDone = 0;
    p.outstanding = 0;
    p.recordIdx = _records.size();

    _records.push_back(r);
    _progress[node] = p;

    issue(node, _policy->plan(node, *_fleet));
}

void
WarmupManager::abortWarmup(uint32_t node)
{
    _progress.erase(node);
}

/*
 * Open one flow per task. An empty plan means the policy wants to wait --
 * for a local peer to finish warming, say, rather than fall back to slow
 * storage -- so the node goes on the retry list.
 */
void
WarmupManager::issue(uint32_t node, const vector<PullTask> &tasks)
{
    if (tasks.empty()) {
        if (_progress.count(node) && _progress[node].outstanding == 0) {
            _unplanned.push_back(node);
            if (!_retryScheduled) {
                _retryScheduled = true;
                EventList::Get().sourceIsPendingRel(*this, _retry);
            }
        }
        return;
    }

    for (size_t i = 0; i < tasks.size(); i++) {
        startTask(node, tasks[i]);
    }
}

void
WarmupManager::startTask(uint32_t node, const PullTask &task)
{
    if (task.length == 0) {
        return;
    }

    route_t *fwd = NULL, *rev = NULL;
    _routeGen(task, node, fwd, rev);
    if (fwd == NULL || rev == NULL) {
        return;   // topology could not build this route; treat as declined
    }

    /* Same construction FlowGenerator::createFlow uses, minus the workload
     * sampling: this flow's size is the byte range the policy asked for. */
    TcpSrc *src = new TcpSrc(NULL, NULL, task.length);
    TcpSink *snk = new TcpSink();

    if (_endhost == DataSource::DCTCP) {
        TcpSrc::_enable_dctcp = true;
    }

    src->setName("warm-src" + to_string(_flowSeq));
    snk->setName("warm-snk" + to_string(_flowSeq));
    _flowSeq++;

    src->_node_id = task.fromStorage() ? 0 : (uint32_t)task.source;
    snk->_node_id = node;

    fwd->push_back(snk);
    rev->push_back(src);

    src->connect(EventList::Get().now() + 1, *fwd, *rev, *snk);
    src->setFlowGenerator(this);

    InFlight f;
    f.node = node;
    f.task = task;
    _inFlight[src->id] = f;

    _progress[node].outstanding++;
    _records[_progress[node].recordIdx].tasks++;

    /* A donating peer is sourcing this transfer out of its own egress link,
     * so the fleet tracks it and policies can avoid piling onto one donor. */
    if (!task.fromStorage()) {
        _fleet->noteDonationStart((uint32_t)task.source);
    }
}

void
WarmupManager::finishFlow(uint32_t flow_id)
{
    map<uint32_t, InFlight>::iterator it = _inFlight.find(flow_id);
    if (it == _inFlight.end()) {
        return;   // not one of ours
    }

    uint32_t node = it->second.node;
    PullTask task = it->second.task;
    _inFlight.erase(it);

    if (!task.fromStorage()) {
        _fleet->noteDonationEnd((uint32_t)task.source);
    }

    map<uint32_t, Progress>::iterator pit = _progress.find(node);
    if (pit == _progress.end()) {
        return;   // warm-up was aborted
    }

    Progress &p = pit->second;
    p.outstanding--;
    p.bytesDone += task.length;

    /* Attribute the bytes to a tier, so the run can report what the scaling
     * decision actually cost each part of the system. */
    Record &rec = _records[p.recordIdx];
    if (task.fromStorage()) {
        rec.bytesFromStorage += task.length;
        _storageBytes += task.length;
    } else if (_fleet->rackOf((uint32_t)task.source) == rec.rack) {
        rec.bytesFromLocalPeer += task.length;
        _localPeerBytes += task.length;
    } else {
        rec.bytesFromRemotePeer += task.length;
        _remotePeerBytes += task.length;
    }

    /* Adaptive policies get a chance to schedule follow-up transfers. */
    vector<PullTask> more = _policy->onTaskComplete(node, task, *_fleet);
    if (!more.empty()) {
        issue(node, more);
        return;
    }

    if (p.bytesDone >= _fleet->stateBytes() && p.outstanding == 0) {
        rec.finished = EventList::Get().now();
        _completed++;
        _progress.erase(pit);
        _fleet->onWarmupComplete(node);   // it can now serve, and donate
    }
}

/*
 * Retry pass: ask the policy again for nodes it previously declined to plan.
 * Conditions change -- a local peer may have finished warming since.
 */
void
WarmupManager::doNextEvent()
{
    _retryScheduled = false;

    vector<uint32_t> pending;
    pending.swap(_unplanned);

    for (size_t i = 0; i < pending.size(); i++) {
        uint32_t node = pending[i];
        if (_progress.count(node) == 0) {
            continue;   // finished or aborted in the meantime
        }
        issue(node, _policy->plan(node, *_fleet));
    }

    if (!_unplanned.empty() && !_retryScheduled) {
        _retryScheduled = true;
        EventList::Get().sourceIsPendingRel(*this, _retry);
    }
}

vector<simtime_picosec>
WarmupManager::durations() const
{
    vector<simtime_picosec> out;
    for (size_t i = 0; i < _records.size(); i++) {
        if (_records[i].finished > 0) {
            out.push_back(_records[i].finished - _records[i].started);
        }
    }
    sort(out.begin(), out.end());
    return out;
}
