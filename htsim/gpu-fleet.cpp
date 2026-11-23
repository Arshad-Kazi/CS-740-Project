/*
 * Elastic fleet of GPU inference nodes. See gpu-fleet.h.
 */
#include "gpu-fleet.h"

#include <algorithm>

using namespace std;

GpuFleet::GpuFleet(uint32_t nRacks,
                   uint32_t nodesPerRack,
                   uint32_t initialActive,
                   uint64_t stateBytes,
                   uint32_t nStorageNodes,
                   Placement placement)
    : _dispatches(0),
    _starts(0),
    _stops(0),
    _maxNodes(nRacks * nodesPerRack),
    _nRacks(nRacks),
    _nodesPerRack(nodesPerRack),
    _nStorage(nStorageNodes),
    _stateBytes(stateBytes),
    _placement(placement),
    _starter(NULL),
    _state(nRacks * nodesPerRack, OFF),
    _rackOf(nRacks * nodesPerRack, 0),
    _slotOf(nRacks * nodesPerRack, 0),
    _compute(nRacks * nodesPerRack, NULL),
    _donations(nRacks * nodesPerRack, 0),
    _nWarming(0),
    _rrNext(0),
    _nodeTimePs(0),
    _lastAccrual(0)
{
    /*
     * Placement decides how many racks a given fleet size touches, which is
     * exactly what peer-assisted warm-up is sensitive to: PACKED needs one
     * seed per rack and then distributes locally, ROUNDROBIN spreads serving
     * load evenly but puts every new node in a rack of its own.
     */
    for (uint32_t i = 0; i < _maxNodes; i++) {
        if (_placement == PACKED) {
            _rackOf[i] = i / _nodesPerRack;
            _slotOf[i] = i % _nodesPerRack;
        } else {
            _rackOf[i] = i % _nRacks;
            _slotOf[i] = i / _nRacks;
        }
    }

    /* The initial fleet is already warm when the run starts. */
    uint32_t n = min(initialActive, _maxNodes);
    for (uint32_t i = 0; i < n; i++) {
        _state[i] = ACTIVE;
        _active.push_back(i);
    }
    _starts = n;
}

bool
GpuFleet::parsePlacement(const string &s, Placement &out)
{
    if (s == "packed")                       { out = PACKED;     return true; }
    if (s == "roundrobin" || s == "spread")  { out = ROUNDROBIN; return true; }
    return false;
}

void
GpuFleet::setComputeQueue(uint32_t i, DreQueue *q)
{
    if (i < _maxNodes) {
        _compute[i] = q;
    }
}

void
GpuFleet::accrue()
{
    simtime_picosec now = EventList::Get().now();
    _nodeTimePs += (uint64_t)_active.size() * (now - _lastAccrual);
    _lastAccrual = now;
}

double
GpuFleet::nodeSeconds()
{
    accrue();
    return timeAsSec(_nodeTimePs);
}

double
GpuFleet::loadOf(uint32_t node)
{
    if (node >= _maxNodes || _compute[node] == NULL) {
        return 0.0;
    }
    return _compute[node]->utilization();
}

double
GpuFleet::meanLoad()
{
    if (_active.empty()) {
        return 0.0;
    }
    double sum = 0.0;
    for (size_t k = 0; k < _active.size(); k++) {
        sum += loadOf(_active[k]);
    }
    return sum / _active.size();
}

double
GpuFleet::maxLoad()
{
    double mx = 0.0;
    for (size_t k = 0; k < _active.size(); k++) {
        mx = max(mx, loadOf(_active[k]));
    }
    return mx;
}

uint32_t
GpuFleet::scaleUp(uint32_t n)
{
    accrue();

    uint32_t started = 0;
    for (uint32_t i = 0; i < _maxNodes && started < n; i++) {
        if (_state[i] != OFF) {
            continue;
        }
        _state[i] = WARMING;
        _nWarming++;
        started++;

        /* The manager asks the policy where the bytes should come from and
         * opens the transfers. The node stays WARMING until they finish. */
        if (_starter != NULL) {
            _starter->startWarmup(i);
        }
    }

    _starts += started;
    return started;
}

uint32_t
GpuFleet::scaleDown(uint32_t n)
{
    accrue();

    uint32_t stopped = 0;

    /*
     * Shed the most recently added nodes first, and never go to zero.
     * Only ACTIVE nodes are shed: cancelling a warm-up mid-transfer would
     * waste the bytes already moved, which a controller has no reason to do.
     */
    while (stopped < n && _active.size() > 1) {
        uint32_t victim = _active.back();
        _active.pop_back();
        _state[victim] = OFF;    // weights are lost with the node
        stopped++;
    }

    _stops += stopped;
    return stopped;
}

void
GpuFleet::onWarmupComplete(uint32_t node)
{
    if (node >= _maxNodes || _state[node] != WARMING) {
        return;
    }
    accrue();

    _state[node] = ACTIVE;
    _active.push_back(node);
    _nWarming--;
    /* The node can now serve -- and, just as importantly, donate. */
}

void
GpuFleet::noteDonationStart(uint32_t node)
{
    if (node < _maxNodes) {
        _donations[node]++;
    }
}

void
GpuFleet::noteDonationEnd(uint32_t node)
{
    if (node < _maxNodes && _donations[node] > 0) {
        _donations[node]--;
    }
}

int
GpuFleet::select()
{
    if (_active.empty()) {
        return -1;
    }
    _dispatches++;
    return (int)_active[_rrNext++ % _active.size()];
}
