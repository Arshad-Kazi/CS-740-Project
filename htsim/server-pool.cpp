/*
 * Backend server pool with elastic capacity. See server-pool.h.
 */
#include "server-pool.h"

#include <algorithm>

using namespace std;

ServerPool::ServerPool(uint32_t maxServers,
                       uint32_t initialActive,
                       simtime_picosec bootDelay)
    : EventSource("ServerPool"),
    _dispatches(0),
    _starts(0),
    _stops(0),
    _serverTimePs(0),
    _maxServers(maxServers),
    _bootDelay(bootDelay),
    _policy(ROUND_ROBIN),
    _state(maxServers, OFF),
    _readyAt(maxServers, 0),
    _egress(maxServers, NULL),
    _nBooting(0),
    _rrNext(0),
    _lastAccrual(0)
{
    /* The initial fleet is up before the simulation starts: no boot delay. */
    uint32_t n = min(initialActive, maxServers);
    for (uint32_t i = 0; i < n; i++) {
        _state[i] = ACTIVE;
        _active.push_back(i);
    }
    _starts = n;
}

bool
ServerPool::parsePolicy(const string &s, LbPolicy &out)
{
    if (s == "rr" || s == "roundrobin") { out = ROUND_ROBIN;  return true; }
    if (s == "random")                  { out = RANDOM;       return true; }
    if (s == "least")                   { out = LEAST_LOADED; return true; }
    if (s == "p2c")                     { out = P2C;          return true; }
    return false;
}

void
ServerPool::setEgressQueue(uint32_t i, DreQueue *q)
{
    if (i < _maxServers) {
        _egress[i] = q;
    }
}

/*
 * Integrate nActive over time, so the end-of-run summary can report
 * server-seconds consumed. This is the "cost" axis: a controller that
 * overshoots keeps more servers running than it needed to.
 */
void
ServerPool::accrueServerTime()
{
    simtime_picosec now = EventList::Get().now();
    _serverTimePs += (uint64_t)_active.size() * (now - _lastAccrual);
    _lastAccrual = now;
}

void
ServerPool::doNextEvent()
{
    simtime_picosec now = EventList::Get().now();
    accrueServerTime();

    for (uint32_t i = 0; i < _maxServers; i++) {
        if (_state[i] == BOOTING && _readyAt[i] <= now) {
            _state[i] = ACTIVE;
            _active.push_back(i);
            _nBooting--;
        }
    }
}

uint32_t
ServerPool::scaleUp(uint32_t n)
{
    accrueServerTime();

    simtime_picosec now = EventList::Get().now();
    uint32_t started = 0;

    for (uint32_t i = 0; i < _maxServers && started < n; i++) {
        if (_state[i] != OFF) {
            continue;
        }
        _state[i] = BOOTING;
        _readyAt[i] = now + _bootDelay;
        _nBooting++;
        started++;

        /* Wake the pool when this server finishes booting. */
        EventList::Get().sourceIsPending(*this, _readyAt[i]);
    }

    _starts += started;
    return started;
}

uint32_t
ServerPool::scaleDown(uint32_t n)
{
    accrueServerTime();

    uint32_t stopped = 0;

    /* Shed the most recently added servers first, and never go to zero. */
    while (stopped < n && _active.size() > 1) {
        uint32_t victim = _active.back();
        _active.pop_back();
        _state[victim] = OFF;
        stopped++;
    }

    _stops += stopped;
    return stopped;
}

double
ServerPool::serverSeconds()
{
    accrueServerTime();
    return timeAsSec(_serverTimePs);
}

double
ServerPool::load(uint32_t i)
{
    if (i >= _maxServers || _egress[i] == NULL) {
        return 0.0;
    }
    return _egress[i]->utilization();
}

double
ServerPool::meanLoad()
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
ServerPool::maxLoad()
{
    double mx = 0.0;
    for (size_t k = 0; k < _active.size(); k++) {
        mx = max(mx, load(_active[k]));
    }
    return mx;
}

int
ServerPool::select(mt19937 &rng)
{
    if (_active.empty()) {
        return -1;
    }

    _dispatches++;
    size_t n = _active.size();

    switch (_policy) {
        case RANDOM:
            return (int)_active[rng() % n];

        case LEAST_LOADED: {
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

        case P2C: {
            size_t a = rng() % n;
            size_t b = rng() % n;
            return (int)_active[load(_active[a]) <= load(_active[b]) ? a : b];
        }

        case ROUND_ROBIN:
        default:
            return (int)_active[_rrNext++ % n];
    }
}
