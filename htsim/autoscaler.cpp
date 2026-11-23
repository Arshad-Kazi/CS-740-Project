/*
 * Autoscaling controller. See autoscaler.h.
 */
#include "autoscaler.h"

#include <algorithm>
#include <cmath>
#include <iomanip>
#include <iostream>

using namespace std;

AutoScaler::AutoScaler(ElasticFleet *fleet,
                       Policy policy,
                       simtime_picosec interval)
    : EventSource("AutoScaler"),
    _ticks(0),
    _upActions(0),
    _downActions(0),
    _fleet(fleet),
    _policy(policy),
    _interval(interval),
    _endTime(0),
    _up(0.70),
    _down(0.30),
    _target(0.60),
    _stepUp(1),
    _stepDown(1),
    _cooldownUp(0),
    _cooldownDown(0),
    _lastUp(0),
    _lastDown(0),
    _countBooting(true)
{
}

bool
AutoScaler::parsePolicy(const string &s, Policy &out)
{
    if (s == "none" || s == "static") { out = NONE;      return true; }
    if (s == "threshold")             { out = THRESHOLD; return true; }
    if (s == "target")                { out = TARGET;    return true; }
    return false;
}

void
AutoScaler::setCooldown(simtime_picosec up, simtime_picosec down)
{
    _cooldownUp = up;
    _cooldownDown = down;
}

void
AutoScaler::start(simtime_picosec startTime, simtime_picosec endTime)
{
    _endTime = endTime;
    EventList::Get().sourceIsPending(*this, startTime);
}

/*
 * Target-tracking: demand must be estimated from the servers that are
 * actually *serving*, i.e. 'serving' x meanLoad server-equivalents, and we
 * want that at _target utilisation.
 *
 * Using the ordered fleet (active + booting) here instead would be a
 * runaway: booting servers carry no load but would scale the demand
 * estimate up, so each tick would order more than the last. The caller
 * compares this result against the ordered fleet, which is what stops it
 * re-ordering capacity that is already on its way.
 *
 * This still under-estimates whenever the fleet is saturated, because
 * utilisation pins near 1.0 and hides the true backlog, so recovery from a
 * large surge takes several control intervals. That lag is realistic.
 */
uint32_t
AutoScaler::desiredServers(double meanLoad, uint32_t serving) const
{
    if (_target <= 0.0) {
        return serving;
    }
    double want = ceil(serving * meanLoad / _target);
    if (want < 1.0) {
        want = 1.0;
    }
    return (uint32_t)want;
}

void
AutoScaler::doNextEvent()
{
    simtime_picosec now = EventList::Get().now();

    if (now > _endTime) {
        return;
    }

    _ticks++;

    double meanLoad = _fleet->meanLoad();
    double maxLoad = _fleet->maxLoad();
    uint32_t active = _fleet->nActive();
    uint32_t booting = _fleet->nPending();

    /* Capacity already ordered, if the controller is told to account for it. */
    uint32_t fleet = active + (_countBooting ? booting : 0);

    const char *action = "hold";
    uint32_t delta = 0;

    switch (_policy) {
        case THRESHOLD:
            if (meanLoad > _up && now - _lastUp >= _cooldownUp) {
                /* Pending boots already address the overload. */
                if (!_countBooting || booting == 0) {
                    delta = _fleet->scaleUp(_stepUp);
                    if (delta > 0) { action = "up"; _lastUp = now; _upActions++; }
                }
            } else if (meanLoad < _down && now - _lastDown >= _cooldownDown) {
                delta = _fleet->scaleDown(_stepDown);
                if (delta > 0) { action = "down"; _lastDown = now; _downActions++; }
            }
            break;

        case TARGET: {
            uint32_t want = desiredServers(meanLoad, active);
            if (want > fleet && now - _lastUp >= _cooldownUp) {
                delta = _fleet->scaleUp(min(want - fleet, _stepUp));
                if (delta > 0) { action = "up"; _lastUp = now; _upActions++; }
            } else if (want < fleet && meanLoad < _target
                       && now - _lastDown >= _cooldownDown) {
                /* The meanLoad guard matters: under saturation the demand
                 * estimate is capped by current capacity, so want < fleet can
                 * be true *while overloaded*. Without this the controller
                 * sheds nodes during a surge and never recovers. */
                delta = _fleet->scaleDown(min(fleet - want, _stepDown));
                if (delta > 0) { action = "down"; _lastDown = now; _downActions++; }
            }
            break;
        }

        case NONE:
        default:
            break;
    }

    /* One line per control tick: the fleet-size time series. */
    cout << fixed << setprecision(4)
         << "AutoScale t " << timeAsUs(now)
         << " active " << active
         << " booting " << booting
         << " mean_util " << meanLoad
         << " max_util " << maxLoad
         << " action " << action
         << " delta " << delta << endl;

    EventList::Get().sourceIsPendingRel(*this, _interval);
}
