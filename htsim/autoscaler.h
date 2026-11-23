/*
 * Autoscaling controller.
 *
 * An EventSource that wakes every control interval, reads the fleet's load
 * from the DRE on each backend's access link, and grows or shrinks the
 * ACTIVE set. It reschedules itself, the same way FlowGenerator does.
 *
 * The controller can only observe utilisation, which saturates at 1.0. An
 * overloaded fleet therefore cannot tell the controller *how* overloaded it
 * is -- exactly the blindness a real autoscaler has. Combined with the boot
 * delay in ServerPool, that is what produces reaction lag, overshoot and
 * oscillation, which is the phenomenon this testbed exists to measure.
 *
 * Policies:
 *   none      static fleet; the baseline to compare against.
 *   threshold reactive: add/remove a fixed step while load is outside
 *             [down, up]. Simple, and oscillates readily.
 *   target    target-tracking (the AWS-style rule): aim for
 *             nActive * meanLoad / targetLoad servers.
 *
 * countBooting decides whether servers still booting count towards the
 * fleet when sizing. Ignoring them is the classic overshoot bug: the
 * controller keeps adding capacity it has already ordered.
 */
#ifndef AUTOSCALER_H
#define AUTOSCALER_H

#include "eventlist.h"
#include "elastic-fleet.h"

#include <string>

class AutoScaler : public EventSource
{
    public:
        enum Policy { NONE, THRESHOLD, TARGET };

        AutoScaler(ElasticFleet *fleet, Policy policy, simtime_picosec interval);

        static bool parsePolicy(const std::string &s, Policy &out);

        void setThresholds(double up, double down) { _up = up; _down = down; }
        void setTarget(double target) { _target = target; }
        void setStep(uint32_t up, uint32_t down) { _stepUp = up; _stepDown = down; }
        void setCooldown(simtime_picosec up, simtime_picosec down);
        void setCountBooting(bool b) { _countBooting = b; }

        /* Begin control at startTime and stop after endTime. */
        void start(simtime_picosec startTime, simtime_picosec endTime);

        void doNextEvent();

        uint64_t _ticks;
        uint64_t _upActions;
        uint64_t _downActions;

    private:
        uint32_t desiredServers(double meanLoad, uint32_t serving) const;

        ElasticFleet *_fleet;
        Policy _policy;
        simtime_picosec _interval;
        simtime_picosec _endTime;

        double _up;       // scale-up threshold (threshold policy)
        double _down;     // scale-down threshold (threshold policy)
        double _target;   // desired utilisation (target policy)

        uint32_t _stepUp;
        uint32_t _stepDown;

        simtime_picosec _cooldownUp;
        simtime_picosec _cooldownDown;
        simtime_picosec _lastUp;
        simtime_picosec _lastDown;

        bool _countBooting;
};

#endif /* AUTOSCALER_H */
