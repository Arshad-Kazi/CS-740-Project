/*
 * Backend server pool with elastic capacity.
 *
 * htsim builds its topology statically, so "adding a node" is modelled by
 * pre-allocating the maximum fleet at setup and gating *admission*: only
 * servers in the ACTIVE set are given to new flows by the load balancer.
 * Because FlowGenerator calls the route generator once per flow, at the
 * instant the flow is created, a server that becomes ACTIVE mid-simulation
 * starts receiving work immediately. Flows already in flight keep the route
 * they were built with, which matches real behaviour.
 *
 * A server's capacity is its access link (server -> leaf). Its load is the
 * DRE utilisation of that link's queue, i.e. the same Discounting Rate
 * Estimator CONGA uses on fabric links. That single signal feeds both the
 * load balancer (which server to dispatch to) and the autoscaler (whether
 * the fleet is too small).
 *
 * Scale-up is not instantaneous: a server asked to start enters BOOTING and
 * only becomes ACTIVE bootDelay later. That lag is the point of the
 * experiment -- it is what makes a reactive controller overshoot.
 */
#ifndef SERVER_POOL_H
#define SERVER_POOL_H

#include "eventlist.h"
#include "dre-queue.h"
#include "elastic-fleet.h"

#include <random>
#include <string>
#include <vector>

class ServerPool : public EventSource, public ElasticFleet
{
    public:
        /* How the load balancer picks among ACTIVE servers. */
        enum LbPolicy {
            ROUND_ROBIN,   // strict rotation
            RANDOM,        // uniform random (= stateless hashing)
            LEAST_LOADED,  // lowest DRE utilisation (full fleet scan)
            P2C            // power of two choices: best of two random picks
        };

        enum State { OFF, BOOTING, ACTIVE };

        ServerPool(uint32_t maxServers, uint32_t initialActive, simtime_picosec bootDelay);

        static bool parsePolicy(const std::string &s, LbPolicy &out);

        /* Register the egress (server -> leaf) queue of server i. */
        void setEgressQueue(uint32_t i, DreQueue *q);
        void setPolicy(LbPolicy p) { _policy = p; }

        /* Promotes BOOTING servers whose boot delay has elapsed. */
        void doNextEvent();

        /* Start n more servers. Returns how many actually began booting. */
        uint32_t scaleUp(uint32_t n);

        /* Remove n servers from the dispatch set. Returns how many went away.
         * In-flight flows on those servers are left alone; they simply stop
         * receiving new work. Never shrinks below one server. */
        uint32_t scaleDown(uint32_t n);

        /* Pick a server for a new flow. Returns -1 if none are ACTIVE. */
        int select(std::mt19937 &rng);

        /* Current DRE utilisation (0 .. ~1) of server i's access link. */
        double load(uint32_t i);

        /* Utilisation statistics over the ACTIVE set. */
        double meanLoad();
        double maxLoad();

        /* ElasticFleet: expt 4's pending capacity is booting servers. */
        uint32_t nPending() const { return _nBooting; }

        /* Server-seconds consumed so far. Accrues up to the current time
         * when called, so it is correct even for a static fleet where no
         * scaling event ever fires. */
        double serverSeconds();

        uint32_t nActive() const { return (uint32_t)_active.size(); }
        uint32_t nBooting() const { return _nBooting; }
        uint32_t maxServers() const { return _maxServers; }

        /* Cumulative counters, for the end-of-run summary. */
        uint64_t _dispatches;
        uint64_t _starts;        // servers booted over the whole run
        uint64_t _stops;         // servers removed over the whole run
        uint64_t _serverTimePs;  // integral of nActive over time (server-seconds)

    private:
        void accrueServerTime();

        uint32_t _maxServers;
        simtime_picosec _bootDelay;
        LbPolicy _policy;

        std::vector<State> _state;
        std::vector<simtime_picosec> _readyAt;
        std::vector<DreQueue*> _egress;

        std::vector<uint32_t> _active;   // dispatchable server indices
        uint32_t _nBooting;
        uint32_t _rrNext;                // round-robin cursor

        simtime_picosec _lastAccrual;
};

#endif /* SERVER_POOL_H */
