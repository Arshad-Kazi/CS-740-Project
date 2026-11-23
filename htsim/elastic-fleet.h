/*
 * Abstract elastic fleet.
 *
 * The autoscaler does not care *how* capacity is added -- whether a node
 * becomes ready after a fixed timer (expt 4) or after its model weights
 * finish transferring over the network (expt 5). It only needs to read the
 * fleet's load and ask for more or fewer servers.
 *
 * Keeping that contract in one place means one control loop, tested once,
 * drives both experiments.
 */
#ifndef ELASTIC_FLEET_H
#define ELASTIC_FLEET_H

#include <cstdint>

class ElasticFleet
{
    public:
        virtual ~ElasticFleet() {}

        /* Load over the serving set, 0 .. ~1. Not const: reading a DRE
         * applies its pending decay. */
        virtual double meanLoad() = 0;
        virtual double maxLoad() = 0;

        /* Nodes currently serving. */
        virtual uint32_t nActive() const = 0;

        /* Capacity already ordered but not yet serving: booting (expt 4) or
         * warming (expt 5). A controller that ignores this re-orders capacity
         * that is already on its way. */
        virtual uint32_t nPending() const = 0;

        /* Returns how many were actually started / stopped. */
        virtual uint32_t scaleUp(uint32_t n) = 0;
        virtual uint32_t scaleDown(uint32_t n) = 0;
};

#endif /* ELASTIC_FLEET_H */
