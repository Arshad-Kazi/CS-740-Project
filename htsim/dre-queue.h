/*
 * FIFO queue with a Discounting Rate Estimator (DRE), as used by CONGA to
 * measure the congestion of a fabric link.
 *
 *   - For every packet transmitted on the link:  X = X + packet_size
 *   - Every Tdre:                                X = X * (1 - alpha)
 *
 * In steady state X ~= R * tau, where R is the link's send rate and
 * tau = Tdre / alpha. So X / (C * tau) estimates the link utilization, which
 * is quantized to Q bits to give the congestion metric (0 .. 2^Q - 1).
 *
 * The periodic decay is applied lazily (when the DRE is read or updated)
 * instead of with a timer event per link, which gives the same value without
 * adding events to the simulation.
 */
#ifndef DRE_QUEUE_H
#define DRE_QUEUE_H

#include "queue.h"

class DreQueue : public Queue
{
    public:
        DreQueue(linkspeed_bps bitrate, mem_b maxsize, QueueLogger *logger);

        /* DRE parameters shared by all fabric links. */
        static void configure(simtime_picosec tdre, double alpha, uint32_t qbits);

        /* Current estimated utilization of the link (0 .. ~1). */
        double utilization();

        /* Current congestion metric, quantized to Q bits. */
        uint8_t congestionMetric();

        /* Total bytes transmitted on this link (for statistics). */
        uint64_t bytesSent() const { return _bytesSent; }

    protected:
        virtual void completeService();

    private:
        void decay(simtime_picosec now);

        double _x;                    // DRE register, in bytes.
        simtime_picosec _lastDecay;   // Time of the last applied decay step.
        uint64_t _bytesSent;
        double _capacityBytesPerTau;  // C * tau, in bytes.

        static simtime_picosec _tdre;
        static double _alpha;
        static uint32_t _qbits;
};

#endif /* DRE_QUEUE_H */
