/*
 * FIFO queue with CSIG (Congestion Signaling) available-bandwidth tracking.
 *
 * CSIG measures the minimum available bandwidth (ABW) seen by a packet as
 * it traverses the fabric. Each egress queue computes its local ABW as:
 *
 *     ABW = C - send_rate
 *
 * where C is the link capacity (in bps) and send_rate is estimated from a
 * byte counter over a configurable measurement window T_csig:
 *
 *     send_rate = bytes_sent_in_window * 8 / T_csig
 *
 * The byte counter is reset at the start of each window (applied lazily).
 * When a packet departs, the queue performs a compare-and-replace on the
 * packet's CSIG header: if the local ABW is lower than the value already
 * carried by the packet, the queue overwrites it and stamps its locator ID.
 *
 * CsigQueue inherits from DreQueue so that links can support both CONGA
 * and CSIG metrics, and can be used anywhere a DreQueue or Queue is expected.
 */
#ifndef CSIG_QUEUE_H
#define CSIG_QUEUE_H

#include "dre-queue.h"

class CsigQueue : public DreQueue
{
    public:
        CsigQueue(linkspeed_bps bitrate, mem_b maxsize, QueueLogger *logger,
                  uint32_t csigId = 0);

        /* CSIG parameters shared by all fabric links. */
        static void configure(simtime_picosec tcsig);

        /* Current available bandwidth of this link in bits per second. */
        uint64_t currentAbwBps();

        /* Total bytes transmitted on this link (for statistics). */
        uint64_t bytesSent() const { return _totalBytesSent; }

        /* Locator ID for this queue (identifies this link in CSIG headers). */
        uint32_t csigId() const { return _csigId; }

    protected:
        virtual void completeService();

    private:
        /* Advance the measurement window if enough time has elapsed. */
        void advanceWindow(simtime_picosec now);

        uint32_t _csigId;                  // Locator ID stamped into packets.

        uint64_t _bytesSentInWindow;       // Bytes sent in the current window.
        uint64_t _bytesSentInPrevWindow;   // Bytes sent in the previous window.
        simtime_picosec _windowStart;      // Start time of the current window.
        uint64_t _totalBytesSent;          // Lifetime byte counter.

        static simtime_picosec _tcsig;     // Measurement window duration.
};

#endif /* CSIG_QUEUE_H */
