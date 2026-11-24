/*
 * FIFO queue with CSIG available-bandwidth tracking.
 */
#include "csig-queue.h"

using namespace std;

// Default measurement window: 10 µs (10 000 000 ps).
simtime_picosec CsigQueue::_tcsig = 10000000000ULL; // 10 µs in picoseconds

CsigQueue::CsigQueue(linkspeed_bps bitrate,
                     mem_b maxsize,
                     QueueLogger *logger,
                     uint32_t csigId)
                    : DreQueue(bitrate, maxsize, logger),
                    _csigId(csigId),
                    _bytesSentInWindow(0),
                    _bytesSentInPrevWindow(0),
                    _windowStart(0),
                    _totalBytesSent(0)
{
}

void
CsigQueue::configure(simtime_picosec tcsig)
{
    _tcsig = tcsig;
}

void
CsigQueue::advanceWindow(simtime_picosec now)
{
    if (now < _windowStart + _tcsig) {
        return;  // Still within the current window.
    }

    // How many full windows have elapsed since _windowStart?
    uint64_t elapsed = (now - _windowStart) / _tcsig;
    _windowStart += elapsed * _tcsig;

    if (elapsed == 1) {
        // Normal case: exactly one window boundary crossed.
        _bytesSentInPrevWindow = _bytesSentInWindow;
    } else {
        // Two or more windows elapsed — the previous window was idle.
        _bytesSentInPrevWindow = 0;
    }
    _bytesSentInWindow = 0;
}

uint64_t
CsigQueue::currentAbwBps()
{
    advanceWindow(EventList::Get().now());

    // Estimate send rate from the most recent completed window.
    // send_rate (bps) = bytes_in_prev_window * 8 / T_csig
    double tcsigSec = timeAsSec(_tcsig);
    double sendRateBps = (_bytesSentInPrevWindow * 8.0) / tcsigSec;

    // ABW = C - send_rate, clamped to zero.
    if (sendRateBps >= (double)_bitrate) {
        return 0;
    }
    return (uint64_t)((double)_bitrate - sendRateBps);
}

void
CsigQueue::completeService()
{
    // The packet at the head of the queue is about to leave on the link.
    Packet *pkt = _enqueued.back();

    advanceWindow(EventList::Get().now());
    _bytesSentInWindow += pkt->size();
    _totalBytesSent += pkt->size();

    // CSIG: perform compare-and-replace on the packet header.
    // The packet carries the minimum ABW seen along its path.
    if (pkt->csig_enabled()) {
        pkt->csig_update(currentAbwBps(), _csigId);
    }

    DreQueue::completeService();
}
