/*
 * FIFO queue with a Discounting Rate Estimator (DRE) for CONGA.
 */
#include "dre-queue.h"

#include <cmath>

using namespace std;

// Defaults: tau = Tdre / alpha = 160us, 3-bit metric.
simtime_picosec DreQueue::_tdre = 16000000; // 16us in picoseconds
double DreQueue::_alpha = 0.1;
uint32_t DreQueue::_qbits = 3;

DreQueue::DreQueue(linkspeed_bps bitrate,
                   mem_b maxsize,
                   QueueLogger *logger)
                  : Queue(bitrate, maxsize, logger),
                  _x(0.0),
                  _lastDecay(0),
                  _bytesSent(0)
{
    double tauSec = timeAsSec(_tdre) / _alpha;
    _capacityBytesPerTau = (bitrate / 8.0) * tauSec;
}

void
DreQueue::configure(simtime_picosec tdre,
                    double alpha,
                    uint32_t qbits)
{
    _tdre = tdre;
    _alpha = alpha;
    _qbits = qbits;
}

void
DreQueue::decay(simtime_picosec now)
{
    if (now < _lastDecay + _tdre) {
        return;
    }

    uint64_t periods = (now - _lastDecay) / _tdre;
    _lastDecay += periods * _tdre;

    if (_x <= 0.0) {
        return;
    }
    if (periods > 1000) {
        _x = 0.0;   // (1 - alpha)^1000 is effectively zero.
    } else {
        _x *= pow(1.0 - _alpha, (double)periods);
    }
}

double
DreQueue::utilization()
{
    decay(EventList::Get().now());
    return _x / _capacityBytesPerTau;
}

uint8_t
DreQueue::congestionMetric()
{
    uint32_t levels = 1u << _qbits;
    uint32_t m = (uint32_t)(utilization() * levels);
    if (m > levels - 1) {
        m = levels - 1;
    }
    return (uint8_t)m;
}

void
DreQueue::completeService()
{
    // The packet at the head of the queue is about to leave on the link.
    Packet *pkt = _enqueued.back();

    decay(EventList::Get().now());
    _x += pkt->size();
    _bytesSent += pkt->size();

    // CONGA: the packet carries the maximum congestion seen along its path.
    if (pkt->conga.valid) {
        uint8_t m = congestionMetric();
        if (m > pkt->conga.ce) {
            pkt->conga.ce = m;
        }
    }

    Queue::completeService();
}
