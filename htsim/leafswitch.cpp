/*
 * CONGA leaf switch: Congestion-To-Leaf / Congestion-From-Leaf tables,
 * congestion feedback, and congestion-aware uplink selection.
 */
#include "leafswitch.h"

using namespace std;

simtime_picosec LeafSwitch::_agingTime = 1000000000; // 1ms in picoseconds

/* ---------------- Route hooks ---------------- */

void
LeafUplinkHook::init(LeafSwitch *leaf, uint32_t dstLeaf, uint32_t uplink)
{
    _leaf = leaf;
    _dstLeaf = dstLeaf;
    _uplink = uplink;
}

void
LeafUplinkHook::receivePacket(Packet &pkt)
{
    _leaf->onLeaveRack(pkt, _dstLeaf, _uplink);
    pkt.sendOn();
}

void
LeafDownlinkHook::receivePacket(Packet &pkt)
{
    _leaf->onEnterRack(pkt);
    pkt.sendOn();
}

/* ---------------- Leaf switch ---------------- */

LeafSwitch::LeafSwitch(uint32_t id, uint32_t nLeaves, uint32_t nUplinks)
    : _decisions(0),
      _fbSent(0),
      _fbReceived(0),
      _sumChosenScore(0),
      _sumMeanScore(0),
      _id(id),
      _nUplinks(nUplinks),
      _uplinks(nUplinks, NULL),
      _toLeaf(nLeaves, vector<Entry>(nUplinks, Entry())),
      _fromLeaf(nLeaves, vector<Entry>(nUplinks, Entry())),
      _fbNext(nLeaves, 0),
      _upHooks(nLeaves * nUplinks)
{
    for (uint32_t d = 0; d < nLeaves; d++) {
        for (uint32_t k = 0; k < nUplinks; k++) {
            _upHooks[d * nUplinks + k].init(this, d, k);
        }
    }
    _downHook.init(this);
}

void
LeafSwitch::configure(simtime_picosec agingTime)
{
    _agingTime = agingTime;
}

void
LeafSwitch::setUplinkQueue(uint32_t k, DreQueue *q)
{
    _uplinks[k] = q;
}

PacketSink *
LeafSwitch::uplinkHook(uint32_t dstLeaf, uint32_t uplink)
{
    return &_upHooks[dstLeaf * _nUplinks + uplink];
}

bool
LeafSwitch::fresh(const Entry &e, simtime_picosec now) const
{
    return e.valid && now - e.updated <= _agingTime;
}

/*
 * Pick the uplink with the least congested path to dstLeaf. The path
 * congestion is the max of the local uplink DRE (first hop, measured here)
 * and the remote metric fed back by dstLeaf (whole path). Ties are broken
 * uniformly at random.
 */
uint32_t
LeafSwitch::pathScore(uint32_t dstLeaf, uint32_t uplink, simtime_picosec now)
{
    const Entry &e = _toLeaf[dstLeaf][uplink];
    uint32_t local = _uplinks[uplink]->congestionMetric();
    uint32_t remote = fresh(e, now) ? e.metric : 0;
    return max(local, remote);
}

uint32_t
LeafSwitch::chooseUplink(uint32_t dstLeaf, mt19937 &rng)
{
    simtime_picosec now = EventList::Get().now();

    uint32_t best = 0xff;
    uint32_t choice = 0;
    uint32_t nTies = 0;
    uint32_t sum = 0;

    for (uint32_t k = 0; k < _nUplinks; k++) {
        uint32_t score = pathScore(dstLeaf, k, now);
        sum += score;

        if (score < best) {
            best = score;
            choice = k;
            nTies = 1;
        } else if (score == best) {
            // Reservoir sampling: each tied uplink is chosen with equal probability.
            nTies++;
            if (rng() % nTies == 0) {
                choice = k;
            }
        }
    }

    _decisions++;
    _sumChosenScore += best;
    _sumMeanScore += sum / (double)_nUplinks;
    return choice;
}

void
LeafSwitch::recordChoice(uint32_t dstLeaf, uint32_t uplink)
{
    simtime_picosec now = EventList::Get().now();
    uint32_t sum = 0;
    for (uint32_t k = 0; k < _nUplinks; k++) {
        sum += pathScore(dstLeaf, k, now);
    }
    _decisions++;
    _sumChosenScore += pathScore(dstLeaf, uplink, now);
    _sumMeanScore += sum / (double)_nUplinks;
}

/*
 * Packet leaves this rack: encapsulate with the CONGA header and piggyback
 * one feedback entry for the destination leaf.
 */
void
LeafSwitch::onLeaveRack(Packet &pkt, uint32_t dstLeaf, uint32_t uplink)
{
    Packet::CongaHeader &h = pkt.conga;
    h.valid = true;
    h.srcLeaf = _id;
    h.dstLeaf = dstLeaf;
    h.lbTag = uplink;
    h.ce = 0;
    h.fbValid = false;

    attachFeedback(pkt, dstLeaf);
}

/*
 * Choose which Congestion-From-Leaf[dstLeaf][*] entry to send back to
 * dstLeaf. Entries that changed since they were last fed back go first;
 * otherwise cycle round-robin through the fresh entries.
 */
void
LeafSwitch::attachFeedback(Packet &pkt, uint32_t dstLeaf)
{
    simtime_picosec now = EventList::Get().now();
    vector<Entry> &row = _fromLeaf[dstLeaf];
    uint32_t start = _fbNext[dstLeaf];
    int pick = -1;

    for (uint32_t i = 0; i < _nUplinks; i++) {
        uint32_t k = (start + i) % _nUplinks;
        if (row[k].changed && fresh(row[k], now)) {
            pick = k;
            break;
        }
    }
    if (pick < 0) {
        for (uint32_t i = 0; i < _nUplinks; i++) {
            uint32_t k = (start + i) % _nUplinks;
            if (fresh(row[k], now)) {
                pick = k;
                break;
            }
        }
    }
    if (pick < 0) {
        return; // nothing (fresh) to report yet
    }

    pkt.conga.fbValid = true;
    pkt.conga.fbLbTag = pick;
    pkt.conga.fbMetric = row[pick].metric;
    row[pick].changed = false;
    _fbNext[dstLeaf] = (pick + 1) % _nUplinks;
    _fbSent++;
}

/*
 * Packet enters this rack from the fabric: record the path congestion it
 * measured and absorb the feedback it carries.
 */
void
LeafSwitch::onEnterRack(Packet &pkt)
{
    Packet::CongaHeader &h = pkt.conga;
    if (!h.valid || h.dstLeaf != _id) {
        return;
    }

    simtime_picosec now = EventList::Get().now();

    // Congestion-From-Leaf: path srcLeaf --(uplink lbTag)--> this leaf.
    Entry &from = _fromLeaf[h.srcLeaf][h.lbTag];
    if (!from.valid || from.metric != h.ce) {
        from.changed = true;
    }
    from.metric = h.ce;
    from.valid = true;
    from.updated = now;

    // Congestion-To-Leaf: srcLeaf reports on our path to it via fbLbTag.
    if (h.fbValid) {
        Entry &to = _toLeaf[h.srcLeaf][h.fbLbTag];
        to.metric = h.fbMetric;
        to.valid = true;
        to.updated = now;
        _fbReceived++;
    }

    h.valid = false; // decapsulated
}
