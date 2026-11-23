/*
 * CONGA leaf switch.
 *
 * htsim uses source routing (a packet follows a fixed list of queues and
 * pipes), so switches do not exist as objects. A LeafSwitch is therefore
 * attached to routes through two kinds of zero-delay "hooks":
 *
 *   uplink hook   (server -> leaf -> core):  packet leaves this rack.
 *       Encapsulate: set srcLeaf/dstLeaf/LBTag, reset CE, and piggyback one
 *       entry of the Congestion-From-Leaf table for the destination leaf.
 *
 *   downlink hook (core -> leaf -> server):  packet enters this rack.
 *       Decapsulate: store the packet's CE in Congestion-From-Leaf and the
 *       piggybacked feedback in Congestion-To-Leaf.
 *
 * New flows (treated as flowlets) pick the uplink that minimizes
 *   max(local DRE metric of the uplink, Congestion-To-Leaf[dst][uplink]).
 */
#ifndef LEAFSWITCH_H
#define LEAFSWITCH_H

#include "network.h"
#include "dre-queue.h"

#include <random>
#include <vector>

class LeafSwitch;

/* Route hook: packet leaving the rack toward core 'uplink', headed to 'dstLeaf'. */
class LeafUplinkHook : public PacketSink
{
    public:
        LeafUplinkHook() : _leaf(NULL), _dstLeaf(0), _uplink(0) {}
        void init(LeafSwitch *leaf, uint32_t dstLeaf, uint32_t uplink);
        void receivePacket(Packet &pkt);

    private:
        LeafSwitch *_leaf;
        uint32_t _dstLeaf;
        uint32_t _uplink;
};

/* Route hook: packet arriving at the rack from the fabric. */
class LeafDownlinkHook : public PacketSink
{
    public:
        LeafDownlinkHook() : _leaf(NULL) {}
        void init(LeafSwitch *leaf) { _leaf = leaf; }
        void receivePacket(Packet &pkt);

    private:
        LeafSwitch *_leaf;
};

class LeafSwitch
{
    public:
        LeafSwitch(uint32_t id, uint32_t nLeaves, uint32_t nUplinks);

        /* Metric entries not refreshed for this long are treated as 0. */
        static void configure(simtime_picosec agingTime);

        /* Register the DRE queue of uplink 'k' (leaf -> core k). */
        void setUplinkQueue(uint32_t k, DreQueue *q);

        /* CONGA decision for a new flow(let) from this leaf to 'dstLeaf'. */
        uint32_t chooseUplink(uint32_t dstLeaf, std::mt19937 &rng);

        /* Statistics only: record the path congestion of a choice made by
         * another scheme (ECMP). Does not affect any decision. */
        void recordChoice(uint32_t dstLeaf, uint32_t uplink);

        /* Hooks to insert into routes. */
        PacketSink *uplinkHook(uint32_t dstLeaf, uint32_t uplink);
        PacketSink *downlinkHook() { return &_downHook; }

        /* Called by the hooks. */
        void onLeaveRack(Packet &pkt, uint32_t dstLeaf, uint32_t uplink);
        void onEnterRack(Packet &pkt);

        uint32_t id() const { return _id; }

        // Statistics.
        uint64_t _decisions;     // uplink choices made
        uint64_t _fbSent;        // feedback entries piggybacked
        uint64_t _fbReceived;    // feedback entries received
        double _sumChosenScore;  // sum of path metric of the chosen uplink
        double _sumMeanScore;    // sum of mean path metric over all uplinks

    private:
        struct Entry {
            uint8_t metric;
            bool valid;
            bool changed;             // changed since last fed back (From table)
            simtime_picosec updated;  // last refresh time
        };

        bool fresh(const Entry &e, simtime_picosec now) const;
        uint32_t pathScore(uint32_t dstLeaf, uint32_t uplink, simtime_picosec now);
        void attachFeedback(Packet &pkt, uint32_t dstLeaf);

        uint32_t _id;
        uint32_t _nUplinks;

        std::vector<DreQueue*> _uplinks;               // local uplink DREs
        std::vector<std::vector<Entry> > _toLeaf;      // [dstLeaf][uplink]
        std::vector<std::vector<Entry> > _fromLeaf;    // [srcLeaf][uplink]
        std::vector<uint32_t> _fbNext;                 // round-robin pointer per remote leaf

        std::vector<LeafUplinkHook> _upHooks;          // [dstLeaf * nUplinks + uplink]
        LeafDownlinkHook _downHook;

        static simtime_picosec _agingTime;
};

#endif /* LEAFSWITCH_H */
