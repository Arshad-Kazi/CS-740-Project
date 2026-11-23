
#include "eventlist.h"
#include "logfile.h"
#include "loggers.h"
#include "aprx-fairqueue.h"
#include "fairqueue.h"
#include "priorityqueue.h"
#include "stoc-fairqueue.h"
#include "flow-generator.h"
#include "pipe.h"
#include "test.h"
#include "dre-queue.h"
#include "leafswitch.h"
#include <random>

namespace conga {

    // testbed configuration
    const int N_CORE = 12;     // Maximum (and default) number of core switches
    const int N_LEAF = 24;
    const int N_SERVER = 32;   // Per leaf
    const int N_NODES = N_LEAF * N_SERVER;

    const uint64_t LEAF_BUFFER = 512000;    // leaf output ports (to core, to server)
    const uint64_t CORE_BUFFER = 1024000;   // core output ports (to leaf)
    const uint64_t ENDH_BUFFER = 8192000;   // server NIC

    const uint64_t LEAF_SPEED = 10000000000; // 10gbps  (server <-> leaf)
    const uint64_t CORE_SPEED = 40000000000; // 40gbps  (leaf <-> core)

    const double LINK_DELAY = 0.1;           // per-link propagation delay, in microsec

    enum LoadBalancer { ECMP, CONGA };
    LoadBalancer lbMode = ECMP;

    // Number of core switches actually used (--cores). The lab configuration
    // is 12. Fewer cores means fewer leaf uplinks: with 4 cores each leaf has
    // 4 x 40G = 160G of uplinks for 320G of servers (2:1 oversubscription,
    // like the CONGA paper's testbed).
    int nCores = N_CORE;

    // What --utilization is a fraction of (--loadbase):
    //   server: total server link capacity (768 x 10G). The default.
    //   core:   total leaf-uplink capacity (24 x nCores x 40G), corrected for
    //           the flows that stay inside their rack. This is how the CONGA
    //           paper defines load.
    std::string loadBase = "server";

    // Separate RNG for path selection, so that ECMP and CONGA runs with the
    // same --rngseed see exactly the same workload (sizes, arrivals, src/dst),
    // which is drawn from the global rand() stream.
    std::mt19937 pathRng;

    // Server <-> leaf links, indexed [leaf][server-on-that-leaf].
    Queue *qServerLeaf[N_LEAF][N_SERVER];
    Pipe  *pServerLeaf[N_LEAF][N_SERVER];
    Queue *qLeafServer[N_LEAF][N_SERVER];
    Pipe  *pLeafServer[N_LEAF][N_SERVER];

    // Leaf <-> core (fabric) links. These carry a DRE so CONGA can measure
    // their congestion. The DRE does not change queueing behaviour, so ECMP
    // runs are unaffected.
    DreQueue *qLeafCore[N_LEAF][N_CORE];   // leaf L uplink to core C
    Pipe     *pLeafCore[N_LEAF][N_CORE];
    DreQueue *qCoreLeaf[N_CORE][N_LEAF];   // core C downlink to leaf L
    Pipe     *pCoreLeaf[N_CORE][N_LEAF];

    // CONGA state (tables, feedback) lives in the leaf switches.
    LeafSwitch *leafSwitch[N_LEAF];

    void generateRoute(route_t *&fwd, route_t *&rev, uint32_t &src, uint32_t &dst);
    uint32_t chooseCore(uint32_t srcLeaf, uint32_t dstLeaf);
    Queue *makeQueue(uint64_t speed, uint64_t buffer, const std::string &name);
    DreQueue *makeFabricQueue(uint64_t speed, uint64_t buffer, const std::string &name);
    Pipe  *makePipe(const std::string &name);

    /* Prints fabric load-balance statistics at the end of the simulation. */
    class FabricStats : public EventSource
    {
        public:
            FabricStats(const std::string &lb) : EventSource("FabricStats"), _lb(lb) {}
            void doNextEvent();
        private:
            std::string _lb;
    };
}

using namespace std;
using namespace conga;

void
conga_testbed(const ArgList &args, Logfile &logfile)
{
    (void)logfile; // per-queue logging disabled: it produces GBs of trace output.

    double Duration = 0.1;          // seconds during which new flows are generated
    double Drain = 0.1;             // extra seconds to let in-flight flows finish
    double Utilization = 0.5;       // offered load, as a fraction of --loadbase capacity
    uint32_t Cores = N_CORE;
    uint32_t AvgFlowSize = 100000;  // only used by uniform / pareto
    uint32_t RngSeed = 1729;
    string FlowDist = "uniform";
    string EndHost = "dctcp";
    string LB = "ecmp";

    // CONGA parameters: DRE time constant tau = Tdre / alpha = 160us,
    // 3-bit congestion metric, table entries age out after 1ms.
    double DreTdreUs = 16;
    double DreAlpha = 0.1;
    uint32_t DreQbits = 3;
    double AgingUs = 1000;

    parseDouble(args, "duration", Duration);
    parseDouble(args, "drain", Drain);
    parseDouble(args, "utilization", Utilization);
    parseInt(args, "flowsize", AvgFlowSize);
    parseInt(args, "rngseed", RngSeed);
    parseString(args, "flowdist", FlowDist);
    parseString(args, "endhost", EndHost);
    parseString(args, "lb", LB);
    parseDouble(args, "dre_tdre", DreTdreUs);
    parseDouble(args, "dre_alpha", DreAlpha);
    parseInt(args, "dre_qbits", DreQbits);
    parseDouble(args, "conga_aging", AgingUs);
    parseInt(args, "cores", Cores);
    parseString(args, "loadbase", loadBase);

    if (Cores < 1 || Cores > (uint32_t)N_CORE) {
        cerr << "--cores must be between 1 and " << N_CORE << endl;
        exit(1);
    }
    nCores = Cores;
    if (loadBase != "server" && loadBase != "core") {
        cerr << "Unknown --loadbase=" << loadBase << " (expected server or core)" << endl;
        exit(1);
    }

    if (LB == "ecmp") {
        lbMode = ECMP;
    } else if (LB == "conga") {
        lbMode = CONGA;
    } else {
        cerr << "Unknown --lb=" << LB << " (expected ecmp or conga)" << endl;
        exit(1);
    }
    pathRng.seed(RngSeed);
    DreQueue::configure(timeFromUs(DreTdreUs), DreAlpha, DreQbits);
    LeafSwitch::configure(timeFromUs(AgingUs));

    // ---------------- Build the topology ----------------

    // Server <-> leaf.
    for (int l = 0; l < N_LEAF; l++) {
        for (int s = 0; s < N_SERVER; s++) {
            string id = to_string(l) + "-" + to_string(s);

            // Server NIC: fair-shares the 10G link among the server's flows.
            qServerLeaf[l][s] = new FairQueue(LEAF_SPEED, ENDH_BUFFER, NULL);
            qServerLeaf[l][s]->setName("q-server-leaf-" + id);
            pServerLeaf[l][s] = makePipe("p-server-leaf-" + id);

            qLeafServer[l][s] = makeQueue(LEAF_SPEED, LEAF_BUFFER, "q-leaf-server-" + id);
            pLeafServer[l][s] = makePipe("p-leaf-server-" + id);
        }
    }

    // Leaf <-> core: full bipartite mesh, one 40G link between every pair.
    for (int l = 0; l < N_LEAF; l++) {
        for (int c = 0; c < nCores; c++) {
            string id = to_string(l) + "-" + to_string(c);

            qLeafCore[l][c] = makeFabricQueue(CORE_SPEED, LEAF_BUFFER, "q-leaf-core-" + id);
            pLeafCore[l][c] = makePipe("p-leaf-core-" + id);

            qCoreLeaf[c][l] = makeFabricQueue(CORE_SPEED, CORE_BUFFER, "q-core-leaf-" + id);
            pCoreLeaf[c][l] = makePipe("p-core-leaf-" + id);
        }
    }

    // Leaf switches: each one knows the DREs of its own uplinks (one per core).
    for (int l = 0; l < N_LEAF; l++) {
        leafSwitch[l] = new LeafSwitch(l, N_LEAF, nCores);
        for (int c = 0; c < nCores; c++) {
            leafSwitch[l]->setUplinkQueue(c, qLeafCore[l][c]);
        }
    }

    // ---------------- Workload ----------------

    DataSource::EndHost eh = DataSource::TCP;
    if (EndHost == "dctcp") {
        eh = DataSource::DCTCP;
    } else if (EndHost != "tcp") {
        cerr << "Unsupported --endhost=" << EndHost << " (expected tcp or dctcp)" << endl;
        exit(1);
    }

    Workloads::FlowDist fd = Workloads::UNIFORM;
    if (FlowDist == "pareto") {
        fd = Workloads::PARETO;
    } else if (FlowDist == "enterprise") {
        fd = Workloads::ENTERPRISE;
    } else if (FlowDist == "datamining") {
        fd = Workloads::DATAMINING;
    }

    // Offered load (bits/s for all servers together).
    //   server: at load X every server sends, on average, X * 10Gbps. With 12
    //           cores the leaf uplinks have 1.5x more capacity than the server
    //           links, so the fabric is never the limit on offered load.
    //   core:   at load X the leaf uplinks carry, on average, X of their
    //           capacity. Destinations are uniform over the other servers, so
    //           only (N_NODES - N_SERVER) / (N_NODES - 1) of the traffic
    //           leaves its rack; the rate is scaled up to compensate.
    double serverCapacity = (double)N_NODES * LEAF_SPEED;
    double flowRate;
    if (loadBase == "core") {
        double crossRack = (double)(N_NODES - N_SERVER) / (N_NODES - 1);
        flowRate = Utilization * N_LEAF * nCores * (double)CORE_SPEED / crossRack;
    } else {
        flowRate = Utilization * serverCapacity;
    }

    cerr << "conga_testbed: lb=" << LB << " flowdist=" << FlowDist
         << " utilization=" << Utilization << " loadbase=" << loadBase
         << " cores=" << nCores << " duration=" << Duration
         << "s drain=" << Drain << "s endhost=" << EndHost << endl;
    if (flowRate > serverCapacity) {
        cerr << "WARNING: offered load is " << flowRate / serverCapacity * 100
             << "% of the server link capacity; servers cannot send this much." << endl;
    }

    FlowGenerator *flowGen = new FlowGenerator(eh, generateRoute, llround(flowRate), AvgFlowSize, fd);
    flowGen->setTimeLimits(timeFromUs(1), timeFromSec(Duration));

    simtime_picosec endTime = timeFromSec(Duration + Drain);
    EventList::Get().setEndtime(endTime);

    FabricStats *stats = new FabricStats(LB);
    EventList::Get().sourceIsPending(*stats, endTime - 1);
}

/*
 * Called by the FlowGenerator for every new flow: picks a random
 * source/destination server pair and builds the forward (data) and
 * reverse (ACK) routes through the leaf-spine fabric.
 */
void
conga::generateRoute(route_t *&fwd,
                     route_t *&rev,
                     uint32_t &src,
                     uint32_t &dst)
{
    src = rand() % N_NODES;
    dst = rand() % (N_NODES - 1);
    if (dst >= src) {
        dst++;
    }

    uint32_t srcLeaf = src / N_SERVER, srcSvr = src % N_SERVER;
    uint32_t dstLeaf = dst / N_SERVER, dstSvr = dst % N_SERVER;

    fwd = new route_t();
    rev = new route_t();

    // First hop: server NIC -> its leaf.
    fwd->push_back(qServerLeaf[srcLeaf][srcSvr]);
    fwd->push_back(pServerLeaf[srcLeaf][srcSvr]);
    rev->push_back(qServerLeaf[dstLeaf][dstSvr]);
    rev->push_back(pServerLeaf[dstLeaf][dstSvr]);

    // Different racks: leaf -> core -> leaf. Each direction is load balanced
    // independently by its own source leaf, so data and ACKs may take
    // different cores.
    //
    // The leaf hooks are zero-delay route elements where the leaf switch sees
    // the packet: the uplink hook encapsulates (LBTag, CE=0, feedback), the
    // DRE queues update CE, and the downlink hook at the destination leaf
    // records CE and absorbs the feedback.
    if (srcLeaf != dstLeaf) {
        uint32_t cFwd = chooseCore(srcLeaf, dstLeaf);
        fwd->push_back(leafSwitch[srcLeaf]->uplinkHook(dstLeaf, cFwd));
        fwd->push_back(qLeafCore[srcLeaf][cFwd]);
        fwd->push_back(pLeafCore[srcLeaf][cFwd]);
        fwd->push_back(qCoreLeaf[cFwd][dstLeaf]);
        fwd->push_back(pCoreLeaf[cFwd][dstLeaf]);
        fwd->push_back(leafSwitch[dstLeaf]->downlinkHook());

        uint32_t cRev = chooseCore(dstLeaf, srcLeaf);
        rev->push_back(leafSwitch[dstLeaf]->uplinkHook(srcLeaf, cRev));
        rev->push_back(qLeafCore[dstLeaf][cRev]);
        rev->push_back(pLeafCore[dstLeaf][cRev]);
        rev->push_back(qCoreLeaf[cRev][srcLeaf]);
        rev->push_back(pCoreLeaf[cRev][srcLeaf]);
        rev->push_back(leafSwitch[srcLeaf]->downlinkHook());
    }

    // Last hop: leaf -> destination server.
    fwd->push_back(qLeafServer[dstLeaf][dstSvr]);
    fwd->push_back(pLeafServer[dstLeaf][dstSvr]);
    rev->push_back(qLeafServer[srcLeaf][srcSvr]);
    rev->push_back(pLeafServer[srcLeaf][srcSvr]);
}

/* Which core should traffic from srcLeaf to dstLeaf use? */
uint32_t
conga::chooseCore(uint32_t srcLeaf, uint32_t dstLeaf)
{
    switch (lbMode) {
        case CONGA:
            // Least congested path according to the source leaf's
            // Congestion-To-Leaf table and its local uplink DREs.
            return leafSwitch[srcLeaf]->chooseUplink(dstLeaf, pathRng);
        case ECMP:
        default: {
            // Uniform random core per flow (= hashing the flow's 5-tuple).
            uint32_t core = pathRng() % nCores;
            // Statistics only (no effect on the choice): how congested was
            // the path ECMP picked, according to the CONGA tables?
            leafSwitch[srcLeaf]->recordChoice(dstLeaf, core);
            return core;
        }
    }
}

Queue *
conga::makeQueue(uint64_t speed, uint64_t buffer, const string &name)
{
    Queue *q = new Queue(speed, buffer, NULL);
    q->setName(name);
    return q;
}

DreQueue *
conga::makeFabricQueue(uint64_t speed, uint64_t buffer, const string &name)
{
    DreQueue *q = new DreQueue(speed, buffer, NULL);
    q->setName(name);
    return q;
}

/*
 * Load-balance quality of the fabric over the whole run: for each leaf,
 * max/mean bytes across its uplinks (1.0 = perfectly balanced), and the
 * same for each core's downlinks. Printed as a single "FabricStats" line.
 */
void
conga::FabricStats::doNextEvent()
{
    double upSum = 0, upWorst = 0, downSum = 0, downWorst = 0;
    uint64_t totalBytes = 0;
    int nUp = 0, nDown = 0;

    for (int l = 0; l < N_LEAF; l++) {
        uint64_t mx = 0, sum = 0;
        for (int c = 0; c < nCores; c++) {
            uint64_t b = qLeafCore[l][c]->bytesSent();
            mx = max(mx, b);
            sum += b;
        }
        totalBytes += sum;
        if (sum > 0) {
            double r = mx / (sum / (double)nCores);
            upSum += r;
            upWorst = max(upWorst, r);
            nUp++;
        }
    }

    // Downlinks into each leaf, compared across cores.
    for (int l = 0; l < N_LEAF; l++) {
        uint64_t mx = 0, sum = 0;
        for (int c = 0; c < nCores; c++) {
            uint64_t b = qCoreLeaf[c][l]->bytesSent();
            mx = max(mx, b);
            sum += b;
        }
        if (sum > 0) {
            double r = mx / (sum / (double)nCores);
            downSum += r;
            downWorst = max(downWorst, r);
            nDown++;
        }
    }

    uint64_t decisions = 0, fbSent = 0, fbRecv = 0;
    double chosen = 0, mean = 0;
    for (int l = 0; l < N_LEAF; l++) {
        decisions += leafSwitch[l]->_decisions;
        fbSent += leafSwitch[l]->_fbSent;
        fbRecv += leafSwitch[l]->_fbReceived;
        chosen += leafSwitch[l]->_sumChosenScore;
        mean += leafSwitch[l]->_sumMeanScore;
    }

    cout << "FabricStats lb " << _lb
         << " fabric_bytes " << totalBytes
         << " uplink_maxmean_avg " << (nUp ? upSum / nUp : 0)
         << " uplink_maxmean_worst " << upWorst
         << " downlink_maxmean_avg " << (nDown ? downSum / nDown : 0)
         << " downlink_maxmean_worst " << downWorst
         << " path_decisions " << decisions
         << " avg_chosen_path_metric " << (decisions ? chosen / decisions : 0)
         << " avg_path_metric " << (decisions ? mean / decisions : 0)
         << " feedback_sent " << fbSent
         << " feedback_recv " << fbRecv
         << " cores " << nCores
         << " loadbase " << loadBase << endl;
}

Pipe *
conga::makePipe(const string &name)
{
    Pipe *p = new Pipe(timeFromUs(LINK_DELAY));
    p->setName(name);
    return p;
}
