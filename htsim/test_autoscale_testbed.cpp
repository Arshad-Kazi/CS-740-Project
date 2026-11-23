/*
 * Elastic data-centre testbed: autoscaling a backend fleet under a traffic surge.
 *
 * TOPOLOGY  (leaf-spine, runtime-sized)
 *
 *                 [ core 0 ] ... [ core C-1 ]        40G, full bipartite mesh
 *                    |  \   \      /   /  |
 *                 [ leaf 0 ] ... [ leaf L-1 ]
 *                   /     \         /    \           10G access links
 *            backends   clients  backends clients
 *
 * Each leaf hosts backend-server slots and client slots. Clients are the
 * users; backends are the elastic fleet. A "request" is modelled only by its
 * response, since in a web/DC workload the response dominates: the backend is
 * the DataSource and the client is the DataSink, so the link that saturates
 * under load is the backend's egress (backend -> leaf). That queue is a
 * DreQueue, and its utilisation is the server's load signal.
 *
 * ELASTICITY
 *
 * htsim builds topology statically, so the full fleet is allocated up front
 * and ServerPool gates *admission*: only ACTIVE backends are handed to new
 * flows. FlowGenerator calls generateRoute once per flow at flow-creation
 * time, so a backend that becomes ACTIVE mid-run takes traffic immediately.
 * AutoScaler drives that set from the observed load.
 *
 * TWO INDEPENDENT LOAD-BALANCING LAYERS
 *
 *   --lb_server  which backend serves a request      (this testbed)
 *   --lb_path    which core carries it across the fabric  (ECMP vs CONGA,
 *                reusing leafswitch.cpp / dre-queue.cpp from lab 2)
 *
 * TIMESCALE -- IMPORTANT
 *
 * This is a packet-level simulator; simulating the 30-60s boot time of a real
 * VM at 10Gbps is computationally out of reach. Time is therefore compressed:
 * the boot delay defaults to 10ms, the control interval to 2ms, and the surge
 * lasts ~100ms. What matters for the result is the *ratio* of these --
 * boot delay >> control interval, and surge duration >> boot delay -- since
 * the lag/overshoot behaviour is scale-invariant. Read --bootdelay as "one
 * VM boot", not as ten milliseconds of wall-clock.
 */
#include "eventlist.h"
#include "logfile.h"
#include "loggers.h"
#include "fairqueue.h"
#include "flow-generator.h"
#include "pipe.h"
#include "test.h"
#include "dre-queue.h"
#include "leafswitch.h"
#include "server-pool.h"
#include "autoscaler.h"

#include <iomanip>
#include <random>
#include <vector>

namespace autoscale {

    /* ---- Topology dimensions (all set from the command line) ---- */
    uint32_t nLeaf = 8;              // leaf switches (racks)
    uint32_t nCore = 4;              // spine switches
    uint32_t backendsPerLeaf = 8;    // backend slots per rack (the max fleet)
    uint32_t clientsPerLeaf = 8;     // client slots per rack
    uint32_t maxBackends = 0;        // nLeaf * backendsPerLeaf
    uint32_t nClients = 0;           // nLeaf * clientsPerLeaf

    const uint64_t ACCESS_SPEED = 10000000000ULL; // 10G  server/client <-> leaf
    const uint64_t FABRIC_SPEED = 40000000000ULL; // 40G  leaf <-> core

    const uint64_t ACCESS_BUFFER = 512000;
    const uint64_t FABRIC_BUFFER = 1024000;
    const uint64_t NIC_BUFFER    = 8192000;

    const double LINK_DELAY = 0.1;   // per link, microseconds

    /* ---- Links. Indexed [leaf][slot], sized at runtime. ---- */
    std::vector<std::vector<DreQueue*> > qBackendLeaf;  // load sensor
    std::vector<std::vector<Pipe*> >     pBackendLeaf;
    std::vector<std::vector<Queue*> >    qLeafBackend;
    std::vector<std::vector<Pipe*> >     pLeafBackend;

    std::vector<std::vector<Queue*> >    qClientLeaf;
    std::vector<std::vector<Pipe*> >     pClientLeaf;
    std::vector<std::vector<Queue*> >    qLeafClient;
    std::vector<std::vector<Pipe*> >     pLeafClient;

    std::vector<std::vector<DreQueue*> > qLeafCore;     // [leaf][core]
    std::vector<std::vector<Pipe*> >     pLeafCore;
    std::vector<std::vector<DreQueue*> > qCoreLeaf;     // [core][leaf]
    std::vector<std::vector<Pipe*> >     pCoreLeaf;

    std::vector<LeafSwitch*> leafSwitch;

    enum PathLb { ECMP, CONGA };
    PathLb pathLb = ECMP;

    ServerPool *pool = NULL;
    AutoScaler *scaler = NULL;

    /* Path/server decisions draw from their own RNG so that runs with the
     * same --rngseed but a different policy see an identical workload
     * (flow sizes and arrival times come from the global rand() stream). */
    std::mt19937 lbRng;

    /* Backends and clients are laid out round-robin across leaves, so that
     * activating backends 0..k-1 spreads them over k distinct racks rather
     * than stacking them all behind one leaf. */
    inline uint32_t backendLeaf(uint32_t b) { return b % nLeaf; }
    inline uint32_t backendSlot(uint32_t b) { return b / nLeaf; }
    inline uint32_t clientLeaf(uint32_t c)  { return c % nLeaf; }
    inline uint32_t clientSlot(uint32_t c)  { return c / nLeaf; }

    void generateRoute(route_t *&fwd, route_t *&rev, uint32_t &src, uint32_t &dst);
    uint32_t chooseCore(uint32_t srcLeaf, uint32_t dstLeaf);
    void addFabricHops(route_t *r, uint32_t srcLeaf, uint32_t dstLeaf);

    Pipe *makePipe(const std::string &name);
    Queue *makeQueue(uint64_t speed, uint64_t buffer, const std::string &name);
    DreQueue *makeDreQueue(uint64_t speed, uint64_t buffer, const std::string &name);

    /* End-of-run summary. */
    class RunStats : public EventSource
    {
        public:
            RunStats(const std::string &tag) : EventSource("RunStats"), _tag(tag) {}
            void doNextEvent();
        private:
            std::string _tag;
    };
}

using namespace std;
using namespace autoscale;

void
autoscale_testbed(const ArgList &args, Logfile &logfile)
{
    (void)logfile; // per-queue tracing is off: it produces GBs of output.

    /* ---- Workload shape ---- */
    double Duration   = 0.30;   // seconds of flow generation
    double Drain      = 0.05;   // extra time for in-flight flows to finish
    double SurgeStart = 0.10;
    double SurgeEnd   = 0.20;
    double BaseLoad   = 24.0;   // Gbps offered before/after the surge
    double PeakLoad   = 72.0;   // Gbps offered during the surge
    uint32_t AvgFlowSize = 100000;
    string FlowDist = "uniform";
    string EndHost  = "dctcp";

    /* ---- Fleet and control ---- */
    uint32_t Leaves = 8, Cores = 4, Backends = 8, Clients = 8;
    uint32_t InitServers = 4;
    double BootDelayMs = 10.0;
    double ControlMs   = 2.0;
    string LbServer = "p2c";
    string LbPath   = "ecmp";
    string ScalePolicy = "target";
    double ScaleUp = 0.70, ScaleDown = 0.30, ScaleTarget = 0.60;
    uint32_t StepUp = 4, StepDown = 1;
    double CooldownUpMs = 0.0, CooldownDownMs = 20.0;
    uint32_t CountBooting = 1;
    uint32_t RngSeed = 1729;

    /* ---- CONGA / DRE parameters (as in lab 2) ---- */
    double DreTdreUs = 16, DreAlpha = 0.1, AgingUs = 1000;
    uint32_t DreQbits = 3;

    parseDouble(args, "duration", Duration);
    parseDouble(args, "drain", Drain);
    parseDouble(args, "surgestart", SurgeStart);
    parseDouble(args, "surgeend", SurgeEnd);
    parseDouble(args, "baseload", BaseLoad);
    parseDouble(args, "peakload", PeakLoad);
    parseInt(args, "flowsize", AvgFlowSize);
    parseString(args, "flowdist", FlowDist);
    parseString(args, "endhost", EndHost);

    parseInt(args, "leaves", Leaves);
    parseInt(args, "cores", Cores);
    parseInt(args, "backends", Backends);
    parseInt(args, "clients", Clients);
    parseInt(args, "init_servers", InitServers);
    parseDouble(args, "bootdelay", BootDelayMs);
    parseDouble(args, "control", ControlMs);
    parseString(args, "lb_server", LbServer);
    parseString(args, "lb_path", LbPath);
    parseString(args, "scaler", ScalePolicy);
    parseDouble(args, "scale_up", ScaleUp);
    parseDouble(args, "scale_down", ScaleDown);
    parseDouble(args, "scale_target", ScaleTarget);
    parseInt(args, "step_up", StepUp);
    parseInt(args, "step_down", StepDown);
    parseDouble(args, "cooldown_up", CooldownUpMs);
    parseDouble(args, "cooldown_down", CooldownDownMs);
    parseInt(args, "count_booting", CountBooting);
    parseInt(args, "rngseed", RngSeed);

    parseDouble(args, "dre_tdre", DreTdreUs);
    parseDouble(args, "dre_alpha", DreAlpha);
    parseInt(args, "dre_qbits", DreQbits);
    parseDouble(args, "conga_aging", AgingUs);

    /* ---- Validate ---- */
    if (Leaves < 1 || Cores < 1 || Backends < 1 || Clients < 1) {
        cerr << "--leaves/--cores/--backends/--clients must all be >= 1" << endl;
        exit(1);
    }
    nLeaf = Leaves;
    nCore = Cores;
    backendsPerLeaf = Backends;
    clientsPerLeaf = Clients;
    maxBackends = nLeaf * backendsPerLeaf;
    nClients = nLeaf * clientsPerLeaf;

    if (InitServers < 1 || InitServers > maxBackends) {
        cerr << "--init_servers must be between 1 and " << maxBackends
             << " (= leaves x backends)" << endl;
        exit(1);
    }
    if (SurgeEnd < SurgeStart) {
        cerr << "--surgeend must be >= --surgestart" << endl;
        exit(1);
    }
    if (PeakLoad < BaseLoad) {
        cerr << "--peakload must be >= --baseload" << endl;
        exit(1);
    }

    ServerPool::LbPolicy serverPolicy;
    if (!ServerPool::parsePolicy(LbServer, serverPolicy)) {
        cerr << "Unknown --lb_server=" << LbServer
             << " (expected rr, random, least or p2c)" << endl;
        exit(1);
    }

    AutoScaler::Policy scalerPolicy;
    if (!AutoScaler::parsePolicy(ScalePolicy, scalerPolicy)) {
        cerr << "Unknown --scaler=" << ScalePolicy
             << " (expected none, threshold or target)" << endl;
        exit(1);
    }

    if (LbPath == "ecmp") {
        pathLb = ECMP;
    } else if (LbPath == "conga") {
        pathLb = CONGA;
    } else {
        cerr << "Unknown --lb_path=" << LbPath << " (expected ecmp or conga)" << endl;
        exit(1);
    }

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

    lbRng.seed(RngSeed);
    DreQueue::configure(timeFromUs(DreTdreUs), DreAlpha, DreQbits);
    LeafSwitch::configure(timeFromUs(AgingUs));

    /* ---- Build the topology ---- */

    qBackendLeaf.assign(nLeaf, vector<DreQueue*>(backendsPerLeaf, NULL));
    pBackendLeaf.assign(nLeaf, vector<Pipe*>(backendsPerLeaf, NULL));
    qLeafBackend.assign(nLeaf, vector<Queue*>(backendsPerLeaf, NULL));
    pLeafBackend.assign(nLeaf, vector<Pipe*>(backendsPerLeaf, NULL));

    qClientLeaf.assign(nLeaf, vector<Queue*>(clientsPerLeaf, NULL));
    pClientLeaf.assign(nLeaf, vector<Pipe*>(clientsPerLeaf, NULL));
    qLeafClient.assign(nLeaf, vector<Queue*>(clientsPerLeaf, NULL));
    pLeafClient.assign(nLeaf, vector<Pipe*>(clientsPerLeaf, NULL));

    for (uint32_t l = 0; l < nLeaf; l++) {
        for (uint32_t s = 0; s < backendsPerLeaf; s++) {
            string id = to_string(l) + "-" + to_string(s);
            /* Backend egress: the elastic capacity, and the load sensor. */
            qBackendLeaf[l][s] = makeDreQueue(ACCESS_SPEED, NIC_BUFFER, "q-backend-leaf-" + id);
            pBackendLeaf[l][s] = makePipe("p-backend-leaf-" + id);
            qLeafBackend[l][s] = makeQueue(ACCESS_SPEED, ACCESS_BUFFER, "q-leaf-backend-" + id);
            pLeafBackend[l][s] = makePipe("p-leaf-backend-" + id);
        }
        for (uint32_t s = 0; s < clientsPerLeaf; s++) {
            string id = to_string(l) + "-" + to_string(s);
            /* Client NIC fair-shares its uplink (ACKs only, but keeps one
             * client's ACK stream from starving another's). */
            qClientLeaf[l][s] = new FairQueue(ACCESS_SPEED, NIC_BUFFER, NULL);
            qClientLeaf[l][s]->setName("q-client-leaf-" + id);
            pClientLeaf[l][s] = makePipe("p-client-leaf-" + id);
            qLeafClient[l][s] = makeQueue(ACCESS_SPEED, ACCESS_BUFFER, "q-leaf-client-" + id);
            pLeafClient[l][s] = makePipe("p-leaf-client-" + id);
        }
    }

    qLeafCore.assign(nLeaf, vector<DreQueue*>(nCore, NULL));
    pLeafCore.assign(nLeaf, vector<Pipe*>(nCore, NULL));
    qCoreLeaf.assign(nCore, vector<DreQueue*>(nLeaf, NULL));
    pCoreLeaf.assign(nCore, vector<Pipe*>(nLeaf, NULL));

    for (uint32_t l = 0; l < nLeaf; l++) {
        for (uint32_t c = 0; c < nCore; c++) {
            string id = to_string(l) + "-" + to_string(c);
            qLeafCore[l][c] = makeDreQueue(FABRIC_SPEED, FABRIC_BUFFER, "q-leaf-core-" + id);
            pLeafCore[l][c] = makePipe("p-leaf-core-" + id);
            qCoreLeaf[c][l] = makeDreQueue(FABRIC_SPEED, FABRIC_BUFFER, "q-core-leaf-" + id);
            pCoreLeaf[c][l] = makePipe("p-core-leaf-" + id);
        }
    }

    leafSwitch.assign(nLeaf, NULL);
    for (uint32_t l = 0; l < nLeaf; l++) {
        leafSwitch[l] = new LeafSwitch(l, nLeaf, nCore);
        for (uint32_t c = 0; c < nCore; c++) {
            leafSwitch[l]->setUplinkQueue(c, qLeafCore[l][c]);
        }
    }

    /* ---- Elastic fleet ---- */

    pool = new ServerPool(maxBackends, InitServers, timeFromMs(BootDelayMs));
    pool->setPolicy(serverPolicy);
    for (uint32_t b = 0; b < maxBackends; b++) {
        pool->setEgressQueue(b, qBackendLeaf[backendLeaf(b)][backendSlot(b)]);
    }

    simtime_picosec genEnd = timeFromSec(Duration);
    simtime_picosec endTime = timeFromSec(Duration + Drain);

    scaler = new AutoScaler(pool, scalerPolicy, timeFromMs(ControlMs));
    scaler->setThresholds(ScaleUp, ScaleDown);
    scaler->setTarget(ScaleTarget);
    scaler->setStep(StepUp, StepDown);
    scaler->setCooldown(timeFromMs(CooldownUpMs), timeFromMs(CooldownDownMs));
    scaler->setCountBooting(CountBooting != 0);
    scaler->start(timeFromMs(ControlMs), genEnd);

    /* ---- Traffic: a baseline generator plus a surge generator ----
     *
     * A step surge needs no change to FlowGenerator: run a second generator
     * over [surgestart, surgeend] carrying the extra offered load. The two
     * compose to base -> peak -> base. */

    FlowGenerator *baseGen =
        new FlowGenerator(eh, generateRoute, speedFromGbps(BaseLoad), AvgFlowSize, fd);
    baseGen->setPrefix("base-");
    baseGen->setTimeLimits(timeFromUs(1), genEnd);

    double surgeDelta = PeakLoad - BaseLoad;
    if (surgeDelta > 0 && SurgeEnd > SurgeStart) {
        FlowGenerator *surgeGen =
            new FlowGenerator(eh, generateRoute, speedFromGbps(surgeDelta), AvgFlowSize, fd);
        surgeGen->setPrefix("surge-");
        surgeGen->setTimeLimits(timeFromSec(SurgeStart), timeFromSec(SurgeEnd));
    }

    EventList::Get().setEndtime(endTime);

    RunStats *stats = new RunStats(LbServer + "/" + ScalePolicy + "/" + LbPath);
    EventList::Get().sourceIsPending(*stats, endTime - 1);

    /* ---- Banner: everything needed to reproduce the run ---- */
    double peakCapacity = maxBackends * speedAsGbps(ACCESS_SPEED);
    double clientCapacity = nClients * speedAsGbps(ACCESS_SPEED);

    cerr << "autoscale_testbed:"
         << " leaves=" << nLeaf << " cores=" << nCore
         << " backends=" << maxBackends << " clients=" << nClients
         << " init_servers=" << InitServers
         << " lb_server=" << LbServer << " lb_path=" << LbPath
         << " scaler=" << ScalePolicy << " target=" << ScaleTarget
         << " step_up=" << StepUp << " boot=" << BootDelayMs << "ms"
         << " control=" << ControlMs << "ms"
         << " count_booting=" << CountBooting
         << " load=" << BaseLoad << "->" << PeakLoad << "Gbps"
         << " surge=[" << SurgeStart << "," << SurgeEnd << "]s"
         << " dur=" << Duration << "s drain=" << Drain << "s"
         << " flowdist=" << FlowDist << " endhost=" << EndHost << endl;

    cerr << "  fleet capacity: " << InitServers * speedAsGbps(ACCESS_SPEED)
         << "Gbps active, " << peakCapacity << "Gbps max; "
         << "peak offered " << PeakLoad << "Gbps needs >= "
         << (uint32_t)ceil(PeakLoad / speedAsGbps(ACCESS_SPEED)) << " servers" << endl;

    if (PeakLoad > peakCapacity) {
        cerr << "  WARNING: peak load exceeds the whole fleet ("
             << peakCapacity << "Gbps). Raise --backends or lower --peakload."
             << endl;
    }
    if (PeakLoad > clientCapacity) {
        cerr << "  WARNING: peak load exceeds client capacity ("
             << clientCapacity << "Gbps); clients, not backends, will be the "
             << "bottleneck. Raise --clients." << endl;
    }
}

/*
 * Called by each FlowGenerator for every new flow.
 *
 * The load balancer picks the backend here, at flow-creation time, from the
 * pool's current ACTIVE set -- which is how a server that booted moments ago
 * starts serving without the topology ever changing.
 */
void
autoscale::generateRoute(route_t *&fwd,
                         route_t *&rev,
                         uint32_t &src,
                         uint32_t &dst)
{
    int b = pool->select(lbRng);
    if (b < 0) {
        b = 0; // pool never empties, but never dereference -1
    }
    uint32_t backend = (uint32_t)b;
    uint32_t client = lbRng() % nClients;

    /* Node ids for the log: backends 0..maxBackends-1, clients after them. */
    src = backend;
    dst = maxBackends + client;

    uint32_t srcLeaf = backendLeaf(backend), srcSlot = backendSlot(backend);
    uint32_t dstLeaf = clientLeaf(client),   dstSlot = clientSlot(client);

    fwd = new route_t();
    rev = new route_t();

    /* Forward = the response: backend -> leaf -> [fabric] -> leaf -> client. */
    fwd->push_back(qBackendLeaf[srcLeaf][srcSlot]);
    fwd->push_back(pBackendLeaf[srcLeaf][srcSlot]);
    addFabricHops(fwd, srcLeaf, dstLeaf);
    fwd->push_back(qLeafClient[dstLeaf][dstSlot]);
    fwd->push_back(pLeafClient[dstLeaf][dstSlot]);

    /* Reverse = the ACKs, load balanced independently by its source leaf. */
    rev->push_back(qClientLeaf[dstLeaf][dstSlot]);
    rev->push_back(pClientLeaf[dstLeaf][dstSlot]);
    addFabricHops(rev, dstLeaf, srcLeaf);
    rev->push_back(qLeafBackend[srcLeaf][srcSlot]);
    rev->push_back(pLeafBackend[srcLeaf][srcSlot]);
}

/*
 * Append leaf -> core -> leaf hops, if the two racks differ. The leaf hooks
 * are zero-delay route elements where the CONGA logic sees the packet.
 */
void
autoscale::addFabricHops(route_t *r, uint32_t srcLeaf, uint32_t dstLeaf)
{
    if (srcLeaf == dstLeaf) {
        return; // same rack: the leaf switches locally
    }

    uint32_t c = chooseCore(srcLeaf, dstLeaf);
    r->push_back(leafSwitch[srcLeaf]->uplinkHook(dstLeaf, c));
    r->push_back(qLeafCore[srcLeaf][c]);
    r->push_back(pLeafCore[srcLeaf][c]);
    r->push_back(qCoreLeaf[c][dstLeaf]);
    r->push_back(pCoreLeaf[c][dstLeaf]);
    r->push_back(leafSwitch[dstLeaf]->downlinkHook());
}

uint32_t
autoscale::chooseCore(uint32_t srcLeaf, uint32_t dstLeaf)
{
    if (pathLb == CONGA) {
        return leafSwitch[srcLeaf]->chooseUplink(dstLeaf, lbRng);
    }
    uint32_t core = lbRng() % nCore;
    leafSwitch[srcLeaf]->recordChoice(dstLeaf, core); // statistics only
    return core;
}

void
autoscale::RunStats::doNextEvent()
{
    /* Server-seconds: the cost of the scaling decisions over the run. */
    double serverSec = pool->serverSeconds();

    uint64_t fabricBytes = 0;
    for (uint32_t l = 0; l < nLeaf; l++) {
        for (uint32_t c = 0; c < nCore; c++) {
            fabricBytes += qLeafCore[l][c]->bytesSent();
        }
    }

    /* Imbalance across the active fleet: max/mean bytes served. 1.0 is a
     * perfectly even spread; higher means the server LB left hot spots. */
    uint64_t mx = 0, sum = 0;
    uint32_t used = 0;
    for (uint32_t b = 0; b < maxBackends; b++) {
        uint64_t sent = qBackendLeaf[backendLeaf(b)][backendSlot(b)]->bytesSent();
        if (sent > 0) {
            used++;
            sum += sent;
            if (sent > mx) {
                mx = sent;
            }
        }
    }
    double imbalance = (used > 0 && sum > 0) ? mx / (sum / (double)used) : 0.0;

    cout << fixed << setprecision(4)
         << "RunStats tag " << _tag
         << " final_active " << pool->nActive()
         << " max_servers " << pool->maxServers()
         << " starts " << pool->_starts
         << " stops " << pool->_stops
         << " server_seconds " << serverSec
         << " dispatches " << pool->_dispatches
         << " servers_used " << used
         << " backend_maxmean " << imbalance
         << " fabric_bytes " << fabricBytes
         << " scaler_ticks " << scaler->_ticks
         << " scale_ups " << scaler->_upActions
         << " scale_downs " << scaler->_downActions
         << endl;
}

Pipe *
autoscale::makePipe(const string &name)
{
    Pipe *p = new Pipe(timeFromUs(LINK_DELAY));
    p->setName(name);
    return p;
}

Queue *
autoscale::makeQueue(uint64_t speed, uint64_t buffer, const string &name)
{
    Queue *q = new Queue(speed, buffer, NULL);
    q->setName(name);
    return q;
}

DreQueue *
autoscale::makeDreQueue(uint64_t speed, uint64_t buffer, const string &name)
{
    DreQueue *q = new DreQueue(speed, buffer, NULL);
    q->setName(name);
    return q;
}
