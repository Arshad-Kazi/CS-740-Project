/*
 * GPU inference cluster: peer-assisted warm-up vs central storage.
 *
 * A GPU node cannot serve until the model weights are resident. Getting them
 * there is a network transfer that competes with live serving traffic, so
 * "time to add capacity" is a measured outcome, not a parameter. The question
 * this testbed exists to answer: when a surge arrives, is it better to pull
 * weights from slow central storage, or from a peer that already has them
 * but is busy serving?
 *
 * TOPOLOGY
 *
 *                   [ core 0 ] ... [ core C-1 ]          40G mesh
 *                      /  |  \        /  |  \
 *          [ rack 0 ] ... [ rack R-1 ]   [ storage leaf ]
 *            /     \                          |
 *        GPUs    clients                 storage nodes
 *       (elastic) (users)               (read-rate limited)
 *
 * Inside a rack the GPUs additionally share an NVSwitch-class interconnect
 * (~10x the NIC), which never touches the leaf or the fabric.
 *
 * THREE WAYS TO WARM A NODE, WITH DIFFERENT COSTS
 *
 *   central storage    TcpSrc -> [read rate] -> [storage NIC] -> fabric -> node
 *                      Slow, but costs the serving fleet nothing.
 *
 *   same-rack peer     TcpSrc -> [NVLink egress] -> [NVLink ingress] -> node
 *                      Fast, bypasses leaf and fabric entirely, and does not
 *                      touch the donor's NIC -- so it is nearly free.
 *
 *   cross-rack peer    TcpSrc -> [donor NIC] -> fabric -> node
 *                      Fast, but shares the donor's NIC with its own serving
 *                      traffic. Donating costs the donor.
 *
 * Note what is deliberately absent from the peer paths: the donor's compute
 * queue. Reading weights out of HBM and pushing them over the wire is modelled
 * as DMA that bypasses the GPU's serving pipeline, so a donor loses bandwidth
 * but not compute.
 *
 * SEEDING IS THE RATE LIMIT
 *
 * At the start of a surge nobody has the weights, so the first pull into each
 * rack must come from storage. Peer distribution is an epidemic; how fast it
 * spreads depends on how many seeds you buy and how they are placed.
 *
 * TIMESCALE: compressed, as in expt 4. A 50 MB "model" at 4 Gbps storage
 * reproduces the dynamics of a 140 GB model at ~20 GB/s -- what matters is
 * the ratio of warm-up time to surge duration, not the absolute sizes, which
 * are far beyond packet-level simulation (140 GB is 93 million packets).
 */
#include "eventlist.h"
#include "logfile.h"
#include "loggers.h"
#include "fairqueue.h"
#include "flow-generator.h"
#include "pipe.h"
#include "test.h"
#include "dre-queue.h"
#include "gpu-fleet.h"
#include "warmup-policy.h"
#include "warmup-manager.h"
#include "autoscaler.h"

#include <iomanip>
#include <random>
#include <vector>

namespace gpuwarm {

    /* ---- Dimensions (set from the command line) ---- */
    uint32_t nRacks = 8;
    uint32_t nCore = 4;
    uint32_t gpusPerRack = 8;
    uint32_t clientsPerRack = 8;
    uint32_t nStorage = 4;
    uint32_t maxNodes = 0;       // nRacks * gpusPerRack
    uint32_t nClients = 0;       // nRacks * clientsPerRack
    uint32_t storageLeaf = 0;    // leaf index of the storage rack (== nRacks)

    const uint64_t NIC_SPEED    = 10000000000ULL;  // 10G  node/client <-> leaf
    const uint64_t FABRIC_SPEED = 40000000000ULL;  // 40G  leaf <-> core

    uint64_t nvlinkSpeed  = 100000000000ULL;       // 100G intra-rack
    uint64_t computeSpeed = 4000000000ULL;         // set from --compute_ratio
    uint64_t storageRead  = 4000000000ULL;         // --storage_read

    const uint64_t NIC_BUFFER     = 8192000;
    const uint64_t LEAF_BUFFER    = 512000;
    const uint64_t FABRIC_BUFFER  = 1024000;
    const uint64_t COMPUTE_BUFFER = 8192000;
    const uint64_t NVLINK_BUFFER  = 4096000;

    const double LINK_DELAY   = 0.1;   // us, per fabric/access link
    const double NVLINK_DELAY = 0.02;  // us, intra-rack

    /* ---- Per-GPU-node links ---- */
    std::vector<DreQueue*> qCompute;                 // capacity + load sensor
    std::vector<Queue*> qNicOut;   std::vector<Pipe*> pNicOut;
    std::vector<Queue*> qNicIn;    std::vector<Pipe*> pNicIn;
    std::vector<Queue*> qNvOut;    std::vector<Pipe*> pNvOut;
    std::vector<Queue*> qNvIn;     std::vector<Pipe*> pNvIn;

    /* ---- Clients ---- */
    std::vector<std::vector<Queue*> > qCliOut, qCliIn;
    std::vector<std::vector<Pipe*> >  pCliOut, pCliIn;

    /* ---- Storage ---- */
    std::vector<DreQueue*> qStoRead;                 // the slow device
    std::vector<Queue*> qStoOut;   std::vector<Pipe*> pStoOut;
    std::vector<Queue*> qStoIn;    std::vector<Pipe*> pStoIn;
    uint32_t storageRR = 0;

    /* ---- Fabric: leaves 0..nRacks-1 are GPU racks, nRacks is storage ---- */
    std::vector<std::vector<DreQueue*> > qLeafCore, qCoreLeaf;
    std::vector<std::vector<Pipe*> >     pLeafCore, pCoreLeaf;

    GpuFleet *fleet = NULL;
    WarmupManager *warmup = NULL;
    AutoScaler *scaler = NULL;
    std::mt19937 rng;

    void servingRoute(route_t *&fwd, route_t *&rev, uint32_t &src, uint32_t &dst);
    void warmupRoute(const PullTask &task, uint32_t dstNode, route_t *&fwd, route_t *&rev);
    void addFabric(route_t *r, uint32_t srcLeaf, uint32_t dstLeaf);

    Pipe *mkPipe(double us, const std::string &n);
    Queue *mkQueue(uint64_t bw, uint64_t buf, const std::string &n);
    DreQueue *mkDre(uint64_t bw, uint64_t buf, const std::string &n);

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
using namespace gpuwarm;

void
gpu_warmup_testbed(const ArgList &args, Logfile &logfile)
{
    (void)logfile;   // per-queue tracing off: gigabytes of output

    /* ---- Workload ---- */
    double Duration = 0.40, Drain = 0.10;
    double SurgeStart = 0.10, SurgeEnd = 0.25;
    double BaseLoad = 15.0, PeakLoad = 45.0;      // Gbps offered
    uint32_t AvgFlowSize = 100000;
    /*
     * uniform, not enterprise. The CONGA enterprise trace spans four orders
     * of magnitude (p50 2KB, max 27MB); a single 27MB flow pins one GPU for
     * ~54ms, which is 27 control intervals, so the load signal the autoscaler
     * reads becomes noise. It also under-delivers: htsim declares
     * _avgFlowSize=215000 for that CDF but its true mean is ~124KB, so the
     * arrival rate is set ~42% too low (see workloads.cpp:20).
     *
     * Inference responses are streamed token sequences of broadly similar
     * length anyway, so a narrow distribution is the more faithful choice.
     */
    string FlowDist = "uniform", EndHost = "dctcp";

    /* ---- Cluster ---- */
    uint32_t Racks = 8, Cores = 4, Gpus = 8, Clients = 8;
    uint32_t InitNodes = 6, StorageNodes = 4;
    double ComputeRatio = 0.4;
    double StorageReadGbps = 4.0, NvlinkGbps = 100.0;
    double StateSizeMB = 50.0;
    string Placement = "packed", Warmup = "storage";

    /* ---- Control ---- */
    double ControlMs = 2.0;
    string ScalePolicy = "target";
    double ScaleTarget = 0.60, ScaleUp = 0.70, ScaleDown = 0.30;
    uint32_t StepUp = 4, StepDown = 1;
    double CooldownDownMs = 30.0;
    uint32_t CountPending = 1, RngSeed = 1729;

    double DreTdreUs = 16, DreAlpha = 0.1;
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

    parseInt(args, "racks", Racks);
    parseInt(args, "cores", Cores);
    parseInt(args, "gpus", Gpus);
    parseInt(args, "clients", Clients);
    parseInt(args, "init_nodes", InitNodes);
    parseInt(args, "storage_nodes", StorageNodes);
    parseDouble(args, "compute_ratio", ComputeRatio);
    parseDouble(args, "storage_read", StorageReadGbps);
    parseDouble(args, "nvlink", NvlinkGbps);
    parseDouble(args, "statesize", StateSizeMB);
    parseString(args, "placement", Placement);
    parseString(args, "warmup", Warmup);

    parseDouble(args, "control", ControlMs);
    parseString(args, "scaler", ScalePolicy);
    parseDouble(args, "scale_target", ScaleTarget);
    parseDouble(args, "scale_up", ScaleUp);
    parseDouble(args, "scale_down", ScaleDown);
    parseInt(args, "step_up", StepUp);
    parseInt(args, "step_down", StepDown);
    parseDouble(args, "cooldown_down", CooldownDownMs);
    parseInt(args, "count_pending", CountPending);
    parseInt(args, "rngseed", RngSeed);
    parseDouble(args, "dre_tdre", DreTdreUs);
    parseDouble(args, "dre_alpha", DreAlpha);
    parseInt(args, "dre_qbits", DreQbits);

    /* ---- Validate ---- */
    if (Racks < 1 || Cores < 1 || Gpus < 1 || Clients < 1 || StorageNodes < 1) {
        cerr << "--racks/--cores/--gpus/--clients/--storage_nodes must be >= 1" << endl;
        exit(1);
    }
    if (ComputeRatio <= 0) {
        cerr << "--compute_ratio must be > 0" << endl;
        exit(1);
    }
    if (SurgeEnd < SurgeStart || PeakLoad < BaseLoad) {
        cerr << "surge window / load range is inverted" << endl;
        exit(1);
    }

    nRacks = Racks; nCore = Cores; gpusPerRack = Gpus; clientsPerRack = Clients;
    nStorage = StorageNodes;
    maxNodes = nRacks * gpusPerRack;
    nClients = nRacks * clientsPerRack;
    storageLeaf = nRacks;                       // one extra leaf
    uint32_t nLeaves = nRacks + 1;

    if (InitNodes < 1 || InitNodes > maxNodes) {
        cerr << "--init_nodes must be between 1 and " << maxNodes << endl;
        exit(1);
    }

    computeSpeed = (uint64_t)(ComputeRatio * NIC_SPEED);
    storageRead = speedFromGbps(StorageReadGbps);
    nvlinkSpeed = speedFromGbps(NvlinkGbps);
    uint64_t stateBytes = (uint64_t)(StateSizeMB * 1000000.0);

    GpuFleet::Placement place;
    if (!GpuFleet::parsePlacement(Placement, place)) {
        cerr << "Unknown --placement=" << Placement << " (packed or roundrobin)" << endl;
        exit(1);
    }

    WarmupPolicy *policy = makeWarmupPolicy(Warmup);
    if (policy == NULL) {
        cerr << "Unknown --warmup=" << Warmup
             << " (storage, peer or peer-racklocal)" << endl;
        exit(1);
    }

    AutoScaler::Policy scalerPolicy;
    if (!AutoScaler::parsePolicy(ScalePolicy, scalerPolicy)) {
        cerr << "Unknown --scaler=" << ScalePolicy << endl;
        exit(1);
    }

    DataSource::EndHost eh = DataSource::TCP;
    if (EndHost == "dctcp") {
        eh = DataSource::DCTCP;
    } else if (EndHost != "tcp") {
        cerr << "Unsupported --endhost=" << EndHost << endl;
        exit(1);
    }

    Workloads::FlowDist fd = Workloads::UNIFORM;
    if (FlowDist == "pareto")          fd = Workloads::PARETO;
    else if (FlowDist == "enterprise") fd = Workloads::ENTERPRISE;
    else if (FlowDist == "datamining") fd = Workloads::DATAMINING;

    rng.seed(RngSeed);
    DreQueue::configure(timeFromUs(DreTdreUs), DreAlpha, DreQbits);

    /* ================= Topology ================= */

    qCompute.assign(maxNodes, NULL);
    qNicOut.assign(maxNodes, NULL);  pNicOut.assign(maxNodes, NULL);
    qNicIn.assign(maxNodes, NULL);   pNicIn.assign(maxNodes, NULL);
    qNvOut.assign(maxNodes, NULL);   pNvOut.assign(maxNodes, NULL);
    qNvIn.assign(maxNodes, NULL);    pNvIn.assign(maxNodes, NULL);

    for (uint32_t i = 0; i < maxNodes; i++) {
        string id = to_string(i);
        /* Compute: below NIC line rate, so it -- not the wire -- saturates. */
        qCompute[i] = mkDre(computeSpeed, COMPUTE_BUFFER, "q-compute-" + id);
        qNicOut[i] = mkQueue(NIC_SPEED, NIC_BUFFER, "q-nic-out-" + id);
        pNicOut[i] = mkPipe(LINK_DELAY, "p-nic-out-" + id);
        qNicIn[i] = mkQueue(NIC_SPEED, LEAF_BUFFER, "q-nic-in-" + id);
        pNicIn[i] = mkPipe(LINK_DELAY, "p-nic-in-" + id);
        /* NVSwitch-class intra-rack fabric: separate from the NIC. */
        qNvOut[i] = mkQueue(nvlinkSpeed, NVLINK_BUFFER, "q-nv-out-" + id);
        pNvOut[i] = mkPipe(NVLINK_DELAY, "p-nv-out-" + id);
        qNvIn[i] = mkQueue(nvlinkSpeed, NVLINK_BUFFER, "q-nv-in-" + id);
        pNvIn[i] = mkPipe(NVLINK_DELAY, "p-nv-in-" + id);
    }

    qCliOut.assign(nRacks, vector<Queue*>(clientsPerRack, NULL));
    pCliOut.assign(nRacks, vector<Pipe*>(clientsPerRack, NULL));
    qCliIn.assign(nRacks, vector<Queue*>(clientsPerRack, NULL));
    pCliIn.assign(nRacks, vector<Pipe*>(clientsPerRack, NULL));

    for (uint32_t l = 0; l < nRacks; l++) {
        for (uint32_t s = 0; s < clientsPerRack; s++) {
            string id = to_string(l) + "-" + to_string(s);
            qCliOut[l][s] = new FairQueue(NIC_SPEED, NIC_BUFFER, NULL);
            qCliOut[l][s]->setName("q-cli-out-" + id);
            pCliOut[l][s] = mkPipe(LINK_DELAY, "p-cli-out-" + id);
            qCliIn[l][s] = mkQueue(NIC_SPEED, LEAF_BUFFER, "q-cli-in-" + id);
            pCliIn[l][s] = mkPipe(LINK_DELAY, "p-cli-in-" + id);
        }
    }

    qStoRead.assign(nStorage, NULL);
    qStoOut.assign(nStorage, NULL);  pStoOut.assign(nStorage, NULL);
    qStoIn.assign(nStorage, NULL);   pStoIn.assign(nStorage, NULL);

    for (uint32_t s = 0; s < nStorage; s++) {
        string id = to_string(s);
        /* The slow device sits inside the storage node, behind a normal NIC,
         * so the bottleneck is the hardware rather than a hand-tuned link. */
        qStoRead[s] = mkDre(storageRead, NIC_BUFFER, "q-sto-read-" + id);
        qStoOut[s] = mkQueue(NIC_SPEED, NIC_BUFFER, "q-sto-out-" + id);
        pStoOut[s] = mkPipe(LINK_DELAY, "p-sto-out-" + id);
        qStoIn[s] = mkQueue(NIC_SPEED, LEAF_BUFFER, "q-sto-in-" + id);
        pStoIn[s] = mkPipe(LINK_DELAY, "p-sto-in-" + id);
    }

    qLeafCore.assign(nLeaves, vector<DreQueue*>(nCore, NULL));
    pLeafCore.assign(nLeaves, vector<Pipe*>(nCore, NULL));
    qCoreLeaf.assign(nCore, vector<DreQueue*>(nLeaves, NULL));
    pCoreLeaf.assign(nCore, vector<Pipe*>(nLeaves, NULL));

    for (uint32_t l = 0; l < nLeaves; l++) {
        for (uint32_t c = 0; c < nCore; c++) {
            string id = to_string(l) + "-" + to_string(c);
            qLeafCore[l][c] = mkDre(FABRIC_SPEED, FABRIC_BUFFER, "q-leaf-core-" + id);
            pLeafCore[l][c] = mkPipe(LINK_DELAY, "p-leaf-core-" + id);
            qCoreLeaf[c][l] = mkDre(FABRIC_SPEED, FABRIC_BUFFER, "q-core-leaf-" + id);
            pCoreLeaf[c][l] = mkPipe(LINK_DELAY, "p-core-leaf-" + id);
        }
    }

    /* ================= Fleet, warm-up, control ================= */

    fleet = new GpuFleet(nRacks, gpusPerRack, InitNodes, stateBytes, nStorage, place);
    for (uint32_t i = 0; i < maxNodes; i++) {
        fleet->setComputeQueue(i, qCompute[i]);
    }

    warmup = new WarmupManager(fleet, policy, warmupRoute, eh);
    fleet->setWarmupStarter(warmup);

    simtime_picosec genEnd = timeFromSec(Duration);
    simtime_picosec endTime = timeFromSec(Duration + Drain);

    scaler = new AutoScaler(fleet, scalerPolicy, timeFromMs(ControlMs));
    scaler->setThresholds(ScaleUp, ScaleDown);
    scaler->setTarget(ScaleTarget);
    scaler->setStep(StepUp, StepDown);
    scaler->setCooldown(0, timeFromMs(CooldownDownMs));
    scaler->setCountBooting(CountPending != 0);
    /* Hold the controller off for a few intervals so it does not act on a
     * DRE that has not yet filled. */
    scaler->start(timeFromMs(ControlMs) * 5, genEnd);

    /* ================= User traffic ================= */

    FlowGenerator *baseGen =
        new FlowGenerator(eh, servingRoute, speedFromGbps(BaseLoad), AvgFlowSize, fd);
    baseGen->setPrefix("base-");
    baseGen->setTimeLimits(timeFromUs(1), genEnd);

    double delta = PeakLoad - BaseLoad;
    if (delta > 0 && SurgeEnd > SurgeStart) {
        FlowGenerator *surgeGen =
            new FlowGenerator(eh, servingRoute, speedFromGbps(delta), AvgFlowSize, fd);
        surgeGen->setPrefix("surge-");
        surgeGen->setTimeLimits(timeFromSec(SurgeStart), timeFromSec(SurgeEnd));
    }

    EventList::Get().setEndtime(endTime);

    RunStats *stats = new RunStats(string(policy->name()) + "/" + Placement);
    EventList::Get().sourceIsPending(*stats, endTime - 1);

    /* ================= Banner ================= */

    double nodeGbps = speedAsGbps(computeSpeed);
    double needPeak = PeakLoad / nodeGbps;
    double pullMs = timeAsMs(timeFromSec((stateBytes * 8.0) / (double)storageRead));

    cerr << "gpu_warmup_testbed:"
         << " warmup=" << policy->name() << " placement=" << Placement
         << " racks=" << nRacks << " gpus/rack=" << gpusPerRack
         << " max_nodes=" << maxNodes << " init=" << InitNodes
         << " clients=" << nClients << " cores=" << nCore
         << " storage=" << nStorage << "x" << StorageReadGbps << "Gbps"
         << " state=" << StateSizeMB << "MB"
         << " compute=" << nodeGbps << "Gbps (ratio " << ComputeRatio << ")"
         << " nvlink=" << NvlinkGbps << "Gbps"
         << " load=" << BaseLoad << "->" << PeakLoad << "Gbps"
         << " surge=[" << SurgeStart << "," << SurgeEnd << "]s"
         << " scaler=" << ScalePolicy << " step_up=" << StepUp
         << " seed=" << RngSeed << endl;

    cerr << "  capacity: " << InitNodes * nodeGbps << "Gbps active, "
         << maxNodes * nodeGbps << "Gbps max; peak needs >= "
         << (uint32_t)ceil(needPeak) << " nodes" << endl;
    cerr << "  one storage pull = " << pullMs << "ms; "
         << nStorage << " can run at full rate concurrently" << endl;

    if (PeakLoad > maxNodes * nodeGbps) {
        cerr << "  WARNING: peak load exceeds the whole fleet's compute." << endl;
    }
    if (nClients * speedAsGbps(NIC_SPEED) < PeakLoad) {
        cerr << "  WARNING: clients are the bottleneck; raise --clients." << endl;
    }
}

/* ---------------- Serving: GPU -> client ---------------- */

void
gpuwarm::servingRoute(route_t *&fwd, route_t *&rev, uint32_t &src, uint32_t &dst)
{
    int n = fleet->select();
    if (n < 0) {
        n = 0;
    }
    uint32_t node = (uint32_t)n;
    uint32_t client = rng() % nClients;

    src = node;
    dst = maxNodes + client;

    uint32_t nLeaf = fleet->rackOf(node);
    uint32_t cLeaf = client % nRacks, cSlot = client / nRacks;

    fwd = new route_t();
    rev = new route_t();

    /* Response path: compute first (the bottleneck), then the wire. */
    fwd->push_back(qCompute[node]);
    fwd->push_back(qNicOut[node]);
    fwd->push_back(pNicOut[node]);
    addFabric(fwd, nLeaf, cLeaf);
    fwd->push_back(qCliIn[cLeaf][cSlot]);
    fwd->push_back(pCliIn[cLeaf][cSlot]);

    /* ACKs back to the node: its ingress link, no compute. */
    rev->push_back(qCliOut[cLeaf][cSlot]);
    rev->push_back(pCliOut[cLeaf][cSlot]);
    addFabric(rev, cLeaf, nLeaf);
    rev->push_back(qNicIn[node]);
    rev->push_back(pNicIn[node]);
}

/* ---------------- Warm-up: storage or peer -> GPU ---------------- */

void
gpuwarm::warmupRoute(const PullTask &task,
                     uint32_t dstNode,
                     route_t *&fwd,
                     route_t *&rev)
{
    fwd = new route_t();
    rev = new route_t();

    uint32_t dLeaf = fleet->rackOf(dstNode);

    if (task.fromStorage()) {
        /* Round-robin across storage nodes; the slow read queue comes first. */
        uint32_t s = storageRR++ % nStorage;

        fwd->push_back(qStoRead[s]);
        fwd->push_back(qStoOut[s]);
        fwd->push_back(pStoOut[s]);
        addFabric(fwd, storageLeaf, dLeaf);
        fwd->push_back(qNicIn[dstNode]);
        fwd->push_back(pNicIn[dstNode]);

        rev->push_back(qNicOut[dstNode]);
        rev->push_back(pNicOut[dstNode]);
        addFabric(rev, dLeaf, storageLeaf);
        rev->push_back(qStoIn[s]);
        rev->push_back(pStoIn[s]);
        return;
    }

    uint32_t donor = (uint32_t)task.source;
    uint32_t srcLeaf = fleet->rackOf(donor);

    if (srcLeaf == dLeaf) {
        /* Same rack: NVSwitch. Never touches the leaf, the fabric, or the
         * donor's NIC -- so donating costs the donor almost nothing. */
        fwd->push_back(qNvOut[donor]);
        fwd->push_back(pNvOut[donor]);
        fwd->push_back(qNvIn[dstNode]);
        fwd->push_back(pNvIn[dstNode]);

        rev->push_back(qNvOut[dstNode]);
        rev->push_back(pNvOut[dstNode]);
        rev->push_back(qNvIn[donor]);
        rev->push_back(pNvIn[donor]);
        return;
    }

    /* Cross-rack: the donor's NIC, shared with its own serving traffic.
     * Note the absence of qCompute -- weight reads are modelled as DMA that
     * bypasses the GPU's serving pipeline. */
    fwd->push_back(qNicOut[donor]);
    fwd->push_back(pNicOut[donor]);
    addFabric(fwd, srcLeaf, dLeaf);
    fwd->push_back(qNicIn[dstNode]);
    fwd->push_back(pNicIn[dstNode]);

    rev->push_back(qNicOut[dstNode]);
    rev->push_back(pNicOut[dstNode]);
    addFabric(rev, dLeaf, srcLeaf);
    rev->push_back(qNicIn[donor]);
    rev->push_back(pNicIn[donor]);
}

void
gpuwarm::addFabric(route_t *r, uint32_t srcLeaf, uint32_t dstLeaf)
{
    if (srcLeaf == dstLeaf) {
        return;   // same leaf switches locally
    }
    uint32_t c = rng() % nCore;   // ECMP
    r->push_back(qLeafCore[srcLeaf][c]);
    r->push_back(pLeafCore[srcLeaf][c]);
    r->push_back(qCoreLeaf[c][dstLeaf]);
    r->push_back(pCoreLeaf[c][dstLeaf]);
}

/* ---------------- End-of-run summary ---------------- */

void
gpuwarm::RunStats::doNextEvent()
{
    vector<simtime_picosec> d = warmup->durations();
    double p50 = d.empty() ? 0 : timeAsMs(d[d.size() / 2]);
    double p99 = d.empty() ? 0 : timeAsMs(d[(size_t)(d.size() * 0.99) < d.size()
                                            ? (size_t)(d.size() * 0.99) : d.size() - 1]);
    double mx = d.empty() ? 0 : timeAsMs(d.back());
    double mn = d.empty() ? 0 : timeAsMs(d.front());

    /* Time from the first scale-up decision to the first node serving, and
     * to the last one: these diverge under batching. */
    const vector<WarmupManager::Record> &recs = warmup->records();
    double firstReadyMs = 0, lastReadyMs = 0, firstStartMs = 0;
    bool any = false;
    for (size_t i = 0; i < recs.size(); i++) {
        double st = timeAsMs(recs[i].started);
        if (!any || st < firstStartMs) {
            firstStartMs = st;
        }
        if (recs[i].finished > 0) {
            double f = timeAsMs(recs[i].finished);
            if (!any || f < firstReadyMs) firstReadyMs = f;
            if (f > lastReadyMs) lastReadyMs = f;
            any = true;
        }
    }

    uint32_t racksSeeded = 0;
    for (size_t i = 0; i < recs.size(); i++) {
        if (recs[i].bytesFromStorage > 0) {
            racksSeeded++;
        }
    }

    cout << fixed << setprecision(3)
         << "WarmStats tag " << _tag
         << " warm_started " << recs.size()
         << " warm_done " << warmup->completed()
         << " warm_stuck " << fleet->nStuck()
         << " warm_p50_ms " << p50
         << " warm_p99_ms " << p99
         << " warm_min_ms " << mn
         << " warm_max_ms " << mx
         << " first_decision_ms " << firstStartMs
         << " first_ready_ms " << (any ? firstReadyMs : 0)
         << " last_ready_ms " << lastReadyMs
         << " storage_pulls " << racksSeeded
         << " storage_mb " << warmup->storageBytes() / 1e6
         << " local_peer_mb " << warmup->localPeerBytes() / 1e6
         << " remote_peer_mb " << warmup->remotePeerBytes() / 1e6
         << " final_active " << fleet->nActive()
         << " node_seconds " << fleet->nodeSeconds()
         << " dispatches " << fleet->_dispatches
         << endl;
}

Pipe *
gpuwarm::mkPipe(double us, const string &n)
{
    Pipe *p = new Pipe(timeFromUs(us));
    p->setName(n);
    return p;
}

Queue *
gpuwarm::mkQueue(uint64_t bw, uint64_t buf, const string &n)
{
    Queue *q = new Queue(bw, buf, NULL);
    q->setName(n);
    return q;
}

DreQueue *
gpuwarm::mkDre(uint64_t bw, uint64_t buf, const string &n)
{
    DreQueue *q = new DreQueue(bw, buf, NULL);
    q->setName(n);
    return q;
}
