
#include <wrench-dev.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <fstream>
#include <iomanip>

#include <services/compute/batch/BatchComputeServiceProperty.h>
#include <LevelByLevelAlgorithm/LevelByLevelWMS.h>
#include "Simulator.h"
#include "Util/WorkflowUtil.h"
#include "StaticClusteringAlgorithms/StaticClusteringWMS.h"
#include "ZhangClusteringAlgorithms/ZhangWMS.h"
#include "GlumeAlgorithm/GlumeWMS.h"
#include "Globals.h"

#include <sys/types.h>

XBT_LOG_NEW_DEFAULT_CATEGORY(task_clustering_simulator, "Log category for Task Clustering Simulator");

using namespace wrench;

unsigned long Simulator::sequence_number = 0;

nlohmann::json Globals::sim_json;

int Simulator::main(int argc, char **argv) {

    // Create and initialize a simulation
    auto simulation = new wrench::Simulation();
    simulation->init(&argc, argv);

    // Parse command-line arguments
    if ((argc != 8) and (argc != 9)) {
        std::cerr << "\e[1;31mUsage: " << argv[0]
                  << " <num_compute_nodes> <job trace file> <max jobs in system> <workflow specification> <workflow start time> <algorithm> <batch algorithm> [DISABLED: csv batch log file] [OPTIONAL: json result file]\e[0m"
                  << "\n";
        std::cerr << "  \e[1;32m### workflow specification options ###\e[0m" << "\n";
        std::cerr << "    *  \e[1mindep:s:n:t1:t2\e[0m " << "\n";
        std::cerr << "      - Just a set of independent tasks" << "\n";
        std::cerr << "      - n: number of tasks" << "\n";
        std::cerr << "      - t1/t2: min/max task durations in integral seconds (actual times uniformly sampled)"
                  << "\n";
        std::cerr << "      - s: rng seed" << "\n";
        std::cerr << "    * \e[1mlevels:s:l0:t0:T0:l1:t1:T1:....:ln:tn:Tn\e[0m" << "\n";
        std::cerr << "      - A strictly levelled workflow" << "\n";
        std::cerr << "      - lx: num tasks in level x" << "\n";
        std::cerr << "      - each task in level x depends on ALL tasks in level x-1" << "\n";
        std::cerr << "      - tx/Tx: min/max task durations in level x, in integral second (times uniformly sampled)"
                  << "\n";
        std::cerr << "      - s: rng seed" << "\n";
        std::cerr << "    * \e[1mdax:filename\e[0m" << "\n";
        std::cerr << "      - A workflow imported from a DAX file" << "\n";
        std::cerr << "      - Files and Data dependencies are ignored. Only control dependencies are preserved" << "\n";
        std::cerr << "    * \e[1mjson:filename\e[0m" << "\n";
        std::cerr << "      - A workflow imported from a JSON file" << "\n";
        std::cerr << "      - Files and Data dependencies are ignored. Only control dependencies are preserved" << "\n";
        std::cerr << "\n";
        std::cerr << "  \e[1;32m### algorithm options ###\e[0m" << "\n";
        std::cerr << "    * \e[1mstatic:levelbylevel-m\e[0m" << "\n";
        std::cerr << "      - run each workflow level as a job" << "\n";
        std::cerr << "      - m: number of hosts used to execute each job" << "\n";
        std::cerr << "              - if m = 0, then pick best number nodes based on queue wait time prediction"
                  << "\n";
        std::cerr << "    * \e[1mstatic:one_job-m-waste_bound\e[0m" << "\n";
        std::cerr << "      - run the workflow as a single job" << "\n";
        std::cerr << "      - m: number of hosts used to execute the job" << "\n";
        std::cerr << "              - if m = 0, then pick best number nodes based on queue wait time prediction"
                  << "\n";
        std::cerr << "      - waste_bound: maximum percentage of wasted node time e.g. 0.2" << "\n";
        std::cerr << "    * \e[1mstatic:one_job_per_task\e[0m" << "\n";
        std::cerr << "      - run each task as a single one-host job" << "\n";
        std::cerr << "      - Submit a job only once a task is ready, no queue wait time estimation" << "\n";
        std::cerr << "    * \e[1mstatic:hc-[vprior|vposterior|vnone]-n-m\e[0m" << "\n";
        std::cerr
                << "      - Horizontal Clustering algorithm (ref [12] in \"Using Imbalance Metrics to Optimize Task Clustering in Scientific Workflow Executions\" by Chen at al.)"
                << "\n";
        std::cerr << "      - Simply clusters tasks in each level in whatever order and execute " << "\n";
        std::cerr << "        each cluster on the same number of hosts " << "\n";
        std::cerr << "      - [vprior|vposterior|vnone]: application of vertical clustering" << "\n";
        std::cerr << "      - n: number of ready tasks in each cluster" << "\n";
        std::cerr << "      - m: number of hosts used to execute each cluster" << "\n";
        std::cerr << "              - if m = 0, then pick best number nodes based on queue wait time prediction"
                  << "\n";
        std::cerr << "    * \e[1mstatic:dfjs-[vprior|vposterior|vnone]-t-m\e[0m" << "\n";
        std::cerr
                << "      - DFJS algorithm (ref [5] in \"Using Imbalance Metrics to Optimize Task Clustering in Scientific Workflow Executions\" by Chen at al.)"
                << "\n";
        std::cerr << "      - Simply greedily clusters tasks in each level in whatever order so that " << "\n";
        std::cerr << "        each cluster has a runtime that's bounded. (extended to deal with numbers of hosts)"
                  << "\n";
        std::cerr << "      - [vprior|vposterior|vnone]: application of vertical clustering" << "\n";
        std::cerr << "      - t: bound on runtime (in seconds)" << "\n";
        std::cerr << "      - m: number of hosts used to execute each cluster" << "\n";
        std::cerr << "              - if m = 0, then pick best number nodes based on queue wait time prediction"
                  << "\n";
        std::cerr << "    * \e[1mstatic:hrb-[vprior|vposterior|vnone]-n-m\e[0m" << "\n";
        std::cerr
                << "      - The HRB algorithm in \"Using Imbalance Metrics to Optimize Task Clustering in Scientific Workflow Executions\" by Chen at al."
                << "\n";
        std::cerr
                << "      - Modified to specify 'number of tasks per cluster' rather than 'number of clusters per level'"
                << "\n";
        std::cerr << "      - Cluster tasks in each level so that clusters are load-balanced" << "\n";
        std::cerr << "        (extended to deal with numbers of hosts)" << "\n";
        std::cerr << "      - [vprior|vposterior|vnone]: application of vertical clustering" << "\n";
        std::cerr << "      - n: number of ready tasks in each cluster" << "\n";
        std::cerr << "      - m: number of hosts used to execute each cluster" << "\n";
        std::cerr << "              - if m = 0, then pick best number nodes based on queue wait time prediction"
                  << "\n";
        std::cerr << "    * \e[1mstatic:hifb-[vprior|vposterior|vnone-]n-m\e[0m" << "\n";
        std::cerr
                << "      - The HIFB algorithm in \"Using Imbalance Metrics to Optimize Task Clustering in Scientific Workflow Executions\" by Chen at al."
                << "\n";
        std::cerr
                << "      - Modified to specify 'number of tasks per cluster' rather than 'number of clusters per level'"
                << "\n";
        std::cerr << "      - Cluster tasks in each level so that clusters have similar impact factors" << "\n";
        std::cerr << "        (extended to deal with numbers of hosts)" << "\n";
        std::cerr << "      - [vprior|vposterior|vnone]: application of vertical clustering" << "\n";
        std::cerr << "      - n: number of ready tasks in each cluster" << "\n";
        std::cerr << "      - m: number of hosts used to execute each cluster" << "\n";
        std::cerr << "              - if m = 0, then pick best number nodes based on queue wait time prediction"
                  << "\n";
        std::cerr << "    * \e[1mstatic:hdb-[vprior|vposterior|vnone]-n-m\e[0m" << "\n";
        std::cerr
                << "      - The HDB algorithm in \"Using Imbalance Metrics to Optimize Task Clustering in Scientific Workflow Executions\" by Chen at al."
                << "\n";
        std::cerr
                << "      - Modified to specify 'number of tasks per cluster' rather than 'number of clusters per level'"
                << "\n";
        std::cerr << "      - Cluster tasks in each level so that clusters have similar distances" << "\n";
        std::cerr << "        (extended to deal with numbers of hosts)" << "\n";
        std::cerr << "      - [vprior|vposterior|vnone]: application of vertical clustering" << "\n";
        std::cerr << "      - n: number of ready tasks in each cluster" << "\n";
        std::cerr << "      - m: number of hosts used to execute each cluster" << "\n";
        std::cerr << "              - if m = 0, then pick best number nodes based on queue wait time prediction"
                  << "\n";
        std::cerr << "    * \e[1mstatic:vc\e[0m" << "\n";
        std::cerr
                << "      - The VC algorithm in \"Using Imbalance Metrics to Optimize Task Clustering in Scientific Workflow Executions\" by Chen at al."
                << "\n";
        std::cerr << "      - Cluster tasks with single-parent-single-child depepdencies" << "\n";
        std::cerr << "    * \e[1mzhang:[global|noglobal]:[bsearch|nobsearch]:[prediction|noprediction]\e[0m" << "\n";
        std::cerr << "      - The algorithm by Zhang, Koelbel, and Cooper + our improvements" << "\n";
        std::cerr << "      - [global|noglobal]: pick the globally best ratio; otherwise, greedily pick" << "\n";
        std::cerr
                << "      - [bsearch|nobsearch]: find leeway with a binary search; otherwise, use zhangs one-way search"
                << "\n";
        std::cerr << "      - [prediction|noprediction]: pick parallelism based on makespan+wait predictions"
                  << "\n";
        std::cerr << "    * \e[1mglume:waste_bound:beat_bound\e[0m" << "\n";
        std::cerr << "      - GLUME: Group Levels Using Makespan Estimates" << "\n";
        std::cerr << "      - waste_bound: maximum percentage of wasted node time e.g. 0.2" << "\n";
        std::cerr
                << "      - beat_bound: percentage splitting time must beat non-splitting time by to be viable e.g. 0.1"
                << "\n";
        std::cerr << "    * \e[1mlevelbylevel:[overlap|nooverlap]:levelclustering\e[0m" << "\n";
        std::cerr << "        - A level-by-level-with overlap algorithm that clusters tasks in each level." << "\n";
        std::cerr << "          Tasks in level n+1 are submitted to the batch queue as soon as all tasks in level n"
                  << "\n";
        std::cerr << "          have started. Timout behavior similar as that in the algorithm by Zhang et al." << "\n";
        std::cerr << "        - levelclustering: the algorithm uses to cluster tasks in each level. Options are: "
                  << "\n";
        std::cerr << "          - one_job-m: the level is submitted as a single job" << "\n";
        std::cerr << "            - m: number of hosts used to execute each cluster" << "\n";
        std::cerr << "              - if m = 0, then pick best number nodes based on queue wait time prediction"
                  << "\n";
        std::cerr << "          - one_job_per_task: one job per task" << "\n";
        std::cerr << "          - hc-n-m:  HC static algorithm" << "\n";
        std::cerr << "            - n: number of ready tasks in each cluster" << "\n";
        std::cerr << "            - m: number of hosts used to execute each cluster" << "\n";
        std::cerr << "              - if m = 0, then pick best number nodes based on queue wait time prediction"
                  << "\n";
        std::cerr << "          - djfs-t-m1-m2: DJFS static algorithm" << "\n";
        std::cerr << "            - t: bound on runtime (in seconds)" << "\n";
        std::cerr << "            - m1: number of hosts used to compute the cluster" << "\n";
        std::cerr << "            - m2: number of hosts used to execute each cluster" << "\n";
        std::cerr << "              - if m2 = 0, then pick best number nodes based on queue wait time prediction"
                  << "\n";
        std::cerr << "          - hrb-n-m: HRB algorithm" << "\n";
        std::cerr << "            - n: number of ready tasks in each cluster" << "\n";
        std::cerr << "            - m: number of hosts used to execute each cluster" << "\n";
        std::cerr << "              - if m = 0, then pick best number nodes based on queue wait time prediction"
                  << "\n";
        std::cerr << "          - hifb-n-m: HIFB algorithm" << "\n";
        std::cerr << "            - n: number of ready tasks in each cluster" << "\n";
        std::cerr << "            - m: number of hosts used to execute each cluster" << "\n";
        std::cerr << "              - if m = 0, then pick best number nodes based on queue wait time prediction"
                  << "\n";
        std::cerr << "          - hdb-n-m: HDC algorithm" << "\n";
        std::cerr << "            - n: number of ready tasks in each cluster" << "\n";
        std::cerr << "            - m: number of hosts used to execute each cluster" << "\n";
        std::cerr << "              - if m = 0, then pick best number nodes based on queue wait time prediction"
                  << "\n";
        std::cerr << "          - clever: A clever heuristic" << "\n";
        std::cerr << "            - A heuristic that cleverly splits a level into jobs based on task executiong times "
                  << "\n";
        std::cerr << "              and queue wait times" << "\n";
        std::cerr << "\n";
        std::cerr << "  \e[1;32m### batch algorithm options ###\e[0m" << "\n";
        std::cerr << "    * \e[1mconservative_bf\e[0m" << "\n";
        std::cerr << "      - classical conservative backfilling" << "\n";
        std::cerr << "    * \e[1mfast_conservative_bf\e[0m" << "\n";
        std::cerr << "      - faster backfilling algorithm, still conservative but not exactly canon" << "\n";
        std::cerr << "    * \e[1mfcfs_fast\e[0m" << "\n";
        std::cerr << "      - first come, first serve" << "\n";
        std::cerr << "\n";
        exit(1);
    }
    unsigned long num_compute_nodes;
    if ((sscanf(argv[1], "%lu", &num_compute_nodes) != 1) or (num_compute_nodes < 1)) {
        std::cerr << "Invalid number of compute nodes\n";
    }

    unsigned long max_num_jobs;
    if ((sscanf(argv[3], "%lu", &max_num_jobs) != 1) or (max_num_jobs < 1)) {
        std::cerr << "Invalid maximum number of jobs\n";
    }


    double workflow_start_time;
    if ((sscanf(argv[5], "%lf", &workflow_start_time) != 1) or (workflow_start_time < 0)) {
        std::cerr << "Invalid workflow start time\n";
    }

    std::string scheduler_spec = std::string(argv[6]);

    // Setup the simulation platform
    setupSimulationPlatform(simulation, num_compute_nodes);

    // Create a BatchComputeService
    std::vector<std::string> compute_nodes;
    for (unsigned int i = 0; i < num_compute_nodes; i++) {
        compute_nodes.push_back(std::string("ComputeNode_") + std::to_string(i));
    }
    std::shared_ptr<wrench::BatchComputeService> batch_service = nullptr;
    std::string login_hostname = "Login";

    std::string csv_batch_log = "/tmp/batch_log.csv";
    // disable custom batch_log file for now
//    if (argc == 9) {
//        csv_batch_log = std::string(argv[8]);
//    }


    wrench::BatchComputeService *tmp_batch_service = nullptr;
    try {
        tmp_batch_service = new BatchComputeService(login_hostname, compute_nodes, "",
                                                    {{BatchComputeServiceProperty::OUTPUT_CSV_JOB_LOG,                                             csv_batch_log},
                                                     {BatchComputeServiceProperty::BATCH_SCHEDULING_ALGORITHM,                                     std::string(
                                                             argv[7])},
                                                     {BatchComputeServiceProperty::TASK_SELECTION_ALGORITHM,                                       "maximum_flops"},
                                                     {BatchComputeServiceProperty::SIMULATED_WORKLOAD_TRACE_FILE,                                  std::string(
                                                             argv[2])},
                                                     {BatchComputeServiceProperty::SIMULATE_COMPUTATION_AS_SLEEP,                                  "true"},
                                                     {BatchComputeServiceProperty::BATSCHED_CONTIGUOUS_ALLOCATION,                                 "true"},
                                                     {BatchComputeServiceProperty::BATSCHED_LOGGING_MUTED,                                         "true"},
                                                     {BatchComputeServiceProperty::IGNORE_INVALID_JOBS_IN_WORKLOAD_TRACE_FILE,                      "true"},
                                                     {BatchComputeServiceProperty::USE_REAL_RUNTIMES_AS_REQUESTED_RUNTIMES_IN_WORKLOAD_TRACE_FILE, "true"},
                                                     {BatchComputeServiceProperty::SUBMIT_TIME_OF_FIRST_JOB_IN_WORKLOAD_TRACE_FILE, "0"},
                                                     {BatchComputeServiceProperty::TASK_STARTUP_OVERHEAD, "0"}

                                                    },
                                                    {
                                                            {BatchComputeServiceMessagePayload::SUBMIT_PILOT_JOB_ANSWER_MESSAGE_PAYLOAD, 0.0},
                                                            {BatchComputeServiceMessagePayload::SUBMIT_PILOT_JOB_REQUEST_MESSAGE_PAYLOAD, 0.0},
                                                    });
    } catch (std::invalid_argument &e) {

        WRENCH_INFO("Cannot instantiate batch service: %s", e.what());
        WRENCH_INFO("Trying the non-BATSCHED version with FCFS...");
        try {
            tmp_batch_service = new BatchComputeService(login_hostname, compute_nodes, "",
                                                        {{BatchComputeServiceProperty::BATCH_SCHEDULING_ALGORITHM,    "FCFS"},
                                                         {BatchComputeServiceProperty::SIMULATED_WORKLOAD_TRACE_FILE, std::string(
                                                                 argv[2])}
                                                        }, {});
        } catch (std::invalid_argument &e) {
            std::cerr << "Giving up as I cannot instantiate the Batch Service: " << e.what() << "\n";
            exit(1);
        }
    }

    batch_service = simulation->add(tmp_batch_service);

    // Create the WMS
    WMS *wms = nullptr;
    try {
        wms = createWMS("Login", batch_service, max_num_jobs, scheduler_spec);
    } catch (std::invalid_argument &e) {
        std::cerr << "Cannot instantiate WMS: " << e.what() << "\n";
        exit(1);
    }

    try {
        simulation->add(wms);
    } catch (std::invalid_argument &e) {
        std::cerr << "Cannot add WMS to simulation: " << e.what() << "\n";
        exit(1);
    }


    // Create the Workflow
    Workflow *workflow = nullptr;
    try {
        workflow = createWorkflow(argv[4]);
    } catch (std::invalid_argument &e) {
        std::cerr << "Cannot create workflow: " << e.what() << "\n";
        exit(1);
    }
    wms->addWorkflow(workflow, workflow_start_time);

    // Launch the simulation
    try { WRENCH_INFO("Launching simulation!");
        simulation->launch();
    } catch (std::runtime_error &e) {
        std::cerr << "Simulation failed: " << e.what() << "\n";
        exit(1);
    }WRENCH_INFO("Simulation done!");

    WorkflowUtil::printRAM();

    std::cout << "MAKESPAN=" << (workflow->getCompletionDate() - workflow_start_time) << "\n";
    std::cout << "NUM PILOT JOB EXPIRATIONS=" << this->num_pilot_job_expirations_with_remaining_tasks_to_do << "\n";
    std::cout << "TOTAL QUEUE WAIT SECONDS=" << this->total_queue_wait_time << "\n";
    std::cout << "USED NODE SECONDS=" << this->used_node_seconds << "\n";
    std::cout << "WASTED NODE SECONDS=" << this->wasted_node_seconds << "\n";
    std::cout << "CSV LOG FILE=" << csv_batch_log << "\n";

    if (argc == 9) {
        std::string json_file_name = std::string(argv[8]);

        // std::cout << json_file_name << std::endl;

        Globals::sim_json["num_compute_nodes"] = argv[1];
        Globals::sim_json["job_trace_file"] = argv[2];
        Globals::sim_json["max_sys_jobs"] = argv[3];
        Globals::sim_json["workflow_specification"] = argv[4];
        Globals::sim_json["start_time"] = argv[5];
        Globals::sim_json["algorithm"] = argv[6];
        Globals::sim_json["batch_algorithm"] = argv[7];

        Globals::sim_json["makespan"] = workflow->getCompletionDate() - workflow_start_time;
        Globals::sim_json["num_p_job_exp"] = this->num_pilot_job_expirations_with_remaining_tasks_to_do;
        Globals::sim_json["total_queue_wait"] = this->total_queue_wait_time;
        Globals::sim_json["used_node_sec"] = this->used_node_seconds;
        Globals::sim_json["wasted_node_seconds"] = this->wasted_node_seconds;

        // TODO - how to handle runtime errors

        std::ofstream out_json(json_file_name);
        out_json << std::setw(4) << Globals::sim_json << std::endl;
    }

    return 0;
}

void Simulator::setupSimulationPlatform(Simulation *simulation, unsigned long num_compute_nodes) {

    // Create a the platform file
    std::string xml = "<?xml version='1.0'?>\n"
                      "<!DOCTYPE platform SYSTEM \"http://simgrid.gforge.inria.fr/simgrid/simgrid.dtd\">\n"
                      "<platform version=\"4.1\">\n"
                      "   <zone id=\"AS0\" routing=\"Full\">\n"
                      "     <cluster id=\"cluster\" prefix=\"ComputeNode_\" suffix=\"\" radical=\"0-";
    xml += std::to_string(num_compute_nodes - 1) +
           "\" speed=\"1f\" bw=\"125GBps\" lat=\"0us\" router_id=\"router\"/>\n";
    xml += "      <zone id=\"AS1\" routing=\"Full\">\n";
    xml += "          <host id=\"Login\" speed=\"1f\"/>\n";
    xml += "          <link id=\"fastlink\" bandwidth=\"10000000GBps\" latency=\"0ms\"/>\n";
    xml += "          <route src=\"Login\" dst=\"Login\"> <link_ctn id=\"fastlink\"/> </route>\n";
    xml += "      </zone>\n";
    xml += "      <link id=\"link\" bandwidth=\"10000000GBps\" latency=\"0ms\"/>\n";
    xml += "      <zoneRoute src=\"cluster\" dst=\"AS1\" gw_src=\"router\" gw_dst=\"Login\">\n";
    xml += "        <link_ctn id=\"link\"/>\n";
    xml += "       </zoneRoute>\n";
    xml += "   </zone>\n";
    xml += "</platform>\n";

    std::string file = ("/tmp/platform_" + std::to_string(getpid()) + ".xml");
    int file_length = file.length();
    char file_name[file_length + 1];
    strcpy(file_name, file.c_str());
    file_name[file_length] = '\0';
    FILE *platform_file = fopen(file_name, "w");
    fprintf(platform_file, "%s", xml.c_str());
    fclose(platform_file);

    try {
        simulation->instantiatePlatform(file_name);
    } catch (std::invalid_argument &e) {  // Unfortunately S4U doesn't throw for this...
        throw std::runtime_error("Invalid generated XML platform file");
    }
}

Workflow *Simulator::createWorkflow(std::string workflow_spec) {

    std::istringstream ss(workflow_spec);
    std::string token;
    std::vector<std::string> tokens;

    while (std::getline(ss, token, ':')) {
        tokens.push_back(token);
    }

    if (tokens[0] == "indep") {
        if (tokens.size() != 5) {
            throw std::invalid_argument("createWorkflow(): Invalid workflow specification " + workflow_spec);
        }
        try {
            return createIndepWorkflow(tokens);
        } catch (std::invalid_argument &e) {
            throw;
        }

    } else if (tokens[0] == "levels") {
        if ((tokens.size() == 2) or (tokens.size() - 2) % 3) {
            throw std::invalid_argument("createWorkflow(): Invalid workflow specification " + workflow_spec);
        }
        try {
            return createLevelsWorkflow(tokens);
        } catch (std::invalid_argument &e) {
            throw;
        }

    } else if (tokens[0] == "dax") {
        if (tokens.size() != 2) {
            throw std::invalid_argument("createWorkflow(): Invalid workflow specification " + workflow_spec);
        }
        try {
            return createWorkflowFromFile("dax",tokens);
        } catch (std::invalid_argument &e) {
            throw;
        }
    } else if (tokens[0] == "json") {
        if (tokens.size() != 2) {
            throw std::invalid_argument("createWorkflow(): Invalid workflow specification " + workflow_spec);
        }
        try {
            return createWorkflowFromFile("json",tokens);
        } catch (std::invalid_argument &e) {
            throw;
        }

    } else {
        throw std::invalid_argument("createWorkflow(): Unknown workflow type " + tokens[0]);
    }
}

Workflow *Simulator::createIndepWorkflow(std::vector<std::string> spec_tokens) {
    unsigned int seed;
    if (sscanf(spec_tokens[1].c_str(), "%u", &seed) != 1) {
        throw std::invalid_argument("createIndepWorkflow(): invalid RNG ssed in workflow specification");
    }
    std::default_random_engine rng(seed);

    unsigned long num_tasks;
    unsigned long min_time;
    unsigned long max_time;

    if ((sscanf(spec_tokens[2].c_str(), "%lu", &num_tasks) != 1) or (num_tasks < 1)) {
        throw std::invalid_argument("createIndepWorkflow(): invalid number of tasks in workflow specification");
    }
    if ((sscanf(spec_tokens[3].c_str(), "%lu", &min_time) != 1) or (min_time < 0.0)) {
        throw std::invalid_argument("createIndepWorkflow(): invalid min task exec time in workflow specification");
    }
    if ((sscanf(spec_tokens[4].c_str(), "%lu", &max_time) != 1) or (max_time < 0.0) or (max_time < min_time)) {
        throw std::invalid_argument("createIndepWorkflow(): invalid max task exec time in workflow specification");
    }

    auto workflow = new Workflow();

    static std::uniform_int_distribution<unsigned long> m_udist(min_time, max_time);
    for (unsigned long i = 0; i < num_tasks; i++) {
        unsigned long flops = m_udist(rng);
        auto t = workflow->addTask("Task_" + std::to_string(i), (double) flops, 1, 1, 1.0);
        WRENCH_INFO("AAAAA   %s %lf", t->getID().c_str(), t->getFlops() );
    }

    return workflow;

}


Workflow *Simulator::createLevelsWorkflow(std::vector<std::string> spec_tokens) {

    unsigned int seed;
    if (sscanf(spec_tokens[1].c_str(), "%u", &seed) != 1) {
        throw std::invalid_argument("createLevelsWorkflow(): invalid RNG ssed in workflow specification");
    }
    std::default_random_engine rng(seed);

    unsigned long num_levels = (spec_tokens.size() - 1) / 3;

    unsigned long num_tasks[num_levels];
    unsigned long min_times[num_levels];
    unsigned long max_times[num_levels];

    WRENCH_INFO("Creating a 'levels' workflow...");

    for (unsigned long l = 0; l < num_levels; l++) {
        if ((sscanf(spec_tokens[2 + l * 3].c_str(), "%lu", &(num_tasks[l])) != 1) or (num_tasks[l] < 1)) {
            throw std::invalid_argument(
                    "createLevelsWorkflow(): invalid number of tasks in level " + std::to_string(l) +
                    " workflow specification");
        }

        if ((sscanf(spec_tokens[2 + l * 3 + 1].c_str(), "%lu", &(min_times[l])) != 1)) {
            throw std::invalid_argument("createLevelsWorkflow(): invalid min task exec time in workflow specification");
        }
        if ((sscanf(spec_tokens[2 + l * 3 + 2].c_str(), "%lu", &(max_times[l])) != 1) or
            (max_times[l] < min_times[l])) {
            throw std::invalid_argument("createLevelsWorkflow(): invalid max task exec time in workflow specification");
        }
    }

    auto workflow = new Workflow();

    // Create the tasks
    std::vector<wrench::WorkflowTask *> tasks[num_levels];

    std::uniform_int_distribution<unsigned long> *m_udists[num_levels];
    for (unsigned long l = 0; l < num_levels; l++) {
        m_udists[l] = new std::uniform_int_distribution<unsigned long>(min_times[l], max_times[l]);
    }

    for (unsigned long l = 0; l < num_levels; l++) {
        for (unsigned long t = 0; t < num_tasks[l]; t++) {
            unsigned long flops = (*m_udists[l])(rng);
            wrench::WorkflowTask *task = workflow->addTask("Task_l" + std::to_string(l) + "_" +
                                                           std::to_string(t), (double) flops, 1, 1, 1.0);
            tasks[l].push_back(task);
        }
    }

    // Create the control dependencies (right now FULL dependencies)
    for (unsigned long l = 1; l < num_levels; l++) {
        for (unsigned long t = 0; t < num_tasks[l]; t++) {
            for (unsigned long p = 0; p < num_tasks[l - 1]; p++) {
                workflow->addControlDependency(tasks[l - 1][p], tasks[l][t]);
            }
        }
    }

//  workflow->exportToEPS("/tmp/foo.eps");

    return workflow;

}

Workflow *Simulator::createWorkflowFromFile(std::string type, std::vector<std::string> spec_tokens) {
    std::string filename = spec_tokens[1];

    Workflow *original_workflow = nullptr;
    try {
        if (type == "dax") {
            original_workflow = PegasusWorkflowParser::createWorkflowFromDAX(filename, "1");
        } else if (type == "json") {
            original_workflow = PegasusWorkflowParser::createWorkflowFromJSON(filename, "1");
        } else {
            throw std::runtime_error("Unknown workflow file type " + type);
        }
    } catch (std::invalid_argument &e) {
        throw std::runtime_error("Cannot import workflow from file");
    }

    auto workflow = new Workflow();

    // Add task replicas
    for (auto t : original_workflow->getTasks()) {
//    WRENCH_INFO("t->getFlops() = %lf", t->getFlops());
        workflow->addTask(t->getID(), t->getFlops(), 1, 1, 1.0);
    }

    // Deal with all dependencies (brute-force, but whatever)
    for (auto t : original_workflow->getTasks()) {
        std::vector<wrench::WorkflowTask *> parents = original_workflow->getTaskParents(t);
        std::vector<wrench::WorkflowTask *> children = original_workflow->getTaskChildren(t);

        for (auto p : parents) {
            std::string parent_id = p->getID();
            std::string child_id = t->getID();
            workflow->addControlDependency(workflow->getTaskByID(parent_id), workflow->getTaskByID(child_id));
        }
        for (auto c : children) {
            std::string parent_id = t->getID();
            std::string child_id = c->getID();
            workflow->addControlDependency(workflow->getTaskByID(parent_id), workflow->getTaskByID(child_id));
        }
    }
//  WRENCH_INFO("NEW WORKFLOW HAS %ld TASKS and %ld LEVELS", workflow->getNumberOfTasks(), workflow->getNumLevels());

    return workflow;

}

WMS *Simulator::createWMS(std::string hostname,
                          std::shared_ptr<BatchComputeService> batch_service,
                          unsigned long max_num_jobs,
                          std::string scheduler_spec) {

    std::istringstream ss(scheduler_spec);
    std::string token;
    std::vector<std::string> tokens;

    while (std::getline(ss, token, ':')) {
        tokens.push_back(token);
    }

    if (tokens[0] == "static") {

        if (tokens.size() != 2) {
            throw std::invalid_argument("createWMS(): Invalid static specification");
        }

        WMS *wms = new StaticClusteringWMS(this, hostname, batch_service, max_num_jobs, tokens[1]);
        return wms;

    } else if (tokens[0] == "zhang") {

        if (tokens.size() != 4) {
            throw std::invalid_argument("createWMS(): Invalid zhang specification");
        }

        bool global, bsearch, prediction;

        if (tokens[1] == "global") {
            global = true;
        } else if (tokens[1] == "noglobal") {
            global = false;
        } else {
            throw std::invalid_argument("createWMS(): Invalid zhang specification");
        }

        if (tokens[2] == "bsearch") {
            bsearch = true;
        } else if (tokens[2] == "nobsearch") {
            bsearch = false;
        } else {
            throw std::invalid_argument("createWMS(): Invalid zhang specification");
        }

        if (tokens[3] == "prediction") {
            prediction = true;
        } else if (tokens[3] == "noprediction") {
            prediction = false;
        } else {
            throw std::invalid_argument("createWMS(): Invalid zhang specification");
        }

        return new ZhangWMS(this, hostname, batch_service, max_num_jobs, global, bsearch, prediction);

    } else if (tokens[0] == "glume") {

        if (tokens.size() != 3) {
            throw std::invalid_argument("createWMS(): Invalid glume specification");
        }

        double waste_bound = std::stod(tokens[1]);
        double beat_bound = std::stod(tokens[2]);

        return new GlumeWMS(this, hostname, waste_bound, beat_bound, batch_service);

    } else if (tokens[0] == "levelbylevel") {
        if (tokens.size() != 3) {
            throw std::invalid_argument("createWMS(): Invalid levelbylevel specification");
        }
        bool overlap;
        if (tokens[1] == "overlap") {
            overlap = true;
        } else if (tokens[1] == "nooverlap") {
            overlap = false;
        } else {
            throw std::invalid_argument("createWMS(): Invalid levelbylevel specification");
        }
        return new LevelByLevelWMS(this, hostname, overlap, tokens[2], batch_service);

    } else {
        throw std::invalid_argument("createStandardJobScheduler(): Unknown algorithm type " + tokens[0]);
    }

}
