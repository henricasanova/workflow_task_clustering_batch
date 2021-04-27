

#include <stdio.h>

#include <Util/WorkflowUtil.h>
#include "StaticClusteringWMS.h"
#include "ClusteredJob.h"

using namespace wrench;


XBT_LOG_NEW_DEFAULT_CATEGORY(static_clustering_wms, "Log category for Static Clustering WMS");

StaticClusteringWMS::StaticClusteringWMS(Simulator *simulator, std::string hostname, std::shared_ptr<BatchComputeService> batch_service,
                                         unsigned long max_num_jobs, std::string algorithm_spec) :
        WMS(nullptr, nullptr, {batch_service}, {}, {}, nullptr, hostname, "static_clustering_wms") {
    this->simulator = simulator;
    this->batch_service = batch_service;
    this->max_num_jobs = max_num_jobs;
    this->algorithm_spec = algorithm_spec;
}


void StaticClusteringWMS::processEventStandardJobCompletion(std::shared_ptr<StandardJobCompletedEvent> e) {
    auto job = e->standard_job;
    WRENCH_INFO("Job %s has completed", job->getName().c_str());


    double first_task_start_time = DBL_MAX;
    for (auto const &t : job->getTasks()) {
        if (t->getStartDate() < first_task_start_time) {
            first_task_start_time = t->getStartDate();
        }
    }
    int num_requested_nodes = stoi(job->getServiceSpecificArguments()["-N"]);
    double job_duration = this->simulation->getCurrentSimulatedDate() - first_task_start_time;
    double wasted_node_seconds = num_requested_nodes * job_duration;
    for (auto const &t : job->getTasks()) {
        this->simulator->used_node_seconds += t->getFlops() / this->core_speed;
        wasted_node_seconds -= t->getFlops() / this->core_speed;
    }

    this->simulator->wasted_node_seconds = wasted_node_seconds;

    this->simulator->total_queue_wait_time += (first_task_start_time - job->getSubmitDate());

    this->num_jobs_in_systems--;
}


void StaticClusteringWMS::processEventStandardJobFailure(std::shared_ptr<StandardJobFailedEvent> e) {
    throw std::runtime_error("A job has failed, which shouldn't happen");
}


std::set<ClusteredJob *> StaticClusteringWMS::createClusteredJobs() {

    std::istringstream ss(this->algorithm_spec);
    std::string token;
    std::vector<std::string> tokens;

    std::set<ClusteredJob *> jobs;

    while (std::getline(ss, token, '-')) {
        tokens.push_back(token);
    }

    /** A level by level split **/
    if (tokens[0] == "levelbylevel") {
        if (tokens.size() != 2) {
            throw std::invalid_argument("Invalid static:levelbylevel specification");
        }

        unsigned long num_nodes;
        if ((sscanf(tokens[1].c_str(), "%lu", &num_nodes) != 1)) {
            throw std::invalid_argument("Invalid static:levelbylevel-m specification");
        }

        unsigned long numLevels = this->getWorkflow()->getNumLevels();
        for (unsigned long currLevel = 0; currLevel < numLevels; currLevel++) {
            ClusteredJob *job = new ClusteredJob();
            for (auto t : this->getWorkflow()->getTasksInTopLevelRange(currLevel, currLevel)) {
                job->addTask(t);
            }
            job->setNumNodes(num_nodes);
            jobs.insert(job);
        }

        return jobs;
    }

    /** A single Job **/
    // TODO This will probably be broken if not doing one_job-0
    if (tokens[0] == "one_job") {
        if (tokens.size() != 3) {
            throw std::invalid_argument("Invalid static:one_job specification");
        }

        unsigned long num_nodes;
        if ((sscanf(tokens[1].c_str(), "%lu", &num_nodes) != 1)) {
            throw std::invalid_argument("Invalid static:one_job-m specification");
        }
        
        double waste_bound = std::stod(tokens[2]);

        ClusteredJob *job = new ClusteredJob();
        for (auto t : this->getWorkflow()->getTasks()) {
            job->addTask(t);
        }
        job->setNumNodes(num_nodes);
        job->setWasteBound(waste_bound);
        jobs.insert(job);
        return jobs;
    }

    /** One Job per Task **/
    if (tokens[0] == "one_job_per_task") {
        if (tokens.size() != 1) {
            throw std::invalid_argument("Invalid static:one_job_per_task specification");
        }

        for (auto t : this->getWorkflow()->getTasks()) {
            ClusteredJob *job = new ClusteredJob();
            job->addTask(t);
            job->setNumNodes(1);
            jobs.insert(job);
        }
        return jobs;
    }

    /** Horizontal Clustering (HC) **/
    if (tokens[0] == "hc") {
        if (tokens.size() != 4) {
            throw std::invalid_argument("Invalid static:hc specification");
        }
        unsigned long num_tasks_per_cluster;
        unsigned long num_nodes_per_cluster;
        if ((sscanf(tokens[2].c_str(), "%lu", &num_tasks_per_cluster) != 1) or (num_tasks_per_cluster < 1) or
            (sscanf(tokens[3].c_str(), "%lu", &num_nodes_per_cluster) != 1)) {
            throw std::invalid_argument("Invalid static:hc specification");
        }
        WRENCH_INFO("TOKENS[1] = %s", tokens[1].c_str());

        if ((tokens[1] != "vprior") and (tokens[1] != "vposterior") and (tokens[1] != "vnone")) {
            throw std::runtime_error("Invalid static:hc specification");
        }
        return createHCJobs(tokens[1], num_tasks_per_cluster, num_nodes_per_cluster,
                            this->getWorkflow(), 0, this->getWorkflow()->getNumLevels() - 1);
    }

    /** DFJS Clustering **/
    if (tokens[0] == "dfjs") {
        if (tokens.size() != 4) {
            throw std::invalid_argument("Invalid static:dfjs specification");
        }
        unsigned long num_seconds_per_cluster;
        unsigned long num_nodes_per_cluster;
        if ((sscanf(tokens[2].c_str(), "%lu", &num_seconds_per_cluster) != 1) or (num_seconds_per_cluster < 1) or
            (sscanf(tokens[3].c_str(), "%lu", &num_nodes_per_cluster) != 1)) {
            throw std::invalid_argument("Invalid static:hc specification");
        }
        if ((tokens[1] != "vprior") and (tokens[1] != "vposterior") and (tokens[1] != "vnone")) {
            throw std::runtime_error("Invalid static:dfjs specification");
        }
        return createDFJSJobs(tokens[1], num_seconds_per_cluster, num_nodes_per_cluster,
                              this->core_speed, this->getWorkflow(), 0, this->getWorkflow()->getNumLevels() - 1);
    }

    /** HRB Clustering **/
    if (tokens[0] == "hrb") {
        if (tokens.size() != 4) {
            throw std::invalid_argument("Invalid static:hrb specification");
        }
        unsigned long num_tasks_per_cluster;
        unsigned long num_nodes_per_cluster;
        if ((sscanf(tokens[2].c_str(), "%lu", &num_tasks_per_cluster) != 1) or (num_tasks_per_cluster < 1) or
            (sscanf(tokens[3].c_str(), "%lu", &num_nodes_per_cluster) != 1)) {
            throw std::invalid_argument("Invalid static:hrb specification");
        }
        if ((tokens[1] != "vprior") and (tokens[1] != "vposterior") and (tokens[1] != "vnone")) {
            throw std::runtime_error("Invalid static:hrb specification");
        }
        return createHRBJobs(tokens[1], num_tasks_per_cluster, num_nodes_per_cluster,
                             this->core_speed, this->getWorkflow(), 0, this->getWorkflow()->getNumLevels() - 1);
    }

    /** HIFB Clustering **/
    if (tokens[0] == "hifb") {
        if (tokens.size() != 4) {
            throw std::invalid_argument("Invalid static:hifb specification");
        }
        unsigned long num_tasks_per_cluster;
        unsigned long num_nodes_per_cluster;
        if ((sscanf(tokens[2].c_str(), "%lu", &num_tasks_per_cluster) != 1) or (num_tasks_per_cluster < 1) or
            (sscanf(tokens[3].c_str(), "%lu", &num_nodes_per_cluster) != 1)) {
            throw std::invalid_argument("Invalid static:hifb specification");
        }
        if ((tokens[1] != "vprior") and (tokens[1] != "vposterior") and (tokens[1] != "vnone")) {
            throw std::runtime_error("Invalid static:hifb specification");
        }
        return createHIFBJobs(tokens[1], num_tasks_per_cluster, num_nodes_per_cluster,
                              this->getWorkflow(), 0, this->getWorkflow()->getNumLevels() - 1);
    }

    /** HDB Clustering **/
    if (tokens[0] == "hdb") {
        if (tokens.size() != 4) {
            throw std::invalid_argument("Invalid static:hdb specification");
        }
        unsigned long num_tasks_per_cluster;
        unsigned long num_nodes_per_cluster;
        if ((sscanf(tokens[2].c_str(), "%lu", &num_tasks_per_cluster) != 1) or (num_tasks_per_cluster < 1) or
            (sscanf(tokens[3].c_str(), "%lu", &num_nodes_per_cluster) != 1)) {
            throw std::invalid_argument("Invalid static:hdb specification");
        }
        if ((tokens[1] != "vprior") and (tokens[1] != "vposterior") and (tokens[1] != "vnone")) {
            throw std::runtime_error("Invalid static:hdb specification");
        }
        return createHDBJobs(tokens[1], num_tasks_per_cluster, num_nodes_per_cluster,
                             this->getWorkflow(), 0, this->getWorkflow()->getNumLevels() - 1);
    }

    /** VC Clustering **/
    if (tokens[0] == "vc") {
        if (tokens.size() != 1) {
            throw std::invalid_argument("Invalid static:vc specification");
        }
        return createVCJobs();
    }

    throw std::runtime_error("Unknown Static Job Clustering method " + tokens[0]);

}

int StaticClusteringWMS::main() {

    WRENCH_INFO("StaticClusteringWMS starting");

    // Acquire core speed the first time
    if (this->core_speed <= 0.0) {
        WRENCH_INFO("Asking the Batch Service for its core rate");
        this->core_speed = (*(this->batch_service->getCoreFlopRate().begin())).second;
    }

    WRENCH_INFO("Asking the Batch Service for its number of hosts");
    this->number_of_nodes = this->batch_service->getNumHosts();

    WRENCH_INFO("Got it!");
    this->checkDeferredStart();

    TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_YELLOW);
    WRENCH_INFO("About to execute a workflow with %lu tasks", this->getWorkflow()->getNumberOfTasks());

    // Create a job manager
    this->job_manager = this->createJobManager();

    // Compute the clustering according to the method
    std::set<ClusteredJob *> jobs = this->createClusteredJobs();

//  WRENCH_INFO("NUMBER OF CLUSTERS JOBS = %ld", jobs.size());
//  WRENCH_INFO("MAX NUM JOBS = %ld", this->max_num_jobs);

    this->num_jobs_in_systems = 0;

    while (true) {

        while (this->num_jobs_in_systems < this->max_num_jobs) {
            // Try to find a ready job
            ClusteredJob *to_submit = nullptr;
            for (auto j : jobs) {
//        WRENCH_INFO("IS THIS JOB READY?");
//        for (auto t : j->getTasks()) {
//          WRENCH_INFO("    - %s", t->getID().c_str());
//        }
                if (j->isReady()) {
                    to_submit = j;
                    break;
                }
            }
            if (to_submit == nullptr) {
                break;
            }

            // Submit the job
            submitClusteredJob(to_submit);
            jobs.erase(to_submit);
            this->num_jobs_in_systems++;
        }

        // Wait for a workflow execution event, and process it
        try {
//      WRENCH_INFO("Waiting for an event");
            this->waitForAndProcessNextEvent();
        } catch (WorkflowExecutionException &e) {
            WRENCH_INFO("Error while getting next execution event (%s)... ignoring and trying again",
                        (e.getCause()->toString().c_str()));
            continue;
        }

        // Are we done?
        if (this->getWorkflow()->isDone()) {
            break;
        }
    }

//  std::cout << "WORKFLOW EXECUTION COMPLETE: " <<  this->simulation->getCurrentSimulatedDate() << "\n";
    job_manager.reset();

    return 0;
}

void StaticClusteringWMS::submitClusteredJob(ClusteredJob *clustered_job) {

    // Compute the maximum (reasonable) number of nodes for the job
    unsigned long num_nodes = std::min<unsigned long>(clustered_job->getMaxParallelism(), clustered_job->getNumNodes());

    if (num_nodes == 0) {
        num_nodes = clustered_job->computeBestNumNodesBasedOnQueueWaitTimePredictions(
                std::min<unsigned long>(clustered_job->getMaxParallelism(), this->number_of_nodes), this->core_speed, this->batch_service);
    }

    // For one_job-max
    if (clustered_job->getNumNodes() == 100000) {
        num_nodes = std::min<unsigned long>(clustered_job->getMaxParallelism(), this->number_of_nodes);
    }

    num_nodes = std::min<unsigned long>(num_nodes, this->number_of_nodes);
    
    double makespan = WorkflowUtil::estimateMakespan(clustered_job->getTasks(), num_nodes, this->core_speed);
    // std::cout << "MAKESPAN ESTIMATE = " << makespan << "\n";

    std::map<std::string, std::string> batch_job_args;
    batch_job_args["-N"] = std::to_string(num_nodes);
    batch_job_args["-t"] = std::to_string(
            (unsigned long) (1 + (makespan * EXECUTION_TIME_FUDGE_FACTOR) / 60.0)); //time in minutes
    batch_job_args["-c"] = "1"; //number of cores per node

    auto standard_job = this->job_manager->createStandardJob(clustered_job->getTasks(), {});
    WRENCH_INFO("Created a batch job with with batch arguments: %s:%s:%s",
                batch_job_args["-N"].c_str(),
                batch_job_args["-t"].c_str(),
                batch_job_args["-c"].c_str());

    try {
        WRENCH_INFO("Submitting a batch job...");
        // std::cout << "REQUESTING " << (unsigned long) (1 + (makespan * EXECUTION_TIME_FUDGE_FACTOR)) << " " << num_nodes << "\n";
        this->job_manager->submitJob(standard_job, batch_service, batch_job_args);
//    this->job_map.insert(std::make_pair(standard_job, clustered_job));
    } catch (WorkflowExecutionException &e) {
        throw std::runtime_error("Couldn't submit job: " + e.getCause()->toString());
    }

}

std::set<ClusteredJob *> StaticClusteringWMS::createHCJobs(
        std::string vc, unsigned long num_tasks_per_cluster, unsigned long num_nodes_per_cluster,
        Workflow *workflow, unsigned long start_level, unsigned long end_level) {

    std::set<ClusteredJob *> jobs;

    if (vc == "vprior") {
        mergeSingleParentSingleChildPairs(workflow);
    }

    // Go through each level and creates jobs
    for (unsigned long l = start_level; l <= end_level; l++) {
        auto tasks_in_level = workflow->getTasksInTopLevelRange(l, l);
        ClusteredJob *job = nullptr;
        for (auto t : tasks_in_level) {
            if (job == nullptr) {
                job = new ClusteredJob();
                job->setNumNodes(num_nodes_per_cluster);
            }
            job->addTask(t);
            if (job->getNumTasks() == num_tasks_per_cluster) {
                jobs.insert(job);
                job = nullptr;
            }
        }
        if (job != nullptr) {
            jobs.insert(job);
        }
    }

    if (vc == "vposterior") {
        jobs = applyPosteriorVC(workflow, jobs);
    }

    return jobs;
}


std::set<ClusteredJob *> StaticClusteringWMS::createDFJSJobs(
        std::string vc, unsigned long num_seconds_per_cluster, unsigned long num_nodes_per_cluster,
        double core_speed,
        Workflow *workflow, unsigned long start_level, unsigned long end_level) {
    std::set<ClusteredJob *> jobs;


    if (vc == "vprior") {
        mergeSingleParentSingleChildPairs(workflow);
    }

    // Go through each level and creates jobs
    for (unsigned long l = start_level; l <= end_level; l++) {
        auto tasks_in_level = workflow->getTasksInTopLevelRange(l, l);

        auto job = new ClusteredJob();
        job->setNumNodes(num_nodes_per_cluster);
        for (auto t : tasks_in_level) {
            auto task_execution_time = (unsigned long) (ceil(t->getFlops() / core_speed));
            if (task_execution_time > num_seconds_per_cluster) {
                throw std::runtime_error(
                        "Task " + t->getID() + " by itself takes longer (" + std::to_string(task_execution_time) +
                        " sec) than the cluster duration upper bound ( " +
                        std::to_string(num_seconds_per_cluster) + " sec)!");
            }
            // Should we add to the job?
            std::vector<wrench::WorkflowTask *> tentative_tasks = job->getTasks();
            tentative_tasks.push_back(t);
            double estimated_makespan = WorkflowUtil::estimateMakespan(tentative_tasks, num_nodes_per_cluster,
                                                                       core_speed);
            if ((unsigned long) (ceil(estimated_makespan)) <= num_seconds_per_cluster) {
                job->addTask(t);
            } else {
                jobs.insert(job);
                job = new ClusteredJob();
                job->setNumNodes(num_nodes_per_cluster);
                job->addTask(t);
            }
        }
        jobs.insert(job);
    }

    if (vc == "vposterior") {
        jobs = applyPosteriorVC(workflow, jobs);
    }

    // Sanity check
    for (auto job : jobs) {
        if (job->getNumTasks() == 0) {
            throw std::runtime_error("DFJS Failure: some jobs have no tasks (likely the time bound is too low");
        }
    }

    WRENCH_INFO("DFJS clustering: %ld clusters", jobs.size());
    return jobs;
}


std::set<ClusteredJob *> StaticClusteringWMS::createHRBJobs(
        std::string vc, unsigned long num_tasks_per_cluster, unsigned long num_nodes_per_cluster,
        double core_speed, Workflow *workflow, unsigned long start_level, unsigned long end_level) {
    std::set<ClusteredJob *> jobs;

    if (vc == "vprior") {
        mergeSingleParentSingleChildPairs(workflow);
    }

    // Go through each level and creates jobs
    for (unsigned long l = start_level; l <= end_level; l++) {
        auto tasks_in_level = workflow->getTasksInTopLevelRange(l, l);

        // Create all the jobs
        unsigned long num_level_jobs = tasks_in_level.size() / num_tasks_per_cluster +
                                       (tasks_in_level.size() % num_tasks_per_cluster != 0);

        ClusteredJob *level_jobs[num_level_jobs];
        for (unsigned long i = 0; i < num_level_jobs; i++) {
            level_jobs[i] = new ClusteredJob();
            level_jobs[i]->setNumNodes(num_nodes_per_cluster);
        }

        // Sort the tasks by decreasing Flops
        std::sort(tasks_in_level.begin(), tasks_in_level.end(),
                  [](const wrench::WorkflowTask *t1, const wrench::WorkflowTask *t2) -> bool {
                      if (fabs(t1->getFlops() - t2->getFlops()) < 0.001) {
                          return ((uintptr_t) t1 > (uintptr_t) t2);
                      } else {
                          return (t1->getFlops() > t2->getFlops());
                      }
                  });

        // Assign each task to a job
        for (auto t : tasks_in_level) {
            // Find the job with the min completion time
            unsigned long selected_index = 0;
            for (unsigned long i = 1; i < num_level_jobs; i++) {
                double currently_selected_makespan = WorkflowUtil::estimateMakespan(
                        level_jobs[selected_index]->getTasks(),
                        num_nodes_per_cluster, core_speed);
                double candidate_makespan = WorkflowUtil::estimateMakespan(
                        level_jobs[i]->getTasks(),
                        num_nodes_per_cluster, core_speed);
                if ((candidate_makespan < currently_selected_makespan) and
                    (level_jobs[i]->getNumTasks() < num_tasks_per_cluster)) {
                    selected_index = i;
                }
            }
//      WRENCH_INFO("ADDING TASK (%lf) TO JOB %ld", t->getFlops(), selected_index);
            level_jobs[selected_index]->addTask(t);
        }

        // Put the jobs into the overall job set
        for (unsigned long i = 0; i < num_level_jobs; i++) {
            jobs.insert(level_jobs[i]);
        }

    }

    if (vc == "vposterior") {
        jobs = applyPosteriorVC(workflow, jobs);
    }

    return jobs;
}


std::set<ClusteredJob *> StaticClusteringWMS::createHIFBJobs(
        std::string vc, unsigned long num_tasks_per_cluster, unsigned long num_nodes_per_cluster,
        Workflow *workflow, unsigned long start_level, unsigned long end_level) {
    std::set<ClusteredJob *> jobs;

    if (vc == "vprior") {
        mergeSingleParentSingleChildPairs(workflow);
    }

    /** Compute all task "Impact Factors" **/
//  WRENCH_INFO("Compute all IFs");
    std::map<wrench::WorkflowTask *, double> impact_factors;
    for (unsigned long l = 0; l < workflow->getNumLevels(); l++) {
        unsigned long level = workflow->getNumLevels() - 1 - l;
        auto tasks_in_level = workflow->getTasksInTopLevelRange(level, level);
        for (auto t : tasks_in_level) {
            if (t->getNumberOfChildren() == 0) {
                impact_factors.insert(std::make_pair(t, 1.0));
            } else {
                double impact_factor = 0.0;
                for (auto child : workflow->getTaskChildren(t)) {
                    impact_factor += impact_factors[child] / child->getNumberOfParents();
                }
                impact_factors.insert(std::make_pair(t, impact_factor));
            }
        }
    }

//  for (auto f : impact_factors) {
//    WRENCH_INFO("   --> IF(%s) = %lf", f.first->getID().c_str(), f.second);
//  }

    /** Go through each level and creates jobs **/
    for (unsigned long l = start_level; l <= end_level; l++) {

        auto tasks_in_level = workflow->getTasksInTopLevelRange(l, l);

        // Create all the jobs
        unsigned long num_level_jobs = tasks_in_level.size() / num_tasks_per_cluster +
                                       (tasks_in_level.size() % num_tasks_per_cluster != 0);

        ClusteredJob **level_jobs = (ClusteredJob **) calloc(num_level_jobs, sizeof(ClusteredJob *));
        for (unsigned long i = 0; i < num_level_jobs; i++) {
            level_jobs[i] = new ClusteredJob();
            level_jobs[i]->setNumNodes(num_nodes_per_cluster);
        }

        // Sort the tasks by decreasing Flops
        std::sort(tasks_in_level.begin(), tasks_in_level.end(),
                  [](const wrench::WorkflowTask *t1, const wrench::WorkflowTask *t2) -> bool {

                      if (fabs(t1->getFlops() - t2->getFlops()) < 0.001) {
                          return ((uintptr_t) t1 > (uintptr_t) t2);
                      } else {
                          return (t1->getFlops() > t2->getFlops());
                      }
                  });

        // Assign each task to a job
        for (auto t : tasks_in_level) {

            // Compute IF similarity between jobs and the task that needs to be put in a job
            std::vector<std::pair<ClusteredJob *, double>> IF_similarity;
            IF_similarity.clear();
            for (unsigned long i = 0; i < num_level_jobs; i++) {

                // compute average impact_factor value
                double average_IF = 0.0;
                for (auto task_in_job : level_jobs[i]->getTasks()) {
                    average_IF += impact_factors[task_in_job];
                }
                average_IF += impact_factors[t];
                average_IF /= (level_jobs[i]->getNumTasks() + 1.0);

                // compute standard deviation
                double similarity = 0.0;
                for (auto task_in_job : level_jobs[i]->getTasks()) {
                    similarity += pow(impact_factors[task_in_job] - average_IF, 2.0);
                }
                similarity += pow(impact_factors[t] - average_IF, 2.0);

                similarity /= level_jobs[i]->getNumTasks();
                similarity = sqrt(similarity);
                IF_similarity.push_back(std::make_pair(level_jobs[i], similarity));
            }

//      for (auto p : IF_similarity) {
//        WRENCH_INFO("---> job with %ld tasks (%ld), %lf", p.first->getNumTasks(),  (unsigned long)(p.first), p.second);
//      }

            // Sort jobs by similarity, and makespan when similarity is the same
            std::sort(IF_similarity.begin(), IF_similarity.end(),
                      [num_nodes_per_cluster](const std::pair<ClusteredJob *, double> &t1,
                                              const std::pair<ClusteredJob *, double> &t2) -> bool {
                          double t1_similarity = t1.second;
                          double t2_similarity = t2.second;
                          ClusteredJob *t1_job = t1.first;
                          ClusteredJob *t2_job = t2.first;

//                    WRENCH_INFO("IN SORT: %lf %lf", t1_similarity, t2_similarity);
//                    WRENCH_INFO("  IN SORT: %ld %ld", (unsigned long)t1_job, (unsigned long)t2_job);

                          if (fabs(t1_similarity - t2_similarity) < 0.01) { // IMPORTANT TO NOT USE EQUAL!
                              double t1_makespan = WorkflowUtil::estimateMakespan(t1_job->getTasks(),
                                                                                  num_nodes_per_cluster, 1.0);
                              double t2_makespan = WorkflowUtil::estimateMakespan(t2_job->getTasks(),
                                                                                  num_nodes_per_cluster, 1.0);
                              if (fabs(t1_makespan - t2_makespan) < 0.01) {
                                  return ((uintptr_t) &t1 > (uintptr_t) &t2);
                              } else {
                                  return (t1_makespan < t2_makespan);
                              }
                          } else {
                              return (t1_similarity < t2_similarity);
                          }
                          return true;
                      });

            // Go through the list of j ob and add the task to the first one that works
            bool task_was_put_into_job = false;
            for (auto p : IF_similarity) {
                ClusteredJob *job = std::get<0>(p);
                if (job->getNumTasks() < num_tasks_per_cluster) {
                    job->addTask(t);
//          WRENCH_INFO("PUTTING TASK %s into job %ld", t->getID().c_str(), (unsigned long)(job));
                    task_was_put_into_job = true;
                    break;
                }
            }

            if (not task_was_put_into_job) {
                throw std::runtime_error("Cannot put task " + t->getID() + " into any cluster!");
            }

        }

        // Put the jobs into the overall job set
        for (unsigned long i = 0; i < num_level_jobs; i++) {
            jobs.insert(level_jobs[i]);
        }

    }

    if (vc == "vposterior") {
        jobs = applyPosteriorVC(workflow, jobs);
    }

    return jobs;
}


std::set<ClusteredJob *> StaticClusteringWMS::createHDBJobs(
        std::string vc, unsigned long num_tasks_per_cluster, unsigned long num_nodes_per_cluster,
        Workflow *workflow, unsigned long start_level, unsigned long end_level) {
    std::set<ClusteredJob *> jobs;

    if (vc == "vprior") {
        mergeSingleParentSingleChildPairs(workflow);
    }

    /** Compute all task distances **/
    std::map<std::pair<wrench::WorkflowTask *, wrench::WorkflowTask *>, unsigned long> task_distances;
    for (unsigned long l = 0; l <= workflow->getNumLevels(); l++) {
        unsigned long level = workflow->getNumLevels() - 1 - l;
        std::vector<wrench::WorkflowTask *> tasks_in_level = workflow->getTasksInTopLevelRange(level, level);
        // Last level
        if (level == workflow->getNumLevels() - 1) {
            for (auto u : tasks_in_level) {
                for (auto v : tasks_in_level) {
                    if (u != v) {
                        task_distances.insert(std::make_pair(std::make_pair(u, v), 10000000.0));  // infty?
                    }
                }
            }
        } else {
            for (auto u : tasks_in_level) {
                for (auto v : tasks_in_level) {
                    if (u != v) {
                        std::vector<wrench::WorkflowTask *> u_children = workflow->getTaskChildren(u);
                        std::vector<wrench::WorkflowTask *> v_children = workflow->getTaskChildren(v);
                        double min_distance = -1.0;
                        for (auto cu : u_children) {
                            for (auto cv : v_children) {
                                if ((min_distance == -1.0) or (task_distances[std::make_pair(cu, cv)] < min_distance)) {
                                    min_distance = task_distances[std::make_pair(cu, cv)];
                                }
                            }
                        }
                        task_distances.insert(std::make_pair(std::make_pair(u, v), 2 + min_distance));
                    }
                }
            }
        }
    }

    // DEBUG
//  for (unsigned long l = 0; l <= this->getWorkflow()->getNumLevels(); l++) {
//    WRENCH_INFO("LEVEL %ld", l);
//    std::vector<wrench::WorkflowTask *> tasks_in_level = this->getWorkflow()->getTasksInTopLevelRange(l,l);
//
//    for (auto u : tasks_in_level) {
//      for (auto v : tasks_in_level) {
//        if (u != v) {
//          WRENCH_INFO("  DISTANCE(%s,%s) = %lu",
//                      u->getID().c_str(), v->getID().c_str(), task_distances[std::make_pair(u,v)]);
//        }
//      }
//    }
//
//  }


    /** Go through each level and creates jobs **/
    for (unsigned long l = start_level; l <= end_level; l++) {

        auto tasks_in_level = workflow->getTasksInTopLevelRange(l, l);

        // Create all the jobs
        unsigned long num_level_jobs = tasks_in_level.size() / num_tasks_per_cluster +
                                       (tasks_in_level.size() % num_tasks_per_cluster != 0);

        ClusteredJob **level_jobs = (ClusteredJob **) calloc(num_level_jobs, sizeof(ClusteredJob *));
        for (unsigned long i = 0; i < num_level_jobs; i++) {
            level_jobs[i] = new ClusteredJob();
            level_jobs[i]->setNumNodes(num_nodes_per_cluster);
        }

        // Sort the tasks by decreasing Flops
        std::sort(tasks_in_level.begin(), tasks_in_level.end(),
                  [](const wrench::WorkflowTask *t1, const wrench::WorkflowTask *t2) -> bool {

                      if (fabs(t1->getFlops() - t2->getFlops()) < 0.001) {
                          return ((uintptr_t) t1 > (uintptr_t) t2);
                      } else {
                          return (t1->getFlops() > t2->getFlops());
                      }
                  });

        // Assign each task to a job
        for (auto t : tasks_in_level) {

            // Compute distance similarity between jobs and the task that needs to be put in a job
            std::vector<std::pair<ClusteredJob *, double>> distance_similarity;
            distance_similarity.clear();

            for (unsigned long i = 0; i < num_level_jobs; i++) {

                // compute  distance similarity
                double average_distance = 0.0;
                for (auto u : level_jobs[i]->getTasks()) {
                    for (auto v : level_jobs[i]->getTasks()) {
                        if (u == v) {
                            continue;
                        }
                        average_distance += task_distances[std::make_pair(u, v)];
                    }
                    average_distance += task_distances[std::make_pair(u, t)];
                }
                average_distance /= pow(level_jobs[i]->getNumTasks(), 2.0);

                // compute standard deviation
                double similarity = 0.0;
                for (auto u : level_jobs[i]->getTasks()) {
                    for (auto v : level_jobs[i]->getTasks()) {
                        if (u == v) {
                            continue;
                        }
                        similarity += pow(task_distances[std::make_pair(u, v)] - average_distance, 2.0);
                    }
                    similarity += pow(task_distances[std::make_pair(u, t)] - average_distance, 2.0);
                }

                similarity /= pow(level_jobs[i]->getNumTasks(), 2.0) - 1;
                similarity = sqrt(similarity);
                distance_similarity.push_back(std::make_pair(level_jobs[i], similarity));
            }

//      for (auto p : IF_similarity) {
//        WRENCH_INFO("---> job with %ld tasks (%ld), %lf", p.first->getNumTasks(),  (unsigned long)(p.first), p.second);
//      }

            // Sort jobs by similarity, and makespan when similarity is the same
            std::sort(distance_similarity.begin(), distance_similarity.end(),
                      [num_nodes_per_cluster](const std::pair<ClusteredJob *, double> &t1,
                                              const std::pair<ClusteredJob *, double> &t2) -> bool {
                          double t1_similarity = t1.second;
                          double t2_similarity = t2.second;
                          ClusteredJob *t1_job = t1.first;
                          ClusteredJob *t2_job = t2.first;

//                    WRENCH_INFO("IN SORT: %lf %lf", t1_similarity, t2_similarity);
//                    WRENCH_INFO("  IN SORT: %ld %ld", (unsigned long)t1_job, (unsigned long)t2_job);

                          if (fabs(t1_similarity - t2_similarity) < 0.01) { // IMPORTANT TO NOT USE EQUAL!
                              double t1_makespan = WorkflowUtil::estimateMakespan(t1_job->getTasks(),
                                                                                  num_nodes_per_cluster, 1.0);
                              double t2_makespan = WorkflowUtil::estimateMakespan(t2_job->getTasks(),
                                                                                  num_nodes_per_cluster, 1.0);
                              if (fabs(t1_makespan - t2_makespan) < 0.01) {
                                  return ((uintptr_t) &t1 > (uintptr_t) &t2);
                              } else {
                                  return (t1_makespan < t2_makespan);
                              }
                          } else {
                              return (t1_similarity < t2_similarity);
                          }
                          return true;
                      });

            // Go through the list of j ob and add the task to the first one that works
            bool task_was_put_into_job = false;
            for (auto p : distance_similarity) {
                ClusteredJob *job = std::get<0>(p);
                if (job->getNumTasks() < num_tasks_per_cluster) {
                    job->addTask(t);
//          WRENCH_INFO("PUTTING TASK %s into job %ld", t->getID().c_str(), (unsigned long)(job));
                    task_was_put_into_job = true;
                    break;
                }
            }

            if (not task_was_put_into_job) {
                throw std::runtime_error("Cannot put task " + t->getID() + " into any cluster!");
            }

        }

        // Put the jobs into the overall job set
        for (unsigned long i = 0; i < num_level_jobs; i++) {
            jobs.insert(level_jobs[i]);
        }

    }

    if (vc == "vposterior") {
        jobs = applyPosteriorVC(workflow, jobs);
    }

    return jobs;
}


void StaticClusteringWMS::mergeSingleParentSingleChildPairs(Workflow *workflow) {
// Modify the workflow to cluster tasks
    while (true) {
        std::vector<wrench::WorkflowTask *> tasks = workflow->getTasks();
        wrench::WorkflowTask *parent_to_merge = nullptr;
        wrench::WorkflowTask *child_to_merge = nullptr;
        for (auto t : tasks) {
            if ((t->getNumberOfChildren() == 1) and
                (workflow->getTaskChildren(t)[0]->getNumberOfParents() == 1)) {
                parent_to_merge = t;
                child_to_merge = workflow->getTaskChildren(t)[0];
                break;
            }
        }
        if (parent_to_merge == nullptr) {
            break;
        }
        // do the merge
        // WRENCH_INFO("MERGING %s and %s", parent_to_merge->getID().c_str(), child_to_merge->getID().c_str());

        wrench::WorkflowTask *merged_task = workflow->addTask(
                parent_to_merge->getID() + "_" + child_to_merge->getID(),
                parent_to_merge->getFlops() + child_to_merge->getFlops(),
                1, 1, 1.0);

        for (auto parent : workflow->getTaskParents(parent_to_merge)) {
            workflow->addControlDependency(parent, merged_task);
        }
        for (auto child : workflow->getTaskChildren(child_to_merge)) {
            workflow->addControlDependency(merged_task, child);
        }

        workflow->removeTask(parent_to_merge);
        workflow->removeTask(child_to_merge);

    }
}

std::set<ClusteredJob *> StaticClusteringWMS::createVCJobs() {

    mergeSingleParentSingleChildPairs(this->getWorkflow());

    // Created one job per "task"
    std::set<ClusteredJob *> jobs;
    for (auto t : this->getWorkflow()->getTasks()) {
        ClusteredJob *job = new ClusteredJob();
        job->addTask(t);
        job->setNumNodes(1);
        jobs.insert(job);
    }
    return jobs;

}


std::set<ClusteredJob *>
StaticClusteringWMS::applyPosteriorVC(Workflow *workflow, std::set<ClusteredJob *> input_jobs) {
    std::set<ClusteredJob *> output_jobs;

    // Copy input to output
    for (auto j : input_jobs) {
        output_jobs.insert(j);
    }


    while (true) {
        ClusteredJob *to_merge_1 = nullptr, *to_merge_2 = nullptr;
        for (auto j1 : output_jobs) {
            for (auto j2 : output_jobs) {
                if (j1 == j2) continue;
                if (areJobsMergable(workflow, j1, j2)) {
                    to_merge_1 = j1;
                    to_merge_2 = j2;
                    break;
                }
            }
            if (to_merge_1 != nullptr) {
                break;
            }
        }

        if (to_merge_1 != nullptr) {
            /** Do the merge **/
            ClusteredJob *new_job = new ClusteredJob();
            // Set the number of nodes
            if (to_merge_1->getNumNodes() != to_merge_2->getNumNodes()) {
                throw std::runtime_error("Posterior VC: Don't know how to merge jobs with different numbers of nodes");
            }
            new_job->setNumNodes(to_merge_1->getNumNodes());
            // Add the tasks
            for (auto t : to_merge_1->getTasks()) {
                new_job->addTask(t);
            }
            for (auto t : to_merge_2->getTasks()) {
                new_job->addTask(t);
            }
            // Add the job
            output_jobs.insert(new_job);
            // Remove the old ones
            output_jobs.erase(to_merge_1);
            output_jobs.erase(to_merge_2);


        } else {
            // Couldn't find another merge
            break;
        }
    }

    return output_jobs;
}

bool StaticClusteringWMS::areJobsMergable(Workflow *workflow, ClusteredJob *j1, ClusteredJob *j2) {

    return isSingleParentSingleChildPair(workflow, j1, j2) or
           isSingleParentSingleChildPair(workflow, j2, j1);

}

bool StaticClusteringWMS::isSingleParentSingleChildPair(Workflow *workflow, ClusteredJob *pj, ClusteredJob *cj) {

    for (auto parent_task : pj->getTasks()) {
        for (auto child_task : workflow->getTaskChildren(parent_task)) {
            std::vector<wrench::WorkflowTask *> cj_tasks = cj->getTasks();
            bool child_task_in_cj = std::find(cj_tasks.begin(), cj_tasks.end(), child_task) != cj_tasks.end();
            std::vector<wrench::WorkflowTask *> pj_tasks = pj->getTasks();
            bool child_task_in_pj = std::find(pj_tasks.begin(), pj_tasks.end(), child_task) != pj_tasks.end();
            if ((not child_task_in_cj) and (not child_task_in_pj)) {
                return false;
            }
        }
    }
    return true;
}
