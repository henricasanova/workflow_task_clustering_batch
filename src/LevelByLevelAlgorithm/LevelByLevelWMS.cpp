/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */


#include <wms/WMS.h>
#include <workflow/job/PilotJob.h>
#include <logging/TerminalOutput.h>
#include <managers/JobManager.h>
#include <StaticClusteringAlgorithms/ClusteredJob.h>
#include <StaticClusteringAlgorithms/StaticClusteringWMS.h>
#include "Simulator.h"
#include "LevelByLevelWMS.h"
#include "OngoingLevel.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(level_by_level_clustering_wms, "Log category for Level-by-Level Clustering WMS");

typedef unsigned long ulong;

namespace wrench {

    LevelByLevelWMS::LevelByLevelWMS(Simulator *simulator, std::string hostname, bool overlap,
                                     std::string clustering_spec,
                                     std::shared_ptr<BatchComputeService> batch_service) :
            WMS(nullptr, nullptr, {batch_service}, {}, {}, nullptr, hostname, "clustering_wms") {
        this->simulator = simulator;
        this->overlap = overlap;
        this->batch_service = batch_service;
        this->clustering_spec = clustering_spec;

        // TODO - Overlapping is broken due to leeway issues
        assert(not this->overlap);
    }


    int LevelByLevelWMS::main() {

        TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_CYAN);

        this->checkDeferredStart();

        // Find out core speed on the batch service
        this->core_speed = (*(this->batch_service->getCoreFlopRate().begin())).second;
        // Find out #hosts on the batch service
        this->number_of_nodes = this->batch_service->getNumHosts();

        // Create a job manager
        this->job_manager = this->createJobManager();

        while (not this->getWorkflow()->isDone()) {

            submitPilotJobsForNextLevel();

            this->waitForAndProcessNextEvent();

        }

        return 0;
    }


    void LevelByLevelWMS::submitPilotJobsForNextLevel() {

        WRENCH_INFO("Seeing if I can submit jobs for the 'next' level...");

        // If more than 2 levels are going on, forget it
        if (this->ongoing_levels.size() >= 2) {

            WRENCH_INFO("Too many ongoing levels going on... will try later");
            return;
        }

        if (not(this->overlap or ongoing_levels.empty())) {
            return;
        }

        // TODO - next level to submit? What if incomplete tasks?

        // Compute which level should be submitted
        ulong level_to_submit = this->last_level_completed;

        for (auto l : this->ongoing_levels) {
            ulong level_number = l.second->level_number;
            if (level_to_submit < level_number) {
                level_to_submit = level_number;
            }
        }

        if (level_to_submit == ULONG_MAX) {
            level_to_submit = 0;
        } else {
            level_to_submit += 1;
        }

        if (level_to_submit >= this->getWorkflow()->getNumLevels()) {

            WRENCH_INFO("All workflow levels have been submitted!");

//            std::cout << "All levels have already been submitted" << std::endl;

            return;
        }

        // Make sure that all PH jobs in the previous level have started
        if (level_to_submit > 0 and (this->ongoing_levels.find(level_to_submit) != this->ongoing_levels.end())) {
            if (not(this->ongoing_levels[level_to_submit - 1]->pending_placeholder_jobs.empty())) {

                WRENCH_INFO(
                        "Cannot submit pilot jobs for level %ld since level %ld still has "
                        "pilot jobs that haven't started yet", level_to_submit, level_to_submit - 1);

                return;
            }
        }

        if (level_to_submit > 0) {

            WRENCH_INFO("All pilot jobs from level %ld have started... off I go with level %ld!",
                        level_to_submit - 1, level_to_submit);

        } else {

            WRENCH_INFO("Starting the first level!");

        }

        WRENCH_INFO("Creating a new ongoing level for level %lu", level_to_submit);

//        printf("Creating a new ongoing level for level %lu of %lu\n", level_to_submit, (this->getWorkflow()->getNumLevels() - 1));

        OngoingLevel *new_ongoing_level = new OngoingLevel();
        new_ongoing_level->level_number = level_to_submit;

        // Create all placeholder jobs for level
        std::set<PlaceHolderJob *> placeholder_jobs;

        placeholder_jobs = createPlaceHolderJobsForLevel(level_to_submit);

        // TODO - Must calculate leeway
        // Submit placeholder jobs
        for (auto ph : placeholder_jobs) {
            new_ongoing_level->pending_placeholder_jobs.insert(ph);

            // Create the pilot job
            double makespan = ph->clustered_job->estimateMakespan(this->core_speed) * EXECUTION_TIME_FUDGE_FACTOR;

            // Create the pilot job
            ph->pilot_job = this->job_manager->createPilotJob();

            // submit the corresponding pilot job
            std::map<std::string, std::string> service_specific_args;
            service_specific_args["-N"] = std::to_string(ph->clustered_job->getNumNodes());
            service_specific_args["-c"] = std::to_string(1);
            service_specific_args["-t"] = std::to_string(1 + ((ulong) (makespan) / 60));
            this->job_manager->submitJob(ph->pilot_job, this->batch_service,
                                         service_specific_args);

            WRENCH_INFO("Submitted a Pilot Job (%s hosts, %s min) for workflow level %lu (%s)",
                        service_specific_args["-N"].c_str(),
                        service_specific_args["-t"].c_str(),
                        level_to_submit,
                        ph->pilot_job->getName().c_str());

            WRENCH_INFO("This pilot job has these tasks:");

            for (auto t : ph->clustered_job->getTasks()) {

                WRENCH_INFO("     - %s (flops: %lf)", t->getID().c_str(),t->getFlops());
            }
        }

        ongoing_levels.insert(std::make_pair(level_to_submit, new_ongoing_level));
    }


    std::set<PlaceHolderJob *> LevelByLevelWMS::createPlaceHolderJobsForLevel(ulong level) {

        /** Identify relevant tasks **/
        WRENCH_INFO("IN CREATE PLACE HOLDER JOBS FOR LEVEL %lu", level);
        std::vector<WorkflowTask *> tasks_to_submit;

        std::vector<WorkflowTask *> tasks_in_level = this->getWorkflow()->getTasksInTopLevelRange(level, level);

        for (auto t : tasks_in_level) {
            if (t->getState() != WorkflowTask::COMPLETED) {
                tasks_to_submit.push_back(t);
            }
        }

        /** Apply Clustering heuristics **/
        std::istringstream ss(this->clustering_spec);
        std::string token;
        std::vector<std::string> tokens;

        while (std::getline(ss, token, '-')) {
            tokens.push_back(token);
        }

        /** Invoke task clustering heuristics **/
        std::set<ClusteredJob *> clustered_jobs;
        if (tokens[0] == "one_job") {
            if (tokens.size() != 2) {
                throw std::runtime_error(
                        "createPlaceHolderJobsForLevel(): Invalid clustering spec " + this->clustering_spec);
            }
            ulong num_nodes_per_cluster;
            if ((sscanf(tokens[1].c_str(), "%lu", &num_nodes_per_cluster) != 1)) {
                throw std::invalid_argument("Invalid one_job specification");
            }

            auto job = new ClusteredJob();

            for (auto t : tasks_to_submit) {
                job->addTask(t);
            }

            job->setNumNodes(num_nodes_per_cluster);

            WRENCH_INFO("NUM tasks in job: %lu", job->getNumTasks());

            // Use prediction to compute the best number of nodes if oj-0
            if (job->getNumNodes() == 0) {
                // std::cout << "num tasks in job: " << job->getNumTasks() << std::endl;
                ulong num_nodes = job->computeBestNumNodesBasedOnQueueWaitTimePredictions(
                        std::min<ulong>(job->getNumTasks(), this->number_of_nodes), this->core_speed,
                        this->batch_service);
                job->setNumNodes(num_nodes, true);
            }

            WRENCH_INFO("Num requested nodes: %lu", job->getNumNodes());

            clustered_jobs.insert(job);

        } else if (tokens[0] == "one_job_per_task") {
            if (tokens.size() != 1) {
                throw std::runtime_error(
                        "createPlaceHolderJobsForLevel(): Invalid clustering spec " + this->clustering_spec);
            }
            for (auto t : tasks_to_submit) {
                auto job = new ClusteredJob();
                job->addTask(t);
                job->setNumNodes(1);
                clustered_jobs.insert(job);
            }

        } else if (tokens[0] == "hc") {
            if (tokens.size() != 3) {
                throw std::runtime_error(
                        "createPlaceHolderJobsForLevel(): Invalid clustering spec " + this->clustering_spec);
            }
            ulong num_tasks_per_cluster;
            ulong num_nodes_per_cluster;
            if ((sscanf(tokens[1].c_str(), "%lu", &num_tasks_per_cluster) != 1) or (num_tasks_per_cluster < 1) or
                (sscanf(tokens[2].c_str(), "%lu", &num_nodes_per_cluster) != 1)) {
                throw std::invalid_argument("Invalid hc specification");
            }
            // Compute clusters (could be 0 nodes, in which case queue prediction will be triggered)
            clustered_jobs = StaticClusteringWMS::createHCJobs(
                    "none", num_tasks_per_cluster, num_nodes_per_cluster,
                    this->getWorkflow(), level, level);

        } else if (tokens[0] == "djfs") {
            if (tokens.size() != 4) {
                throw std::runtime_error(
                        "createPlaceHolderJobsForLevel(): Invalid clustering spec " + this->clustering_spec);
            }
            ulong num_seconds_per_cluster;
            ulong num_nodes_to_compute_clustering;
            ulong num_nodes_per_cluster;
            if ((sscanf(tokens[1].c_str(), "%lu", &num_seconds_per_cluster) != 1) or (num_seconds_per_cluster < 1) or
                (sscanf(tokens[2].c_str(), "%lu", &num_nodes_to_compute_clustering) != 1) or
                (sscanf(tokens[3].c_str(), "%lu", &num_nodes_per_cluster) != 1)) {
                throw std::invalid_argument("Invalid djfs specification");
            }
            // Compute clusters
            clustered_jobs = StaticClusteringWMS::createDFJSJobs(
                    "none", num_seconds_per_cluster, num_nodes_to_compute_clustering, this->core_speed,
                    this->getWorkflow(), level, level);
            // Now set the num nodes to the effective one to use (if 0, queue wait time predictions will be triggered)
            for (auto cj : clustered_jobs) {
                cj->setNumNodes(num_nodes_per_cluster);
            }

        } else if (tokens[0] == "hrb") {
            if (tokens.size() != 3) {
                throw std::runtime_error(
                        "createPlaceHolderJobsForLevel(): Invalid clustering spec " + this->clustering_spec);
            }
            ulong num_tasks_per_cluster;
            ulong num_nodes_per_cluster;
            if ((sscanf(tokens[1].c_str(), "%lu", &num_tasks_per_cluster) != 1) or (num_tasks_per_cluster < 1) or
                (sscanf(tokens[2].c_str(), "%lu", &num_nodes_per_cluster) != 1)) {
                throw std::invalid_argument("Invalid hrb specification");
            }
            // Compute clusters (could be 0 nodes, in which case queue prediction will be triggered)
            clustered_jobs = StaticClusteringWMS::createHRBJobs(
                    "none", num_tasks_per_cluster, num_nodes_per_cluster, this->core_speed,
                    this->getWorkflow(), level, level);

        } else if (tokens[0] == "hifb") {
            if (tokens.size() != 3) {
                throw std::runtime_error(
                        "createPlaceHolderJobsForLevel(): Invalid clustering spec " + this->clustering_spec);
            }
            ulong num_tasks_per_cluster;
            ulong num_nodes_per_cluster;
            if ((sscanf(tokens[1].c_str(), "%lu", &num_tasks_per_cluster) != 1) or (num_tasks_per_cluster < 1) or
                (sscanf(tokens[2].c_str(), "%lu", &num_nodes_per_cluster) != 1)) {
                throw std::invalid_argument("Invalid hifb specification");
            }
            // Compute clusters (could be 0 nodes, in which case queue prediction will be triggered)
            clustered_jobs = StaticClusteringWMS::createHIFBJobs(
                    "none", num_tasks_per_cluster, num_nodes_per_cluster,
                    this->getWorkflow(), level, level);

        } else if (tokens[0] == "hdb") {
            if (tokens.size() != 3) {
                throw std::runtime_error(
                        "createPlaceHolderJobsForLevel(): Invalid clustering spec " + this->clustering_spec);
            }
            ulong num_tasks_per_cluster;
            ulong num_nodes_per_cluster;
            if ((sscanf(tokens[1].c_str(), "%lu", &num_tasks_per_cluster) != 1) or (num_tasks_per_cluster < 1) or
                (sscanf(tokens[2].c_str(), "%lu", &num_nodes_per_cluster) != 1)) {
                throw std::invalid_argument("Invalid hdb specification");
            }
            // Compute clusters (could be 0 nodes, in which case queue prediction will be triggered)
            clustered_jobs = StaticClusteringWMS::createHDBJobs(
                    "none", num_tasks_per_cluster, num_nodes_per_cluster,
                    this->getWorkflow(), level, level);

        } else if (tokens[0] == "clever") {
            if (tokens.size() != 1) {
                throw std::runtime_error(
                        "createPlaceHolderJobsForLevel(): Invalid clustering spec " + this->clustering_spec);
            }
            // TODO: IMPLEMENT SOMETHING!
            throw std::runtime_error("'clever' clustering heuristic not implemented yet!");

        } else {
            throw std::runtime_error(
                    "createPlaceHolderJobsForLevel(): Unknown clustering spec " + this->clustering_spec);
        }


        /** Transform clustered jobs into PlaceHolderJobs */
        std::set<PlaceHolderJob *> place_holder_jobs;
        for (auto cj : clustered_jobs) {

            // Create the placeholder job (with a for-now nullptr PilotJob)
            WRENCH_INFO("Creating a placeholder job for level %ld based on a clustered job with %ld tasks",
                        level, cj->getNumTasks());
            PlaceHolderJob *ph = new PlaceHolderJob(nullptr, cj, level, level);

            // Add it to the set
            place_holder_jobs.insert(ph);
        }

        return place_holder_jobs;
    }


    void LevelByLevelWMS::processEventPilotJobStart(std::shared_ptr<PilotJobStartedEvent> e) {
        // Just for kicks, check it was the pending one
        WRENCH_INFO("Got a Pilot Job Start event: %s", e->pilot_job->getName().c_str());

//        std::cout << "Got a pilot job start event" << std::endl;

        // Update queue waiting time
        this->simulator->total_queue_wait_time +=
                this->simulation->getCurrentSimulatedDate() - e->pilot_job->getSubmitDate();

        // Find the placeholder job in the pending list
        PlaceHolderJob *placeholder_job = nullptr;
        OngoingLevel *ongoing_level = nullptr;
        for (auto ol : this->ongoing_levels) {
            for (auto ph : ol.second->pending_placeholder_jobs) {
                if (ph->pilot_job == e->pilot_job) {
                    placeholder_job = ph;
                    ongoing_level = ol.second;
                    break;
                }
            }
        }

        if (placeholder_job == nullptr) {
            throw std::runtime_error("Fatal Error: couldn't find a placeholder job for a pilot job that just started");
        }

        WRENCH_INFO("The corresponding placeholder job has %ld tasks",
                    placeholder_job->clustered_job->getTasks().size());

//        printf("The corresponding placeholder job has %ld tasks\n",
//               placeholder_job->clustered_job->getTasks().size());

        // Mote the placeholder job to running
        ongoing_level->pending_placeholder_jobs.erase(placeholder_job);
        ongoing_level->running_placeholder_jobs.insert(placeholder_job);

        // Submit all ready tasks to it each in its standard job
        std::string output_string = "";
        for (auto task : placeholder_job->clustered_job->getTasks()) {
            if (task->getState() == WorkflowTask::READY) {
                auto standard_job = this->job_manager->createStandardJob(task, {});
                output_string += " " + task->getID();

                WRENCH_INFO("Submitting task %s as part of placeholder job %ld-%ld",
                            task->getID().c_str(), placeholder_job->start_level, placeholder_job->end_level);
                this->job_manager->submitJob(standard_job, placeholder_job->pilot_job->getComputeService());
            } else { WRENCH_INFO("Task %s is not ready", task->getID().c_str());
            }
        }

    }


    void LevelByLevelWMS::processEventPilotJobExpiration(std::shared_ptr<PilotJobExpiredEvent> e) {

//        std::cout << "GOT AN EXPIRATION" << std::endl;

        // Find the placeholder job
        // Find the placeholder job in the pending list
        PlaceHolderJob *placeholder_job = nullptr;
        OngoingLevel *ongoing_level = nullptr;

        for (auto ol : this->ongoing_levels) {
            for (auto ph : ol.second->running_placeholder_jobs) {
                if (ph->pilot_job == e->pilot_job) {
                    placeholder_job = ph;
                    ongoing_level = ol.second;
                    break;
                }
            }
        }

        if (placeholder_job == nullptr) {
            throw std::runtime_error("Got a pilot job expiration, but no matching placeholder job found");
        }

        WRENCH_INFO("Got a pilot job expiration for a placeholder job that deals with levels %ld-%ld (%s)",
                    placeholder_job->start_level, placeholder_job->end_level,
                    placeholder_job->pilot_job->getName().c_str());

//        printf("Got a pilot job expiration for a placeholder job that deals with levels %ld-%ld (%s)\n",
//               placeholder_job->start_level, placeholder_job->end_level,
//               placeholder_job->pilot_job->getName().c_str());

        // Check if there are unprocessed tasks
        bool unprocessed = false;
        for (auto t : placeholder_job->clustered_job->getTasks()) {
            if (t->getState() != WorkflowTask::COMPLETED) {
                unprocessed = true;
                break;
            }
        }

//      double wasted_node_seconds = e->pilot_job->getNumHosts() * e->pilot_job->getDuration();
        ulong num_used_nodes;
        sscanf(e->pilot_job->getServiceSpecificArguments()["-N"].c_str(), "%lu", &num_used_nodes);
        ulong num_used_minutes;
        sscanf(e->pilot_job->getServiceSpecificArguments()["-t"].c_str(), "%lu", &num_used_minutes);
        double wasted_node_seconds = 60.0 * num_used_minutes * num_used_nodes;
        
        double used_seconds = 0;
        for (auto t : placeholder_job->clustered_job->getTasks()) {
            if (t->getState() == WorkflowTask::COMPLETED) {
                wasted_node_seconds -= t->getFlops() / this->core_speed;
                used_seconds += t->getFlops() / this->core_speed;
            }
        }

        this->simulator->wasted_node_seconds += wasted_node_seconds;

        if (not unprocessed) { // Nothing to do
            WRENCH_INFO("This placeholder job has no unprocessed tasks. great.");
            return;
        }

        // TODO - this will break for all algorithms except OJ!
//        std::cout << "Removing level of expired job from ongoing_levels" << std::endl;

        this->ongoing_levels.erase((int) ongoing_level->level_number);

        this->simulator->num_pilot_job_expirations_with_remaining_tasks_to_do++;

        return;

        // TODO - Let grouping heuristic process expired tasks in level...


        ongoing_level->running_placeholder_jobs.erase(placeholder_job);

        WRENCH_INFO("This placeholder job has unprocessed tasks... resubmit it as a restart");
        // Create a new Clustered Job
        ClusteredJob *cj = new ClusteredJob();
        for (auto t : placeholder_job->clustered_job->getTasks()) {
            if (t->getState() != WorkflowTask::COMPLETED) {
                cj->addTask(t);
            }
        }

        if (not placeholder_job->clustered_job->isNumNodesBasedOnQueueWaitTimePrediction()) {
            // Don't be stupid, don't ask for more nodes than tasks
            cj->setNumNodes(std::min(placeholder_job->clustered_job->getNumNodes(), cj->getNumTasks()));
        } else {
            ulong num_nodes = cj->computeBestNumNodesBasedOnQueueWaitTimePredictions(cj->getNumTasks(),
                                                                                     this->core_speed,
                                                                                     this->batch_service);
            cj->setNumNodes(num_nodes, true);
        }

        double makespan = cj->estimateMakespan(this->core_speed) * EXECUTION_TIME_FUDGE_FACTOR;

        // Create the pilot job
        auto pj = this->job_manager->createPilotJob();

        PlaceHolderJob *replacement_placeholder_job =
                new PlaceHolderJob(pj, cj,
                                   ongoing_level->level_number, ongoing_level->level_number);

        // Resubmit it!
        ongoing_level->pending_placeholder_jobs.insert(replacement_placeholder_job);
        // submit the corresponding pilot job
        std::map<std::string, std::string> service_specific_args;
        service_specific_args["-N"] = std::to_string(cj->getNumNodes());
        service_specific_args["-c"] = std::to_string(1);
        service_specific_args["-t"] = std::to_string(1 + ((ulong) (makespan)) / 60);
        this->job_manager->submitJob(replacement_placeholder_job->pilot_job, this->batch_service,
                                     service_specific_args);WRENCH_INFO(
                "Submitted a Pilot Job (%s hosts, %s min) for workflow level %lu (%s)",
                service_specific_args["-N"].c_str(),
                service_specific_args["-t"].c_str(),
                ongoing_level->level_number,
                replacement_placeholder_job->pilot_job->getName().c_str());WRENCH_INFO(
                "This pilot job has these tasks:");
        for (auto t : replacement_placeholder_job->clustered_job->getTasks()) { WRENCH_INFO("     - %s (flops: %lf)",
                                                                                            t->getID().c_str(),
                                                                                            t->getFlops());
        }

    }


    void LevelByLevelWMS::processEventStandardJobCompletion(std::shared_ptr<StandardJobCompletedEvent> e) {

        WorkflowTask *completed_task = e->standard_job->tasks[0]; // only one task per job

        WRENCH_INFO("Got a standard job completion for task %s", completed_task->getID().c_str());

        this->simulator->used_node_seconds += completed_task->getFlops() / this->core_speed;

        // Find the placeholder job this task belongs to
        PlaceHolderJob *placeholder_job = nullptr;
        OngoingLevel *ongoing_level = nullptr;
        for (auto ol : this->ongoing_levels) {
            for (auto ph : ol.second->running_placeholder_jobs) {
                for (auto task : ph->clustered_job->getTasks()) {
                    if (task == completed_task) {
                        ongoing_level = ol.second;
                        placeholder_job = ph;
                        break;
                    }
                }
            }
        }

        if (placeholder_job == nullptr) {
            throw std::runtime_error("Got a task completion, but couldn't find a placeholder for the task, "
                                     "and we're not in individual mode");
        }

        // Terminate the pilot job in case all its tasks are done
        bool all_tasks_done = true;
        for (auto task : placeholder_job->clustered_job->getTasks()) {
            if (task->getState() != WorkflowTask::COMPLETED) {
                all_tasks_done = false;
                break;
            }
        }
        if (all_tasks_done) {

            // Update the wasted no seconds metric
//        double wasted_node_seconds = placeholder_job->pilot_job->getNumHosts() * placeholder_job->pilot_job->getDuration();
            ulong num_used_nodes;
            sscanf(placeholder_job->pilot_job->getServiceSpecificArguments()["-N"].c_str(), "%lu", &num_used_nodes);
            ulong num_used_minutes;
            sscanf(placeholder_job->pilot_job->getServiceSpecificArguments()["-t"].c_str(), "%lu", &num_used_minutes);
            double wasted_node_seconds = 60.0 * num_used_minutes * num_used_nodes;

            for (auto t : placeholder_job->clustered_job->getTasks()) {
                if (t->getState() == WorkflowTask::COMPLETED) {
                    wasted_node_seconds -= t->getFlops() / this->core_speed;
                }
            }
            this->simulator->wasted_node_seconds += wasted_node_seconds;


            WRENCH_INFO("All tasks are completed in this placeholder job, so I am terminating it (%s)",
                        placeholder_job->pilot_job->getName().c_str());
            try {
                this->job_manager->terminateJob(placeholder_job->pilot_job);
            } catch (WorkflowExecutionException &e) {
                // ignore
            }
            ongoing_level->running_placeholder_jobs.erase(placeholder_job);
            ongoing_level->completed_placeholder_jobs.insert(placeholder_job);
            // TODO - this isn't removing from this->ongoing_levels???
//            std::cout << "Finished all jobs in a placeholder!" << std::endl;
        }

        /**
        // Start all newly ready tasks that depended on the completed task, IN ANY PLACEHOLDER
        // This shouldn't happen in individual mode, but can't hurt
        WRENCH_INFO("Seeing if other tasks (which are now ready) can be submitted...");
        std::vector<WorkflowTask *> children = this->getWorkflow()->getTaskChildren(completed_task);
        for (auto ol : this->ongoing_levels) {
            for (auto ph : ol.second->running_placeholder_jobs) {

                for (auto task : ph->clustered_job->getTasks()) {
                    if ((std::find(children.begin(), children.end(), task) != children.end()) and
                        (task->getState() == WorkflowTask::READY)) {
                        StandardJob *standard_job = this->job_manager->createStandardJob(task, {});WRENCH_INFO(
                                "Submitting task %s  as part of placeholder job %ld-%ld",
                                task->getID().c_str(), ph->start_level, ph->end_level);
                        this->job_manager->submitJob(standard_job, ph->pilot_job->getComputeService());
                    }
                }
            }
        }
        */

        // Remove the ongoing level if it's finished
        if (ongoing_level->pending_placeholder_jobs.empty() and
            ongoing_level->running_placeholder_jobs.empty()) {

            WRENCH_INFO("Level %ld is finished!", ongoing_level->level_number);

//            printf("Level %ld is finished!\n", ongoing_level->level_number);
            this->last_level_completed = ongoing_level->level_number;

            this->ongoing_levels.erase(ongoing_level->level_number);
        }

    }

    void LevelByLevelWMS::processEventStandardJobFailure(std::shared_ptr<StandardJobFailedEvent> e) {
        WRENCH_INFO(
                "Got a standard job failure event for task %s -- IGNORING THIS (the pilot job expiration event will handle these issues)",
                e->standard_job->tasks[0]->getID().c_str());
    }

};