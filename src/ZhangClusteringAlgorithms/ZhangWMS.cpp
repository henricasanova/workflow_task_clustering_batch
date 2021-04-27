/**
 * Copyright (c) 2019. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include "ZhangWMS.h"
#include <Util/WorkflowUtil.h>
#include "assert.h"
#include "Globals.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(zhang_wms, "Log category for Zhang WMS");

namespace wrench {

    static int sequence = 0;

    ZhangWMS::ZhangWMS(Simulator *simulator,
                       std::string hostname,
                       std::shared_ptr<BatchComputeService> batch_service,
                       unsigned long max_num_jobs,
                       bool pick_globally_best_split,
                       bool binary_search_for_leeway,
                       bool calculate_parallelism_based_on_predictions) :
            WMS(nullptr, nullptr, {batch_service}, {}, {}, nullptr, hostname, "zhang_wms") {

        this->simulator = simulator;
        this->max_num_jobs = max_num_jobs;
        this->pick_globally_best_split = pick_globally_best_split;
        this->binary_search_for_leeway = binary_search_for_leeway;
        this->calculate_parallelism_based_on_predictions = calculate_parallelism_based_on_predictions;
        this->batch_service = batch_service;
        this->pending_placeholder_job = nullptr;
        this->individual_mode = false;
        this->number_of_splits = 0;
    }

    int ZhangWMS::main() {

        TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_WHITE);

        this->checkDeferredStart();

        this->core_speed = this->batch_service->getCoreFlopRate().begin()->second;
        this->number_of_hosts = this->batch_service->getNumHosts();
        this->num_jobs_in_system = 0;
        this->job_manager = this->createJobManager();
        this->proxyWMS = new ProxyWMS(this->getWorkflow(), this->job_manager, this->batch_service);

        Globals::sim_json["individual_mode"] = false;
        Globals::sim_json["end_levels"] = std::vector<unsigned long> ();

        while (not this->getWorkflow()->isDone()) {
            applyGroupingHeuristic();
            this->waitForAndProcessNextEvent();
        }

        assert(this->num_jobs_in_system == 0);

        std::cout << "#SPLITS=" << this->number_of_splits << "\n";

        Globals::sim_json["num_splits"] = this->number_of_splits;

        return 0;
    }

    void ZhangWMS::applyGroupingHeuristic() {

        if (this->pending_placeholder_job) {
            return;
        }

        if (this->individual_mode) {
            return;
        }

        unsigned long start_level = this->proxyWMS->getStartLevel(this->running_placeholder_jobs);
        unsigned long end_level = this->getWorkflow()->getNumLevels() - 1;

        if (start_level > end_level) {
            return;
        }

        std::tuple<double, double, double, unsigned long, unsigned long> partial_dag = groupLevels(start_level,
                                                                                                   end_level);
        double partial_dag_wait_time = std::get<0>(partial_dag);
        double partial_dag_makespan = std::get<1>(partial_dag);
        double partial_dag_leeway = std::get<2>(partial_dag);
        unsigned long partial_dag_end_level = std::get<3>(partial_dag);
        unsigned long num_nodes = std::get<4>(partial_dag);

        assert(partial_dag_end_level <= end_level);

        std::cout << "*Picked end level: " << partial_dag_end_level << std::endl;
        std::cout << "Wait time: " << partial_dag_wait_time << std::endl;
        std::cout << "Makespan: " << partial_dag_makespan << std::endl;
        std::cout << "Leeway: " << partial_dag_leeway << std::endl;
        std::cout << "Parallelism: " << num_nodes << std::endl;

        if (partial_dag_end_level == end_level) {
            // TO PRESERVE THE SAME INDIVIDUAL MODE SWITCHING BEHAVIOR AS ORIGINAL ZHANG
            // calculate the runtime of entire DAG without predictions
            unsigned long max_parallelism = bestParallelism(start_level, end_level, false);
            double runtime_all = WorkflowUtil::estimateMakespan(
                    this->getWorkflow()->getTasksInTopLevelRange(start_level, end_level),
                    max_parallelism, this->core_speed);
            double wait_time_all = this->proxyWMS->estimateWaitTime(max_parallelism, runtime_all,
                                                                    this->simulation->getCurrentSimulatedDate(),
                                                                    &sequence);

            std::cout << "INDIVIDUAL MODE?  WAITTIME = " << wait_time_all << " AND RUNTIME_ALL = "
                      << runtime_all << "\n";

            if (wait_time_all > runtime_all * 2.0) {
                // submit remaining dag as 1 job per task
                this->individual_mode = true;
                Globals::sim_json["individual_mode"] = true;
                std::cout << "Switching to individual mode!" << std::endl;
            } else {
                std::cout << "NOT INDIVIDUAL\n";
                // submit remaining dag as 1 job
            }
        } else {
            this->number_of_splits++;
        }

        // Add the grouping even if we submit as ojpt
        Globals::sim_json["end_levels"].push_back(partial_dag_end_level);

        if (this->individual_mode) { WRENCH_INFO("Submitting tasks individually after switching to individual mode!");
            this->proxyWMS->submitAllOneJobPerTask(this->core_speed, &(this->num_jobs_in_system), max_num_jobs);
        } else {
            // TODO - should we check this?
            this->pending_placeholder_job = this->proxyWMS->createAndSubmitPlaceholderJob(
                    (partial_dag_makespan + partial_dag_leeway), num_nodes, start_level, partial_dag_end_level);
            this->num_jobs_in_system++;
        }
    }

    // return params: (waitt_time, runtime, leeway, end_level, num_nodes)
    std::tuple<double, double, double, unsigned long, unsigned long>
    ZhangWMS::groupLevels(unsigned long start_level, unsigned long end_level) {
        double best_wait_time = DBL_MAX;
        double best_runtime = 0;
        double leeway_for_best_runtime = DBL_MAX;
        unsigned long num_nodes_for_best_grouping = ULONG_MAX;
        unsigned long best_end_level = ULONG_MAX;

        double parent_runtime = this->proxyWMS->findMaxDuration(this->running_placeholder_jobs);
        std::cout << "\nParent job runtime: " << parent_runtime << std::endl;

        // Another unexplained zhang thing
        bool giant = true;

        // Start here
        unsigned long candidate_end_level = start_level;

        while (candidate_end_level <= end_level) {

            std::cout << "Candidate end level: " << candidate_end_level << std::endl;

            unsigned long num_nodes = bestParallelism(start_level, candidate_end_level, false);
            double runtime = WorkflowUtil::estimateMakespan(
                    this->getWorkflow()->getTasksInTopLevelRange(start_level, candidate_end_level),
                    num_nodes, this->core_speed);
            double wait_time = this->proxyWMS->estimateWaitTime(num_nodes, runtime,
                                                                this->simulation->getCurrentSimulatedDate(),
                                                                &sequence);
            double leeway = calculateLeeway(wait_time, runtime, num_nodes);

            assert(leeway >= 0);

            std::cout << "num nodes: " << num_nodes << std::endl;
            std::cout << "wait_time: " << wait_time << std::endl;
            std::cout << "runtime: " << runtime << std::endl;
            std::cout << "leeway: " << leeway << std::endl;
            std::cout << "ratio: " << wait_time / (runtime) << std::endl;

            // If we are on the last level w/ no best end level yet, we should check the ratio anyways
            // so we don't need to recalculate stuff if we need to return entire DAG
            bool onLastLevelStillNoBestGrouping =
                    (candidate_end_level == end_level) && (best_end_level == ULONG_MAX);

            // std::cout << "onLastLevelStillNoBestGrouping: " << onLastLevelStillNoBestGrouping << std::endl;

            // If we already saw a grouping that had at least a wait/run ratio < 1, we don't need to re-check for
            // following groupings...
            if (giant && (wait_time > runtime) && (not onLastLevelStillNoBestGrouping)) {
                candidate_end_level++;
                continue;
            }

            giant = false;

            // In the spirit of zhang, we check if it got "worse" instead of better
            bool ratio_got_worse = (wait_time / runtime) > (best_wait_time / best_runtime);


            if (ratio_got_worse && (not pick_globally_best_split)) {
                std::cout << "Ratio got worse - breaking from loop\n";
                break;
            }

            if (not ratio_got_worse) {
                std::cout << "Found a better split @ end level: " << candidate_end_level << std::endl;
                best_wait_time = wait_time;
                best_runtime = runtime;
                num_nodes_for_best_grouping = num_nodes;
                leeway_for_best_runtime = leeway;
                best_end_level = candidate_end_level;
            }

            candidate_end_level++;
        }

        assert(not giant);
        assert(best_end_level != ULONG_MAX);
        assert(num_nodes_for_best_grouping != ULONG_MAX);

        if (this->calculate_parallelism_based_on_predictions) {
            num_nodes_for_best_grouping = bestParallelism(start_level, best_end_level, true);
            best_runtime = WorkflowUtil::estimateMakespan(
                    this->getWorkflow()->getTasksInTopLevelRange(start_level, best_end_level),
                    num_nodes_for_best_grouping, this->core_speed);
            best_wait_time = this->proxyWMS->estimateWaitTime(num_nodes_for_best_grouping, best_runtime,
                                                              this->simulation->getCurrentSimulatedDate(),
                                                              &sequence);
            leeway_for_best_runtime = calculateLeeway(best_wait_time, best_runtime, num_nodes_for_best_grouping);
        }

        return std::make_tuple(best_wait_time, best_runtime, leeway_for_best_runtime, best_end_level,
                               num_nodes_for_best_grouping);
    }

    unsigned long ZhangWMS::bestParallelism(unsigned long start_level, unsigned long end_level, bool use_predictions) {
        unsigned long max_parallelism = 0;
        for (unsigned long i = start_level; i <= end_level; i++) {
            unsigned long num_tasks_in_level = this->getWorkflow()->getTasksInTopLevelRange(i, i).size();
            max_parallelism = std::max<unsigned long>(max_parallelism, num_tasks_in_level);
        }

        max_parallelism = std::min<unsigned long>(max_parallelism, this->number_of_hosts);

        if (not use_predictions) {
            return max_parallelism;
        }

        double parent_runtime = this->proxyWMS->findMaxDuration(this->running_placeholder_jobs);

        unsigned long best_parallelism = 0;
        double best_total_time = DBL_MAX;
        for (unsigned long i = 1; i < max_parallelism + 1; i++) {
            double makespan = WorkflowUtil::estimateMakespan(
                    this->getWorkflow()->getTasksInTopLevelRange(start_level, end_level),
                    i, this->core_speed);
            double wait_time = this->proxyWMS->estimateWaitTime(i, makespan,
                                                                this->simulation->getCurrentSimulatedDate(), &sequence);

            if (wait_time < parent_runtime) { // We don't care if your wait time is smaller than the parent runtime!
                wait_time = parent_runtime;
            }

            double total_time = makespan + wait_time;
            if (total_time < best_total_time) {
                best_total_time = total_time;
                best_parallelism = i;
            }
        }

        return best_parallelism;
    }

    double ZhangWMS::calculateLeeway(double wait_time, double runtime, unsigned long num_nodes) {
        double parent_runtime = this->proxyWMS->findMaxDuration(this->running_placeholder_jobs);
        double leeway = parent_runtime - wait_time;
        if (leeway <= 0) {
            return 0;
        }

        if (this->binary_search_for_leeway) {
            return calculateLeewayBinarySearch(runtime, num_nodes, parent_runtime, 0, leeway);
        } else {
            return calculateLeewayZhangHeuristic(wait_time, runtime, num_nodes, parent_runtime);
        }
    }

    double
    ZhangWMS::calculateLeewayBinarySearch(double runtime, unsigned long num_nodes, double parent_runtime, double lower,
                                          double upper) {
        // std::cout << "LOWER: " << lower << " UPPER: " << upper << " greater? " << (lower >= upper) << std::endl;
        assert(upper > 0 && lower >= 0);

        if ((upper - lower) < 600) {
            return upper;
        }

        double middle = floor((lower + upper) / 2.0);

        double new_wait_time = this->proxyWMS->estimateWaitTime(num_nodes, (runtime + middle),
                                                                this->simulation->getCurrentSimulatedDate(), &sequence);
        // std::cout << "NEW WAIT TIME: " << new_wait_time << std::endl;
        double new_leeway = parent_runtime - new_wait_time;

        // not enough overlap :(
        if (new_leeway >= 600) {
            return calculateLeewayBinarySearch(runtime, num_nodes, parent_runtime, middle + 1, upper);
        } else if (new_leeway < 0) {
            return calculateLeewayBinarySearch(runtime, num_nodes, parent_runtime, lower, middle - 1);
        }

        // middle added was enough to create full overlap + (some slack < 10 minutes)
        return middle;
    }

    double ZhangWMS::calculateLeewayZhangHeuristic(double wait_time, double runtime, unsigned long num_nodes,
                                                   double parent_runtime) {
        double leeway = parent_runtime - wait_time;
        assert(leeway > 0);
        while ((leeway > 600) and
               (this->proxyWMS->estimateWaitTime(num_nodes, runtime + leeway / 2.0,
                                                 this->simulation->getCurrentSimulatedDate(), &sequence) >
                parent_runtime)) {
            leeway /= 2.0;
        }

        return leeway;
    }

    void ZhangWMS::processEventPilotJobStart(std::shared_ptr<PilotJobStartedEvent> e) {
        // Update queue waiting time
        this->simulator->total_queue_wait_time +=
                this->simulation->getCurrentSimulatedDate() - e->pilot_job->getSubmitDate();

        WRENCH_INFO("Got a Pilot Job Start event: %s", e->pilot_job->getName().c_str());

        if (this->pending_placeholder_job == nullptr) {
            throw std::runtime_error("Fatal Error: couldn't find a placeholder job for a pilob job that just started");
        }

        WRENCH_INFO("Got a Pilot Job Start event e->pilot_job = %ld, this->pending->pilot_job = %ld (%s)",
                    (unsigned long) e->pilot_job.get(),
                    (unsigned long) this->pending_placeholder_job->pilot_job.get(),
                    this->pending_placeholder_job->pilot_job->getName().c_str());

        if (e->pilot_job != this->pending_placeholder_job->pilot_job) {
            // hmm
            WRENCH_INFO("Must be for a placeholder I already cancelled... nevermind");
            return;
        }

        PlaceHolderJob *placeholder_job = this->pending_placeholder_job;
        this->running_placeholder_jobs.insert(placeholder_job);
        this->pending_placeholder_job = nullptr;

        // std::string output_string = "";

        for (auto task : placeholder_job->tasks) {
            if (task->getState() == WorkflowTask::State::READY  and (placeholder_job->num_standard_job_submitted < placeholder_job->num_hosts)) {
                auto standard_job = this->job_manager->createStandardJob(task, {});

                // output_string += " " + task->getID();

                WRENCH_INFO("Submitting task %s as part of placeholder job %ld-%ld",
                            task->getID().c_str(), placeholder_job->start_level, placeholder_job->end_level);
                this->job_manager->submitJob(standard_job, placeholder_job->pilot_job->getComputeService());
                placeholder_job->num_standard_job_submitted++;
            }
        }

        this->applyGroupingHeuristic();
    }

    void ZhangWMS::processEventPilotJobExpiration(std::shared_ptr<PilotJobExpiredEvent> e) {
        this->num_jobs_in_system--;

        PlaceHolderJob *placeholder_job = nullptr;
        for (auto ph : this->running_placeholder_jobs) {
            if (ph->pilot_job == e->pilot_job) {
                placeholder_job = ph;
                break;
            }
        }

        if (placeholder_job == nullptr) {
            throw std::runtime_error("Got a pilot job expiration, but no matching placeholder job found");
        }

        this->running_placeholder_jobs.erase(placeholder_job);

        WRENCH_INFO("Got a pilot job expiration for a placeholder job that deals with levels %ld-%ld (%s)",
                    placeholder_job->start_level, placeholder_job->end_level,
                    placeholder_job->pilot_job->getName().c_str());

        // Check if there are unprocessed tasks
        bool unprocessed = false;
        for (auto t : placeholder_job->tasks) {
            if (t->getState() != WorkflowTask::COMPLETED) {
                unprocessed = true;
                break;
            }
        }

        unsigned long num_used_nodes;
        sscanf(e->pilot_job->getServiceSpecificArguments()["-N"].c_str(), "%lu", &num_used_nodes);

        unsigned long num_used_minutes;
        sscanf(e->pilot_job->getServiceSpecificArguments()["-t"].c_str(), "%lu", &num_used_minutes);

        double wasted_node_seconds = 60.0 * num_used_minutes * num_used_nodes;

        for (auto t : placeholder_job->tasks) {
            if (t->getState() == WorkflowTask::State::COMPLETED) {
                wasted_node_seconds -= t->getFlops() / this->core_speed;
            }
        }
        this->simulator->wasted_node_seconds += wasted_node_seconds;

        if (not unprocessed) {
            // Nothing to do
            WRENCH_INFO("This placeholder job has no unprocessed tasks. great.");
            return;
        }

        this->simulator->num_pilot_job_expirations_with_remaining_tasks_to_do++;

        WRENCH_INFO("This placeholder job has unprocessed tasks");

        if (this->pending_placeholder_job) {
            // Cancel pending pilot job if any
            WRENCH_INFO("Canceling pending placeholder job (placeholder=%ld,  pilot_job=%ld / %s",
                        (unsigned long) this->pending_placeholder_job,
                        (unsigned long) this->pending_placeholder_job->pilot_job.get(),
                        this->pending_placeholder_job->pilot_job->getName().c_str());
            this->job_manager->terminateJob(this->pending_placeholder_job->pilot_job);
            this->pending_placeholder_job = nullptr;
        }

        // Cancel running pilot jobs if none of their tasks has started

        std::set<PlaceHolderJob *> to_remove;
        for (auto ph : this->running_placeholder_jobs) {
            bool started = false;
            for (auto task : ph->tasks) {
                if (task->getState() != WorkflowTask::State::NOT_READY) {
                    started = true;
                }
            }
            if (not started) {
                // hmm
                WRENCH_INFO("Canceling running placeholder job that handled levels %ld-%ld because none"
                            "of its tasks has started (%s)", ph->start_level, ph->end_level,
                            ph->pilot_job->getName().c_str());
                try {
                    this->job_manager->terminateJob(ph->pilot_job);
                } catch (WorkflowExecutionException &e) {
                    // ignore (likely already dead!)
                }
                to_remove.insert(ph);
            }
        }

        for (auto ph : to_remove) {
            this->running_placeholder_jobs.erase(ph);
        }

        this->applyGroupingHeuristic();
    }

    void ZhangWMS::processEventStandardJobCompletion(std::shared_ptr<StandardJobCompletedEvent> e) {
        assert(this->num_jobs_in_system <= this->max_num_jobs);
         // std::cout << "GOT COMPLETION\n";

        // only one task per job
        WorkflowTask *completed_task = e->standard_job->tasks[0];

        WRENCH_INFO("Got a standard job completion for task %s", completed_task->getID().c_str());

        this->simulator->used_node_seconds += completed_task->getFlops() / this->core_speed;

        // Find the placeholder job this task belongs to
        PlaceHolderJob *placeholder_job = nullptr;
        for (auto ph : this->running_placeholder_jobs) {
            for (auto task : ph->tasks) {
                if (task == completed_task) {
                    placeholder_job = ph;
                    break;
                }
            }
        }

        if ((placeholder_job == nullptr) and (not this->individual_mode)) {
            throw std::runtime_error("Got a task completion, but couldn't find a placeholder for the task, "
                                     "and we're not in individual mode");
        }

        if ((placeholder_job ==  nullptr) and (this->individual_mode)) {
            this->num_jobs_in_system--;
        }

        if (placeholder_job != nullptr) {

            placeholder_job->num_standard_job_submitted--;

            // Terminate the pilot job in case all its tasks are done
            bool all_tasks_done = true;
            for (auto t : placeholder_job->tasks) {
                if (t->getState() != WorkflowTask::COMPLETED) {
                    all_tasks_done = false;
                    break;
                }
            }
            if (all_tasks_done) {
                // Update the wasted no seconds metric
                double first_task_start_time = DBL_MAX;
                for (auto const &t : placeholder_job->tasks) {
                    if (t->getStartDate() < first_task_start_time) {
                        first_task_start_time = t->getStartDate();
                    }
                }
                int num_requested_nodes = stoi(placeholder_job->pilot_job->getServiceSpecificArguments()["-N"]);
                double job_duration = this->simulation->getCurrentSimulatedDate() - first_task_start_time;
                double wasted_node_seconds = num_requested_nodes * job_duration;
                for (auto const &t : placeholder_job->tasks) {
                    wasted_node_seconds -= t->getFlops() / this->core_speed;
                }

                this->simulator->wasted_node_seconds += wasted_node_seconds;

                WRENCH_INFO("All tasks are completed in this placeholder job, so I am terminating it (%s)",
                            placeholder_job->pilot_job->getName().c_str());
                try {
                    // hmm
                    WRENCH_INFO("TERMINATING A PILOT JOB");
                    this->job_manager->terminateJob(placeholder_job->pilot_job);
                } catch (WorkflowExecutionException &e) {
                    // ignore
                }
                this->running_placeholder_jobs.erase(placeholder_job);
                this->num_jobs_in_system--;
            }
        }

        // Start all newly ready tasks that depended on the completed task, IN ANY PLACEHOLDER
        std::vector<WorkflowTask *> children = this->getWorkflow()->getTaskChildren(completed_task);
        for (auto ph : this->running_placeholder_jobs) {

            // Start Other tasks if possible, considering first tasks at the same level of completed_task
            for (auto task : ph->tasks) {
                if ((task->getState() == WorkflowTask::READY) and
                    (task->getTopLevel() == completed_task->getTopLevel()) and
                    (ph->num_standard_job_submitted < ph->num_hosts)) {

                    auto standard_job = this->job_manager->createStandardJob(task, {});
                    // hmm
                    WRENCH_INFO("Submitting task %s  as part of placeholder job %ld-%ld",
                                task->getID().c_str(), placeholder_job->start_level, placeholder_job->end_level);
                    this->job_manager->submitJob(standard_job, ph->pilot_job->getComputeService());

                    ph->num_standard_job_submitted++;

                }
            }

            // Start Any other READY TASKS if possible
            for (auto task : ph->tasks) {
                if ((task->getState() == WorkflowTask::READY) and (ph->num_standard_job_submitted < ph->num_hosts)) {

                    auto standard_job = this->job_manager->createStandardJob(task, {});
                    // hmm
                    WRENCH_INFO("Submitting task %s  as part of placeholder job %ld-%ld",
                                task->getID().c_str(), placeholder_job->start_level, placeholder_job->end_level);
                    this->job_manager->submitJob(standard_job, ph->pilot_job->getComputeService());

                    ph->num_standard_job_submitted++;

                }
            }
        }

        if (this->individual_mode) {
            WRENCH_INFO("Submitting tasks individually after job completion!");
            this->proxyWMS->submitAllOneJobPerTask(this->core_speed, &(this->num_jobs_in_system), this->max_num_jobs);
        }
    }

    void ZhangWMS::processEventStandardJobFailure(std::shared_ptr<StandardJobFailedEvent> e) {
        WRENCH_INFO("Got a standard job failure event for task %s -- IGNORING THIS",
                    e->standard_job->tasks[0]->getID().c_str());
        throw std::runtime_error("A job has failed, which shouldn't happen");
    }

}