/**
* Copyright (c) 2019. The WRENCH Team.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*/

#include "GlumeWMS.h"
#include <Util/WorkflowUtil.h>
#include "Globals.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(glume_wms, "Log category for Glume WMS");

namespace wrench {

    static int sequence = 0;

    GlumeWMS::GlumeWMS(Simulator *simulator, std::string hostname, double waste_bound,
                                         double beat_bound, std::shared_ptr<BatchComputeService> batch_service) :
            WMS(nullptr, nullptr, {batch_service}, {}, {}, nullptr, hostname, "clustering_wms") {
        this->simulator = simulator;
        this->waste_bound = waste_bound;
        this->beat_bound = beat_bound;
        this->batch_service = batch_service;
        this->pending_placeholder_job = nullptr;
        this->number_of_splits = 0;
    }

    int GlumeWMS::main() {

        TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_WHITE);

        this->checkDeferredStart();

        this->core_speed = (*(this->batch_service->getCoreFlopRate().begin())).second;
        this->number_of_hosts = this->batch_service->getNumHosts();
        this->job_manager = this->createJobManager();
        this->proxyWMS = new ProxyWMS(this->getWorkflow(), this->job_manager, this->batch_service);

        Globals::sim_json["end_levels"] = std::vector<unsigned long> ();

        while (not this->getWorkflow()->isDone()) {
            applyGroupingHeuristic();
            this->waitForAndProcessNextEvent();
        }

        WRENCH_INFO("#SPLITS= %lu", this->number_of_splits);

        Globals::sim_json["num_splits"] = this->number_of_splits;

        return 0;
    }

    void GlumeWMS::applyGroupingHeuristic() {

        WRENCH_INFO("APPLYING GROUPING HEURISTIC");

        if (this->pending_placeholder_job) {
            return;
        }

        // TODO - does running_placeholder_jobs need to be instantiated??
        unsigned long start_level = this->proxyWMS->getStartLevel(this->running_placeholder_jobs);
        unsigned long end_level = this->getWorkflow()->getNumLevels() - 1;

        if (start_level > end_level) {
            return;
        }

        unsigned long num_levels = end_level + 1;

        double parent_runtime = this->proxyWMS->findMaxDuration(this->running_placeholder_jobs);

        WRENCH_INFO("Parent job runtime: %lf", parent_runtime);

        // Use these to keep track of the "best" grouping
        std::tuple<double, double, unsigned long> entire_workflow = estimateJob(start_level, end_level, parent_runtime);
        double estimated_wait_time = std::get<0>(entire_workflow);
        double requested_execution_time = std::get<1>(entire_workflow);
        unsigned long requested_parallelism = std::get<2>(entire_workflow);

        WRENCH_INFO("entire dag num nodes: %lu", requested_parallelism);
        WRENCH_INFO("entire dag wait_time: %lf", estimated_wait_time);
        WRENCH_INFO("entire dag runtime: %lf", requested_execution_time);

        // Calculate leeway needed for entire dag vs. currently running parent
        double max_leeway_entire_dag = std::max<double>(0, (parent_runtime - estimated_wait_time));
        double best_leeway_entire_dag = calculateLeewayBinarySearch(requested_execution_time, requested_parallelism,
                                                                    parent_runtime, 0, max_leeway_entire_dag);

        // Adjust the run and wait times for leeway
        if (best_leeway_entire_dag > 0) {
            requested_execution_time += best_leeway_entire_dag;
            estimated_wait_time = this->proxyWMS->estimateWaitTime(requested_parallelism, requested_execution_time,
                                                        this->simulation->getCurrentSimulatedDate(), &sequence);
            WRENCH_INFO("entire dag recalculated wait_time: %lf",estimated_wait_time);
            WRENCH_INFO("entire dag recalculated runtime: %lf", requested_execution_time);
        }

        // TODO - should we overlap with parent?
        double best_makespan = estimated_wait_time + requested_execution_time;

        WRENCH_INFO("entire dag makespan: %lf", (estimated_wait_time + requested_execution_time));

        unsigned long partial_dag_end_level = end_level;

        // Find the best split
        for (unsigned long i = start_level; i < num_levels - 1; i++) {
            WRENCH_INFO("Candidate end level: %lu", i);

            std::tuple<double, double, unsigned long> start_to_split = estimateJob(start_level, i, parent_runtime);
            double wait_one = std::get<0>(start_to_split);
            double run_one = std::get<1>(start_to_split);
            unsigned long nodes_one = std::get<2>(start_to_split);

            WRENCH_INFO("1: num nodes: %lu", std::get<2>(start_to_split));
            WRENCH_INFO("1: wait_time: %lf", wait_one);
            WRENCH_INFO("1: runtime: %lf", run_one);

            // Calculate leeway needed for first group vs. currently running parent
            double max_leeway_one = std::max<double>(0, (parent_runtime - wait_one));
            double best_leeway_one = calculateLeewayBinarySearch(run_one, nodes_one, parent_runtime, 0, max_leeway_one);

            std::cout << "1: leeway needed: " << best_leeway_one << std::endl;

            if (best_leeway_one > (run_one * .1)) {
                std::cout << "Too much leeway needed - skipping group\n";
                continue;
            }

            // Adjust the run and wait times for leeway
            if (best_leeway_one > 0) {
                run_one += best_leeway_one;
                wait_one = this->proxyWMS->estimateWaitTime(nodes_one, run_one,
                                                            this->simulation->getCurrentSimulatedDate(), &sequence);
                std::cout << "1: recalculated wait_time: " << wait_one << std::endl;
                std::cout << "1: recalculated runtime: " << run_one << std::endl;
            }

            std::tuple<double, double, unsigned long> rest = estimateJob(i + 1, end_level, run_one);
            double wait_two = std::get<0>(rest);
            double run_two = std::get<1>(rest);
            unsigned long nodes_two = std::get<2>(rest);

            std::cout << "2: num nodes: " << nodes_two << std::endl;
            std::cout << "2: wait_time: " << wait_two << std::endl;
            std::cout << "2: runtime: " << run_two << std::endl;

            // Calculate leeway needed for second group vs. first group ^
            double max_leeway_two = std::max<double>(0, (run_one - wait_two));
            double best_leeway_two = calculateLeewayBinarySearch(run_two, nodes_two, run_one, 0, max_leeway_two);

            std::cout << "2: leeway needed: " << best_leeway_two << std::endl;

            if (best_leeway_two > (run_two * .1)) {
                std::cout << "Too much leeway needed for grouping\n";
                continue;
            }

            // Adjust the run and wait times for leeway
            if (best_leeway_two > 0) {
                run_two += best_leeway_two;
                wait_two = this->proxyWMS->estimateWaitTime(nodes_two, run_two,
                                                            this->simulation->getCurrentSimulatedDate(), &sequence);
                std::cout << "2: recalculated wait_time: " << wait_two << std::endl;
                std::cout << "2: recalculated runtime: " << run_two << std::endl;
            }

            double makespan = wait_one + std::max<double>(run_one, wait_two) + run_two;

            std::cout << "makespan: " << makespan << std::endl;

            // Make sure we only compare when one_job-0 is still the best grouping
            // Although, i'm not entirely convinced this is still right...
            double adjusted_time = makespan;
            if (partial_dag_end_level == end_level) {
                makespan += (makespan * beat_bound);
                std::cout << "makespan after beat bound adjustment: " << makespan << std::endl;
            }

            if (adjusted_time < best_makespan) {
                std::cout << "found a better split! @ end level = " << i << std::endl;
                partial_dag_end_level = i;
                best_makespan = makespan;
                requested_execution_time = run_one;
                requested_parallelism = nodes_one;
                estimated_wait_time = wait_one;
            }
        }

        if (partial_dag_end_level < end_level) {
            std::cout << "Splitting @ end level = " << partial_dag_end_level << std::endl;
            this->number_of_splits++;
        }

        WRENCH_INFO("GROUPING: %ld-%ld", start_level, partial_dag_end_level);

        assert(partial_dag_end_level <= end_level);

        std::cout << "\n*Picked end level: " << partial_dag_end_level << std::endl;
        std::cout << "Wait time: " << estimated_wait_time << std::endl;
        std::cout << "Runtime: " << requested_execution_time << std::endl;
        std::cout << "Parallelism: " << requested_parallelism << std::endl;

        Globals::sim_json["end_levels"].push_back(partial_dag_end_level);

        this->pending_placeholder_job = this->proxyWMS->createAndSubmitPlaceholderJob(
                requested_execution_time, requested_parallelism, start_level, partial_dag_end_level);
    }

    // Return params: (wait time, runtime, num_hosts)
    std::tuple<double, double, unsigned long>
    GlumeWMS::estimateJob(unsigned long start_level, unsigned long end_level, double delay) {

        double runtime = DBL_MAX;
        double wait_time = DBL_MAX;
        double best_makespan = DBL_MAX;
        unsigned long best_parallelism = 1;

        unsigned long max_parallelism = findMaxParallelism(start_level, end_level);

        for (unsigned long i = 1; i <= max_parallelism; i++) {
            std::tuple<double, double> waitAndRun = estimateWaitAndRunTimes(start_level, end_level, i);
            double curr_wait = std::get<0>(waitAndRun);
            double curr_runtime = std::get<1>(waitAndRun);

            if (isTooWasteful(curr_runtime, i, start_level, end_level)) {
                continue;
            }

            double curr_makespan = std::max<double>(delay, curr_wait) + curr_runtime;

            if (curr_makespan <= best_makespan) {
                runtime = curr_runtime;
                wait_time = curr_wait;
                best_makespan = curr_makespan;
                best_parallelism = i;
            }
        }

        assert(runtime != DBL_MAX && wait_time != DBL_MAX);

        return std::make_tuple(wait_time, runtime, best_parallelism);
    }

    unsigned long GlumeWMS::findMaxParallelism(unsigned long start_level, unsigned long end_level) {
        unsigned long max_parallelism = 0;
        for (unsigned long i = start_level; i <= end_level; i++) {
            unsigned long num_tasks_in_level = this->getWorkflow()->getTasksInTopLevelRange(i, i).size();
            max_parallelism = std::max<unsigned long>(max_parallelism, num_tasks_in_level);
        }

        return std::min<unsigned long>(max_parallelism, this->number_of_hosts);
    }

    std::tuple<double, double>
    GlumeWMS::estimateWaitAndRunTimes(unsigned long start_level, unsigned long end_level,
                                               unsigned long nodes) {
        double runtime = WorkflowUtil::estimateMakespan(
                this->getWorkflow()->getTasksInTopLevelRange(start_level, end_level),
                nodes, this->core_speed);
        double wait_time = this->proxyWMS->estimateWaitTime(nodes, runtime,
                                                            this->simulation->getCurrentSimulatedDate(), &sequence);
        return std::make_tuple(wait_time, runtime);
    }

    bool GlumeWMS::isTooWasteful(double runtime, unsigned long nodes, unsigned long start_level,
                                          unsigned long end_level) {
        double all_tasks_time = 0;
        for (unsigned long i = start_level; i <= end_level; i++) {
            all_tasks_time += WorkflowUtil::estimateMakespan(
                    this->getWorkflow()->getTasksInTopLevelRange(i, i),
                    1, this->core_speed);
        }

        double waste_ratio = (nodes * runtime - all_tasks_time) / (nodes * runtime);

        return waste_ratio > this->waste_bound;
    }

    double
    GlumeWMS::calculateLeewayBinarySearch(double runtime, unsigned long num_nodes, double parent_runtime,
                                                   double lower,
                                                   double upper) {
        // std::cout << "LOWER: " << lower << " UPPER: " << upper << " greater? " << (lower >= upper) << std::endl;
        assert(upper >= lower);

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

    void GlumeWMS::processEventPilotJobStart(std::shared_ptr<PilotJobStartedEvent> e) {
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

    void GlumeWMS::processEventPilotJobExpiration(std::shared_ptr<PilotJobExpiredEvent> e) {
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

    void GlumeWMS::processEventStandardJobCompletion(std::shared_ptr<StandardJobCompletedEvent> e) {
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
    }

    void GlumeWMS::processEventStandardJobFailure(std::shared_ptr<StandardJobFailedEvent> e) {
        WRENCH_INFO("Got a standard job failure event for task %s -- IGNORING THIS",
                    e->standard_job->tasks[0]->getID().c_str());
        throw std::runtime_error("A job has failed, which shouldn't happen");
    }


};
