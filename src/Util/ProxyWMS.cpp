//
// Created by evan on 8/26/19.
//

#include <wms/WMS.h>
#include <Simulator.h>
#include <services/compute/batch/BatchComputeService.h>
#include "ProxyWMS.h"
#include "PlaceHolderJob.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(proxy_wms, "Log category for Proxy WMS");


namespace wrench {

    ProxyWMS::ProxyWMS(Workflow *workflow, std::shared_ptr<JobManager> job_manager,
                       std::shared_ptr<BatchComputeService> batch_service) {
        this->workflow = workflow;
        this->job_manager = job_manager;
        this->batch_service = batch_service;
    }

    PlaceHolderJob *ProxyWMS::createAndSubmitPlaceholderJob(double requested_execution_time,
                                                            unsigned long requested_parallelism,
                                                            unsigned long start_level,
                                                            unsigned long end_level) {

        requested_execution_time = requested_execution_time * EXECUTION_TIME_FUDGE_FACTOR;

        // Aggregate tasks
        std::vector<WorkflowTask *> tasks;
        for (unsigned long l = start_level; l <= end_level; l++) {
            std::vector<WorkflowTask *> tasks_in_level = this->workflow->getTasksInTopLevelRange(l, l);
            for (auto t : tasks_in_level) {
                if (t->getState() != WorkflowTask::COMPLETED) {
                    tasks.push_back(t);
                }
            }
        }

        // Submit the pilot job
        std::map<std::string, std::string> service_specific_args;
        service_specific_args["-N"] = std::to_string(requested_parallelism);
        service_specific_args["-c"] = "1";
        service_specific_args["-t"] = std::to_string(1 + ((unsigned long) requested_execution_time) / 60);

        PlaceHolderJob *pj = new PlaceHolderJob(this->job_manager->createPilotJob(), requested_parallelism, tasks, start_level, end_level);

        WRENCH_INFO("Submitting a Pilot Job (%ld hosts, %.2lf sec) for workflow levels %ld-%ld (%s)",
                    requested_parallelism, requested_execution_time, start_level, end_level,
                    pj->pilot_job->getName().c_str());

        WRENCH_INFO("This pilot job has these tasks:");
        for (auto t : pj->tasks) { WRENCH_INFO("     - %s", t->getID().c_str());
        }

        this->job_manager->submitJob(pj->pilot_job, this->batch_service, service_specific_args);

        return pj;
    }

    void ProxyWMS::submitAllOneJobPerTask(double core_speed, unsigned long * num_jobs_in_system, unsigned long max_num_jobs) {
        for (auto task : this->workflow->getTasks()) {
            if (task->getState() == WorkflowTask::State::READY and (*num_jobs_in_system < max_num_jobs)) {
                // std::cout << "Submitting as ojpt, num jobs in system before submission: " << (*num_jobs_in_system) << std::endl;
                auto standard_job = this->job_manager->createStandardJob(task, {});
                std::map<std::string, std::string> service_specific_args;
                // TODO - this cast is horrible, but should be okay?
                unsigned long requested_execution_time =
                        (unsigned long) (task->getFlops() / core_speed) * EXECUTION_TIME_FUDGE_FACTOR;
                service_specific_args["-N"] = "1";
                service_specific_args["-c"] = "1";
                service_specific_args["-t"] = std::to_string(1 + ((unsigned long) requested_execution_time) / 60);

                WRENCH_INFO("Submitting task %s individually!", task->getID().c_str());
                // std::cout << "Submitting task " << task->getID().c_str() << " individually!\n";
                this->job_manager->submitJob(standard_job, this->batch_service, service_specific_args);
                (*num_jobs_in_system)++;
            }
        }
    }

    double ProxyWMS::findMaxDuration(std::set<wrench::PlaceHolderJob *> jobs) {
        double max_duration = 0;
        for (PlaceHolderJob *pj : jobs) {
            max_duration = std::max<double>(max_duration, pj->getDuration());
        }

        return max_duration;
    }

    double ProxyWMS::estimateWaitTime(long parallelism, double makespan, double simulation_date, int *sequence) {
        std::set<std::tuple<std::string, unsigned long, unsigned long, double>> job_config;
        std::string config_key = "config_XXXX_" + std::to_string((*sequence)++); // need to make it unique for BATSCHED
        job_config.insert(std::make_tuple(config_key, (unsigned int) parallelism, 1, makespan));
        std::map<std::string, double> estimates = this->batch_service->getStartTimeEstimates(job_config);

        if (estimates[config_key] < 0) {
            throw std::runtime_error("Could not obtain start time estimate... aborting");
        }

        double wait_time_estimate = std::max<double>(0, estimates[config_key] - simulation_date);

        return wait_time_estimate;
    }

    unsigned long ProxyWMS::getStartLevel(std::set<PlaceHolderJob *> running_placeholder_jobs) {
        unsigned long start_level = 0;
        for (unsigned long i = 0; i < this->workflow->getNumLevels(); i++) {
            std::vector<WorkflowTask *> tasks_in_level = this->workflow->getTasksInTopLevelRange(i, i);
            bool all_completed = true;
            for (auto task : tasks_in_level) {
                if (task->getState() != WorkflowTask::State::COMPLETED) {
                    all_completed = false;
                }
            }
            if (all_completed) {
                start_level = i + 1;
            }
        }

        for (auto ph : running_placeholder_jobs) {
            start_level = 1 + std::max<unsigned long>(start_level, ph->end_level);
        }

        return start_level;
    }
}
