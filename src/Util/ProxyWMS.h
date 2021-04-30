//
// Created by evan on 8/26/19.
//

#ifndef TASK_CLUSTERING_BATCH_SIMULATOR_PROXYWMS_H
#define TASK_CLUSTERING_BATCH_SIMULATOR_PROXYWMS_H

//#define EXECUTION_TIME_FUDGE_FACTOR 2.1

namespace wrench {

    class PlaceHolderJob;

    class ProxyWMS {

    public:

        ProxyWMS(Workflow *workflow, std::shared_ptr<JobManager> job_manager,
                 std::shared_ptr<BatchComputeService> batch_service);

        PlaceHolderJob *createAndSubmitPlaceholderJob(double requested_execution_time,
                                                      unsigned long requested_parallelism,
                                                      unsigned long start_level,
                                                      unsigned long end_level);

        void submitAllOneJobPerTask(double core_speed, unsigned long * num_jobs_in_system, unsigned long max_num_jobs);

        static double findMaxDuration(std::set<PlaceHolderJob *> jobs);

        double estimateWaitTime(long parallelism, double makespan, double simulation_date, int *sequence);

        unsigned long getStartLevel(std::set<PlaceHolderJob *> running_placeholder_jobs);

    private:

        Workflow *workflow;

        std::shared_ptr<JobManager> job_manager;

        std::shared_ptr<BatchComputeService> batch_service;

    };
}


#endif //TASK_CLUSTERING_BATCH_SIMULATOR_PROXYWMS_H
