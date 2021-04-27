

#ifndef TASK_CLUSTERING_BATCH_SIMULATOR_SIMULATOR_H
#define TASK_CLUSTERING_BATCH_SIMULATOR_SIMULATOR_H

#include "wrench-dev.h"


#define EXECUTION_TIME_FUDGE_FACTOR 1.1

namespace wrench {

    class Simulator {

    public:
        static unsigned long sequence_number;

        unsigned long num_pilot_job_expirations_with_remaining_tasks_to_do = 0;
        double used_node_seconds = 0;
        double wasted_node_seconds = 0;
        double total_queue_wait_time = 0;


        int main(int argc, char **argv);

        void setupSimulationPlatform(wrench::Simulation *simulation, unsigned long num_compute_nodes);

        wrench::Workflow *createWorkflow(std::string workflow_spec);

        wrench::Workflow *createIndepWorkflow(std::vector<std::string> spec_tokens);

        wrench::Workflow *createLevelsWorkflow(std::vector<std::string> spec_tokens);

        wrench::Workflow *createWorkflowFromFile(std::string type, std::vector<std::string> spec_tokens);

        wrench::WMS *
        createWMS(std::string scheduler_spec, std::shared_ptr<wrench::BatchComputeService> batch_service, unsigned long max_num_jobs,
                  std::string algorithm_name);


    };

}

#endif //TASK_CLUSTERING_BATCH_SIMULATOR_SIMULATOR_H
