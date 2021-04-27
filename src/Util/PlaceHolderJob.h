

#ifndef YOUR_PROJECT_NAME_PLACEHOLDERJOB_H
#define YOUR_PROJECT_NAME_PLACEHOLDERJOB_H

#include <vector>
#include <wrench-dev.h>

namespace wrench {

    class WorkflowTask;

    class PilotJob;

    class ClusteredJob;

    class PlaceHolderJob {

    public:

        PlaceHolderJob(std::shared_ptr<PilotJob> pilot_job,
                       unsigned long num_hosts,
                       std::vector<WorkflowTask *> tasks,
                       unsigned long start_level,
                       unsigned long end_level);

        // For lbl
        PlaceHolderJob(std::shared_ptr<PilotJob> pilot_job,
                       ClusteredJob *clustered_job,
                       unsigned long start_level,
                       unsigned long end_level);

        std::shared_ptr<PilotJob> pilot_job;
        unsigned long num_hosts;
        std::vector<WorkflowTask *> tasks;
        unsigned long start_level;
        unsigned long end_level;

        unsigned long num_standard_job_submitted = 0;

        double getDuration();

        // For lbl
        ClusteredJob *clustered_job;
    };

};

#endif //YOUR_PROJECT_NAME_PLACEHOLDERJOB_H
