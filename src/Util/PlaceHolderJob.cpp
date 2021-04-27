//
// Created by evan on 8/28/19.
//

#include <Util/PlaceHolderJob.h>
#include <workflow/job/PilotJob.h>
#include <map>
#include <string>

namespace wrench {

    PlaceHolderJob::PlaceHolderJob(std::shared_ptr<PilotJob> pilot_job, unsigned long num_hosts, std::vector<WorkflowTask *> tasks,
                                   unsigned long start_level, unsigned long end_level) {

        // Sort the tasks
        std::sort(tasks.begin(), tasks.end(),
                  [](const WorkflowTask *t1, const WorkflowTask *t2) -> bool {

                      if (t1->getFlops() == t2->getFlops()) {
                          return (t1->getID() > t2->getID());
                      }
                      return (t1->getFlops() > t2->getFlops());
                  });

        this->pilot_job = pilot_job;
        this->num_hosts = num_hosts;
        this->tasks = tasks;
        this->start_level = start_level;
        this->end_level = end_level;
    }

    PlaceHolderJob::PlaceHolderJob(std::shared_ptr<PilotJob> pilot_job, ClusteredJob *clustered_job, unsigned long start_level,
                                   unsigned long end_level) {
        this->pilot_job = pilot_job;
        this->num_hosts = ULONG_MAX;
        this->clustered_job = clustered_job;
        this->start_level = start_level;
        this->end_level = end_level;
    }

    // Requested time of job
    double PlaceHolderJob::getDuration() {
        std::map<std::string, std::string> service_specific_args = this->pilot_job->getServiceSpecificArguments();
        double duration = std::stod(service_specific_args.find("-t")->second);
        // Duration stored in minutes, convert back to seconds
        return (duration - 1) * 60;
    }
}


