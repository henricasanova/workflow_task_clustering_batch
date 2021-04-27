/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */


#include <cfloat>
#include <xbt/base.h>
#include <xbt/log.h>
#include <wrench-dev.h>

#ifdef PRINT_RAM_MACOSX
#include<mach/mach.h>
#endif
#include <unordered_map>

#include "WorkflowUtil.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(workflow_util, "Log category for Workflow Util");


namespace wrench {

std::unordered_map<WorkflowTask*, std::vector<WorkflowTask*>> lineage;

#ifdef PRINT_RAM_MACOSX
    void WorkflowUtil::printRAM() {

      struct task_basic_info t_info;
      mach_msg_type_number_t t_info_count = TASK_BASIC_INFO_COUNT;
      
      if (KERN_SUCCESS != task_info(mach_task_self(),
                                  TASK_BASIC_INFO, (task_info_t)&t_info,
                                  &t_info_count)) {
          std::cerr << "RAM: ???\n";
      } else {
          std::cerr << "RAM: " << (double)t_info.resident_size / (1024 * 1024) << "MiB\n";
      // resident size is in t_info.resident_size;
      // virtual size is in t_info.virtual_size;
      }
    }
#else
    void WorkflowUtil::printRAM() {}
#endif

    /**
     * @brief Estimate a workflow's makespan
     * @param tasks: a set of tasks. For any task that has parents outside of this set, it is assumed that
     *         those parents are completed. For instance, a task with no parents in this set is assumed ready.
     *         If no task is given, then makespan will be zero.
     * @param num_hosts
     * @param core_speed
     * @return
     */
    double WorkflowUtil::estimateMakespan(std::vector<WorkflowTask *> tasks,
                                          unsigned long num_hosts, double core_speed) {

        if (tasks.size() == 0) {
            return 0.0;
        }

        if (lineage.empty()) {
            auto workflow = (*tasks.begin())->getWorkflow();
            for (auto task : workflow->getTasks()) {
                std::vector<WorkflowTask *> parents = task->getParents();
                lineage[task] = parents;
            }
        }

        if (num_hosts == 0) {
            throw std::runtime_error("Cannot estimate makespan with 0 hosts!");
        }

//        // Sort the tasks
//        std::sort(tasks.begin(), tasks.end(),
//                  [](const WorkflowTask * t1, const WorkflowTask * t2) -> bool {
//
//                      if (t1->getFlops() == t2->getFlops()) {
//                          return (t1->getID() > t2->getID());
//                      }
//                      return (t1->getFlops() > t2->getFlops());
//                  });


        // Initialize host idle dates
        double idle_date[num_hosts];
        memset(idle_date, 0, sizeof(double)*num_hosts);
//        for (unsigned int i=0; i < num_hosts; i++) {
//            idle_date[i] = 0.0;
//        }

        unsigned long num_tasks = tasks.size();

        // Create a list of "fake" tasks
        std::map<WorkflowTask *, double> fake_tasks;  // WorkflowTask, completion time

        // Insert all fake_tasks
        for (auto task : tasks) {
            fake_tasks[task] = -1.0;
        }

        unsigned long num_scheduled_tasks = 0;
        double current_time = 0.0;

        while (num_scheduled_tasks < num_tasks) {

//        WRENCH_INFO("ITERATION");
            bool scheduled_something = false;

            // Schedule ALL READY Tasks
            for (auto real_task : tasks) {
                // Already scheduled?
                if (fake_tasks[real_task] >= 0.0) {
                    continue;
                }

                //WRENCH_INFO("LOOKING AT TASK %s", real_task->getID().c_str());
                // Determine whether the task is schedulable
                bool schedulable = true;
                for (auto parent : lineage[real_task]) {
                    if ((fake_tasks[parent] > current_time) or
                        (fake_tasks[parent] < 0)) {
                        schedulable = false;
                        break;
                    }
                }

                if (not schedulable) {
//            WRENCH_INFO("NOT SCHEDULABLE");
                    continue;
                }

                double real_task_duration = real_task->getFlops() / core_speed;
                for (unsigned int j=0; j < num_hosts; j++) {
//            WRENCH_INFO("LOOKING AT HOST %d: %.2lf", j, idle_date[j]);
                    if (idle_date[j] <= current_time) {
//              WRENCH_INFO("SCHEDULING TASK on HOST %d", j);
                        fake_tasks[real_task] = current_time + real_task_duration;
                        idle_date[j] = current_time + real_task_duration;
//              WRENCH_INFO("SCHEDULED TASK %s on host %d from time %.2lf-%.2lf",
//                          real_task->getID().c_str(), j, current_time,
//                          current_time + real_task->getFlops() / core_speed);
                        scheduled_something = true;
                        num_scheduled_tasks++;
                        break;
                    } else {
//              WRENCH_INFO("THIS HOST DOESN'T WORK");
                    }
                }
            }

//        WRENCH_INFO("UPDATING CURRENT TIME");
            if (scheduled_something) {
                // Set current time to min idle time
                double min_idle_time = idle_date[0];
                for (unsigned int j = 1; j < num_hosts; j++) {
                    if (idle_date[j] < min_idle_time) {
                        min_idle_time = idle_date[j];
                    }
                }
                current_time = min_idle_time;
            } else {
                double second_min_idle_time = DBL_MAX;
                for (unsigned int j = 0; j < num_hosts; j++) {
                    if ((idle_date[j] > current_time) and (idle_date[j] < second_min_idle_time)) {
                        second_min_idle_time = idle_date[j];
                    }
                }
                current_time = second_min_idle_time;
            }
//        WRENCH_INFO("UPDATED CURRENT TIME TO %.2lf", current_time);
        }

        double makespan = 0;
        for (unsigned int i=0; i < num_hosts; i++) {
            makespan = std::max<double>(makespan, idle_date[i]);
        }

        WRENCH_INFO("DONE WITH ESTIMATE MAKESPAN");

        return makespan;

    }
};
