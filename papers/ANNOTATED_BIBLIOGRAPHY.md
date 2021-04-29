
---

### Batch Queue Resource Scheduling for Workflow Applications
by Zhang, Koelbel, Cooper

[PDF](./papers/zhang_batch.pdf)

#### Relevant quotes:

  - "Our work seeks to reduce the workflow turnaroud time by
intelligently using batch queues without relying on reservations"
  - "One advanced feature of Maui that we do use is the start time estimation functionality"
  - "Our work draws inspiration from [pilot jobs], but attempt to choose a more propitious size for the [pilot jobs]"
    
#### General approach:

  - Decide a group of next levels in the DAG to run as a single job,  based on
	  estimated wait time divided by runtime
  - Submit a corresponding pilot job
  - When the pilot job starts, start running the tasks and 
	  repeat the above once to submit the next pilot job
     - <b>So while a pilot job runs, another one travels through the queue</b>
 - If a running job expires with tasks sill running / not done,
	  then "put them back in the DAG" and cancel all pilot jobs
 - Repeat until everything is executed

The "meat" is in the first step: how to decide how many levels to aggregate
in a job. Their answer is a greedy algorithm that looks the the
configuration with the lowest waittime/runtime ratio, but stops at the
first local minimum it finds (trying 1 level, 2 levels, etc...)

One question: they never say how many hosts their ask for for a pilot job.
I believe they ask for the full parallelism. Of course it would be easy to 
consider all kings of options there as well. An easy extension of their
work?




#### More details

```
  - applyGroupingHeuristic(DAG)
  - Wait for an event
  - If event is "pilot job started":
    - schedule all the tasks for that pilot job
    - applyGroupingHeuristic(DAG) 
  - If event is "pilot job has expired":
    - cancel all pending pilot jobs 
    - "undo" tasks that were killed or not executed 
      and put them back into 
    - applyGroupingHeuristic(DAG)
  - If event is "a task has finished"
  	 - mark it as done, make children ready
  	 - if the pilot job that did that task has no more tasks 
  	   to do, then terminate that pilot job
```

The above has at most one pending pilot job in the queue. The authors talk about "interference" between pilot jobs...
The "smarts" of the above algorithm are in the applyGroupingHeuristic() function. 

```
applyGroupingHeuristic():
	- job_description = groupByLevel()
	- If the number of consecutive levels is the full DAG
	  height then:
	    -  If the expected runtime <  expected wait time / 2
	       then run the whole DAG as on batch job 
	    - Otherwise, use one batch job per task
	- Otherwise, submit a pilot job for running the consecutive
	  levels together
```

The above algorithm calls the the groupByLevel() function. In case that function says "run the whole DAG" then there are two options
using a simple heuristic to decide "as one job" or "as n jobs for n tasks".  

```
groupByLevel():
   - ratio[0] = + infty
   - For i=1 to DAG.height
     - Consider running the next i levels in one pilot job
   	    - (using as many host as parallelism? not specified)
   	  - ratio[i] = estimated wait time / execution time
   	  - if ratio[i] > ratio[i-1], break
   	  
   - return (i-1, estimated wait time, execution time, number_of_hosts)
```


groupByLevel() is the main heuristic really, which decides how to aggregate
by level.


#### Papers referencing it
  - We looked at them and only the Shahid, Raza, .. (see below) seems marginally related

---

### Using Imbalance Metrics to Optimize Task Clustering in Scientific Workflow Executions
by Chen et al.

[PDF](./papers/chen-fgcs-2014.pdf)

This is a pretty straightfoward paper: each task execution has some overhead, so we should cluster tasks to reduce that overhead

A few key assumptions:

  - Tasks are sequential (fine)
  - When aggregating tasks in job, the tasks run in sequence (not fine)
  - There is a clustering overhead (which we could simulate easily)
  - All clustering is off-line

Two clustering approaches:
 
  - "Vertical clustering" 
  - "Horizontal clustering" 


Vertical clustering is about data locality (we don't care)

Horizontal cluster is simple: when creating clusters of independent tasks
in a level, try to create clusters with as close as possible runtimes to
promote load-balancing.


This paper has a good Related Work section, which should serve as
inspiration.

--- 

### Level based batch scheduling strategy with idle slot reduction under DAG constraints for computational grid

by Shahid, Raza, Sajid

[PDF](./papers/1-s2.0-S0164121215001260-main.pdf)

Pretty far from what we do.  They have sets of DAGs.  They schedule sets of
DAG levels to reduce flow time (HEFT within each level, careful scheduling
of DAG levels to outperform seom global HEFT).  No notion of clustering to
hide submission overhead.  No notion of background load (machine is
dedicated).  Metric is over the set of workflows.  
Not even clear that we should reference it.  
The "communication overlap" is simply picking whether to run a job locally or
on a "remote" node. 


