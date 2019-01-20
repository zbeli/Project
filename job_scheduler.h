#ifndef JOB_SCHEDULER_H
#define JOB_SCHEDULER_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <math.h>
#include <string.h>

#include "thread_pool.h"
#include "result.h"
#include "str.h"

void *ready(void *args);

/* initializes job queue, it s called in thrd_pool_create() */
int job_queue_create(struct job_queue *queue);
/* add a job in the queue , contains the critical section that guarantes that only on thread is pushing a job */
int job_queue_push(struct thrd_pool *pool, int (*func)(void**), void **args);
/* it is used from the threads in the thread pool, it must be called in a critical section guarded from queue mutex */
int job_queue_pull(struct job_queue *queue, struct job *temp_job);

/* parallel functions */
/*initializes hist and psum of the certain thread, and then creates the hist*/
int HistogramJob();
/* reads values from init relation (from start to end positon) ands puts it on reordered relation */
int PartitionJobs();
/* does the radix hash join for a certain bucket */
int JoinJobs();
/* computes the sum for a certain relation column*/
int Sum();
#endif /*job_scheduler.h*/