#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <math.h>
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>
#include <sys/types.h>

#define THREADS 1

struct job{
	int (*func)(void**); 	//function pointer
	void **args;	//arguments of job pushed in the queue

	int id;
	struct job *next;
};

struct job_queue{
	struct job *start;
	struct job *end;

	int length;
};

struct thrd_pool{
	int ret_vals[THREADS];
	pthread_t thrds[THREADS];

	sem_t semaphores[THREADS]; // used to suspend a thread when sleep is called
	// sem_t wait_sem;

	struct job_queue queue;
	pthread_mutex_t queue_mutex;	// quards pull and push in job queue

	int end;	// flag for terminating a thread (if end==1 terminate)

	int active;		//counts number of threads that execute o job
	pthread_mutex_t active_mutex; //guards active counter
};

struct arguments{
	int index;		// number of thread
	struct thrd_pool *temp;	// pointer of the pool
};

/* initializes mutexex, semaphores, creates job queue and creates the threads (number of threads is defined in  this file)*/
int thrd_pool_create(struct thrd_pool *temp);
/* gives end value 1 wait for the threads to terminate and destroys mutexes semaphores*/
int thrd_pool_destroy(struct thrd_pool *temp);
/* is used from main thread to wait all threads to finish all jobs*/
int thrd_pool_wait(struct thrd_pool *temp);


/* suspends all threads */
int thrd_pool_sleep(struct thrd_pool *temp);
/* wakes uo all threads */
int thrd_pool_wake(struct thrd_pool *temp);

#endif /*thread_pool.h*/