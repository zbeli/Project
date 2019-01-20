#include "thread_pool.h"

#include "job_scheduler.h"

struct arguments args[THREADS];		// index of thread and the pointer of the pool

int thrd_pool_create(struct thrd_pool *temp){
	/*	init queue mutex 	*/
	if(pthread_mutex_init(&(temp->queue_mutex), NULL) !=0){
		printf("error init queue mutex\n");
		exit(-1);
	}
	temp->end=0;
	/*	init active mutex 	*/
	temp->active=0;
	if(pthread_mutex_init(&(temp->active_mutex), NULL) !=0){
		printf("error init active mutex\n");
		exit(-1);
	}

	/*	create job queue	*/
	job_queue_create(&(temp->queue));

	/*	create threads 	*/
	for (int i= 0; i < THREADS; ++i){
		sem_init(&(temp->semaphores[i]),0,1);


		temp->ret_vals[i] = -1;

		args[i].index = i;
		args[i].temp = temp;
		temp->ret_vals[i] = pthread_create(&(temp->thrds[i]), NULL, ready, (void*)(&args[i]));

		if(temp->ret_vals[i]){
			printf("could not create thread\n");
			exit(-1);
		}
	}
}

int thrd_pool_destroy(struct thrd_pool *temp){
	/*	wait all threads to terminate 	*/
	temp->end=1;
	for (int i= 0; i < THREADS; ++i){
		sem_destroy(&(temp->semaphores[i]));
		if(pthread_join(temp->thrds[i],NULL) !=0){
			printf("pthread_exit error\n");
			exit(-1);
		}
	}
	/*	destroy queue mutex 	*/
	if(pthread_mutex_destroy(&(temp->queue_mutex)) !=0){
		printf("error destroy queue mutex\n");
		exit(-1);
	}
	/*	destroy active mutex 	*/
	if(pthread_mutex_destroy(&(temp->active_mutex)) !=0){
		printf("error destroy active mutex\n");
		exit(-1);
	}
}

int thrd_pool_wait(struct thrd_pool *temp){
	while(1){
		if(temp->queue.length==0 && temp->active==0){
				pthread_mutex_lock(&(temp->queue_mutex));
				if(temp->active==0){
					pthread_mutex_unlock(&(temp->queue_mutex));
					break;
				}
				pthread_mutex_unlock(&(temp->queue_mutex));
		}
	}
}

int thrd_pool_sleep(struct thrd_pool *temp){
	for (int i = 0; i < THREADS; ++i)  // suspend all threads
	{
		sem_wait(&(temp->semaphores[i]));
	}
}

int thrd_pool_wake(struct thrd_pool *temp){
	for (int i = 0; i < THREADS; ++i)	// wake up all threads
	{
		sem_post(&(temp->semaphores[i]));
	}	
}