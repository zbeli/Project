#include "job_scheduler.h"

#include "parse.h"

void *ready(void *args){
	struct arguments* arg = args;
	int index = arg->index;
	struct job temp_job;
	struct thrd_pool *temp = arg->temp;
	while(1){	// breaks when there end flag equals 1 and there is no job left in queue
		sem_wait(&(temp->semaphores[index]));	// wait on sleep-wake semaphore 
		/*	cs for pulling a job from queue 	*/
		pthread_mutex_lock(&(temp->queue_mutex));
			// pull a job (if null no job to do)
			job_queue_pull(&(temp->queue),&temp_job);

			if(temp_job.func==NULL){	// no job to do 
				pthread_mutex_unlock(&(temp->queue_mutex)); /*	end of cs for pulling a job from queue 	*/
				sem_post(&(temp->semaphores[index]));	// post on sleep-wake semaphore				
				if(temp->end==1){  // check if destroy thread pool has been called
					break;
				}
				continue;  // back ti line 11
			}
			else{
				pthread_mutex_lock(&(temp->active_mutex));
					temp->active++;	// add an active thread
				pthread_mutex_unlock(&(temp->active_mutex));
			}
		pthread_mutex_unlock(&(temp->queue_mutex)); /*	end of cs for pulling a job from queue 	*/

		// there is a job to do
		(*temp_job.func)(temp_job.args);  // do the job
		pthread_mutex_lock(&(temp->active_mutex));
			temp->active--;	// sub an active thread once you have finished
		pthread_mutex_unlock(&(temp->active_mutex));

		sem_post(&(temp->semaphores[index]));	// post on sleep-wake semaphore	
	}
}

int job_queue_create(struct job_queue *queue){
	queue->start = NULL;
	queue->end = NULL;

	queue->length = 0;
}

int job_queue_push(struct thrd_pool *pool, int (*func)(void**), void **args){
	struct job *temp_job;
	temp_job = (struct job*)malloc(sizeof(struct job));
	temp_job->func = func;
	temp_job->args = args;
	temp_job->next = NULL;

	pthread_mutex_lock(&(pool->queue_mutex));	// only one one thread can push a job at a time (that thread is always the main thread) 
		pool->queue.length++;
		if(pool->queue.length==1){
			pool->queue.start = temp_job;
		}
		else{
			pool->queue.end->next = temp_job;
		}
		pool->queue.end = temp_job;
	pthread_mutex_unlock(&(pool->queue_mutex));
}

int job_queue_pull(struct job_queue *queue, struct job *temp_job){
	if(queue->length==0){
		temp_job->func = NULL;
		return 0;
	}
	temp_job->func = queue->start->func;
	temp_job->args = queue->start->args;
	temp_job->next = queue->start->next;

	queue->length--;
	free(queue->start);
	queue->start = NULL;

	queue->start = temp_job->next;

	return 0;
}

int HistogramJob(void **args){
	struct histogram *hist = (struct histogram*)args[2];
	struct relation *rel = (struct relation *)args[3];
	struct histogram *psum = (struct histogram*)args[4];

	int start = *(int*)args[0];
	int end = *(int*)args[1];

	int i, h_val;
	int hist_rows = (int)pow(2,h1);

	//initialize histogram and psum
	for (i = 0; i < hist_rows; i++){
		hist[i].value = i;
		hist[i].sum = 0;

		psum[i].value = i;
	}

	for(i = start; i <= end; i++){
		h_val = h1_hash(rel->tuples[i].payload);

		hist[h_val].value = h_val;
		hist[h_val].sum++;
	}
}

int PartitionJobs(void **args){
	int start = *(int*)args[4];
	int end = *(int*)args[5];
	struct histogram *hist = (struct histogram*)args[0];
	struct histogram *psum = (struct histogram*)args[1];
	struct relation *rel = (struct relation*)args[2];
	struct relation *ord_rel = (struct relation*)args[3];

	int h_val, i, j;
	int *next = (int*)calloc((int)pow(2,h1),sizeof(int));
	int pos;

	for (i = start; i <= end; ++i){
		h_val = h1_hash(rel -> tuples[i].payload);

		pos = psum[h_val].sum;

		ord_rel->tuples[pos+next[h_val]].key = rel->tuples[i].key;
		ord_rel->tuples[pos+next[h_val]].payload = rel->tuples[i].payload;
		next[h_val]++;
	}
	free(next);
}

int JoinJobs(void **args){
	int startR = *(int*)args[0];
	int endR = *(int*)args[1];
	struct relation *relR = (struct relation*)args[2];
	int startS = *(int*)args[3];
	int endS = *(int*)args[4];
	struct relation *relS = (struct relation*)args[5];
	struct result *res = (struct result*)args[6];

	int num_h2 = (int)pow(2,h2);
	struct relation temp_small_bucket;
	struct bucket temp_bucket;
	struct chain temp_chain;
	temp_bucket.key = (int32_t*)malloc(num_h2*sizeof(int32_t));

	// arxikopihsh temp_bucket
	memset(temp_bucket.key, -1, num_h2*sizeof(int32_t));


	if(endR-startR > endS-startS){
		// arxikopoihsh temp_small_bucket S
		temp_small_bucket.num_tuples = endS-startS + 1;		//megethos bucket
		temp_small_bucket.tuples = (struct tuple*)malloc( (endS-startS + 1)*sizeof(struct tuple));
		for(int j=0 ; j<temp_small_bucket.num_tuples ; j++){
			temp_small_bucket.tuples[j].key = relS->tuples[j+startS].key;
			temp_small_bucket.tuples[j].payload = relS->tuples[j+startS].payload;
		}

		// arxikopoihsh temp_chain S
		temp_chain.num_tuples = endS-startS + 1;	//megethos chain
		temp_chain.key = (int32_t*)malloc( (temp_chain.num_tuples) *sizeof(int32_t));
		memset(temp_chain.key, -1, (temp_chain.num_tuples)*sizeof(int32_t));

		int32_t* temp_key;
		int32_t* temp_k;
		int n, h_val;
		for(int chain_count = temp_small_bucket.num_tuples-1 ; chain_count>=0 ; chain_count--){  // gia to bucket b(h1) ths S
			n = temp_small_bucket.tuples[chain_count].payload;
			h_val = h2_hash(n);
			temp_key = & ( temp_bucket.key[h_val] );
			while( (*temp_key) != -1){
				temp_key = &( temp_chain.key[(*temp_key)] );
			}
			*temp_key = chain_count;
		}
		// int counter=0;
		for(int r=startR; r<=endR ; r++){
			n = relR->tuples[r].payload;
			h_val = h2_hash(n);
			temp_k = & ( temp_bucket.key[h_val] );
			while( (*temp_k) != -1){
				if( temp_small_bucket.tuples[*temp_k].payload == relR->tuples[r].payload ){
					insert_result( relR->tuples[r].key, temp_small_bucket.tuples[(*temp_k)].key , res);
					// counter++;	
				}
				temp_k = &( temp_chain.key[(*temp_k)] );
			}
		}
		// free temp_small_bucket S
		free( temp_small_bucket.tuples );
		// free temp_chain S
		free( temp_chain.key );
	}
	else{
		// arxikopoihsh temp_small_bucket R
		temp_small_bucket.num_tuples = endR-startR + 1;		//megethos bucket
		temp_small_bucket.tuples = (struct tuple*)malloc( (endR-startR + 1)*sizeof(struct tuple));
		for(int j=0 ; j<temp_small_bucket.num_tuples ; j++){
			temp_small_bucket.tuples[j].key = relR->tuples[j+startR].key;
			temp_small_bucket.tuples[j].payload = relR->tuples[j+startR].payload;
		}

		// arxikopoihsh temp_chain R
		temp_chain.num_tuples = endR-startR + 1;	//megethos chain
		temp_chain.key = (int32_t*)malloc( (temp_chain.num_tuples) *sizeof(int32_t));
		memset(temp_chain.key, -1, (temp_chain.num_tuples)*sizeof(int32_t));

		int32_t* temp_key;
		int32_t* temp_k;
		int n, h_val;
		for(int chain_count = temp_small_bucket.num_tuples-1 ; chain_count>=0 ; chain_count--){  // gia to bucket b(h1) ths S
			n = temp_small_bucket.tuples[chain_count].payload;
			h_val = h2_hash(n);
			temp_key = & ( temp_bucket.key[h_val] );
			while( (*temp_key) != -1){
				temp_key = &( temp_chain.key[(*temp_key)] );
			}
			*temp_key = chain_count;
		}
		// int counter=0;
		for(int r=startS; r<=endS ; r++){
			n = relS->tuples[r].payload;
			h_val = h2_hash(n);
			temp_k = & ( temp_bucket.key[h_val] );
			while( (*temp_k) != -1){
				if( temp_small_bucket.tuples[*temp_k].payload == relS->tuples[r].payload ){
					insert_result( temp_small_bucket.tuples[(*temp_k)].key, relS->tuples[r].key , res);
					// counter++;
				}
				temp_k = &( temp_chain.key[(*temp_k)] );
			}
		}
		// free temp_small_bucket R
		free( temp_small_bucket.tuples );
		// free temp_chain R
		free( temp_chain.key );
	}

	free(temp_bucket.key);
}

int Sum(void **args){
	int rel_key = *(int*)args[2];
	int column = *(int*)args[3];
	uint64_t *sum = (uint64_t*)args[4];

	struct result *result = (struct result*)args[0];
	struct file_info *info = (struct file_info*)args[1];

	int i;
    uint64_t *col_ptr; /*pointer to the column of the relation*/

    struct node *current_node;
	current_node = result -> start_list;
	int * temp = current_node -> buffer_start;


    col_ptr = info[rel_key].col_array[column];
    for(i = 0; i < result -> list_size; i++){
   		while((void*)temp < current_node -> buffer){

            *sum = *sum + *(col_ptr + *(temp));	
			temp = temp + 1;	
		}
		if(current_node -> next != NULL){
			current_node = current_node->next;
			temp = current_node->buffer_start;
		}   
    }

}