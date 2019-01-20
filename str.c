#include "str.h"

int h1_rows = (int)pow(2,h1);

int h1_hash(uint32_t n){                //////////////////////////////////
    return (n & (1 << h1)-1);           //                              //
}                                       //      an h1=4 kai h2=6        //
                                        //                              //
int h2_hash(uint32_t n){                //  ... 1001 1101 1111 0011     //
    n = n >> h1;                        //             ^     ^ ^  ^     //
    return (n & (1 << h2)-1);           //             < h2  > <h1>     //
}                                       //////////////////////////////////

int Hist_and_Psum(struct thrd_pool *pool, histogram hist[THREADS][h1_rows], histogram psum[THREADS][h1_rows], relation *rel){
	int start[THREADS],end[THREADS];
	int hist_rows = h1_rows;

	void *args[THREADS][5];
	for (int i = 0; i < THREADS; ++i){
		start[i] = i*(rel->num_tuples/THREADS);
		end[i] = (i+1)*(rel->num_tuples/THREADS)-1;
		if(i==THREADS-1){
			end[i] = rel->num_tuples-1;
		}
		// prepare args for job i/thread i
		args[i][0] = &(start[i]);
		args[i][1] = &(end[i]);
		args[i][2] = hist[i];
		args[i][3] = rel;
		args[i][4] = psum[i];

		job_queue_push(pool, &HistogramJob, args[i]);
	}

	thrd_pool_wait(pool);

	// calculate psum for each thread
	for (int j=0; j<hist_rows; j++){   // gia kathe bucket
		for (int i=0; i<THREADS; i++){ // gia kathe thread
			if( i==0 ){
				if( j==0 ){
					psum[i][j].sum = 0;
				}
				else{
					psum[i][j].sum = psum[THREADS-1][j-1].sum + hist[THREADS-1][j-1].sum ;
				}
			}
			else{
				psum[i][j].sum = psum[i-1][j].sum + hist[i-1][j].sum ;
			}
		}
	}
}

int ReOrdered(struct thrd_pool *pool, relation *ord_rel, relation *rel, struct histogram hist[THREADS][h1_rows], struct histogram psum[THREADS][h1_rows]){
	int tuples = rel->num_tuples;
	ord_rel -> num_tuples = tuples;
	ord_rel -> tuples = (struct tuple*)malloc(tuples*sizeof(struct tuple));
	if(ord_rel->tuples == NULL){
		printf("Error! memory not allocated\n");
		exit(0);
	}

	memset(ord_rel->tuples, -1, tuples*2*sizeof(int));

	int start[THREADS],end[THREADS];

	void *args[THREADS][6];
	for (int i = 0; i < THREADS; ++i){
		start[i] = i*(rel->num_tuples/THREADS);
		end[i] = (i+1)*(rel->num_tuples/THREADS)-1;
		if(i==THREADS-1){
			end[i] = rel->num_tuples-1;
		}
	// prepare args for job i/thread i
		args[i][0] = hist[i];
		args[i][1] = psum[i];
		args[i][2] = rel;
		args[i][3] = ord_rel;
		args[i][4] = &(start[i]);  
		args[i][5] = &(end[i]);

		job_queue_push(pool, &PartitionJobs, args[i]);
	}

	thrd_pool_wait(pool);

}

int Join(struct thrd_pool *pool, relation *new_rel_R, relation *new_rel_S, histogram psum_R[THREADS][h1_rows], histogram psum_S[THREADS][h1_rows], histogram h_R[THREADS][h1_rows], histogram h_S[THREADS][h1_rows]){
	int jobs = h1_rows;

	int start_R[jobs],end_R[jobs];
	int start_S[jobs],end_S[jobs];

	struct result res[jobs];
	int sumR, sumS;
	void *args[jobs][7];
	for (int i = 0; i < jobs; ++i){
		result_init(&res[i]);

		sumR = 0;	sumS = 0;
		for(int j=0 ; j<THREADS ; j++){
			sumR += h_R[j][i].sum;
			sumS += h_S[j][i].sum;
		}
		if(sumR==0 || sumS==0){continue;}	// result will be empty
		start_R[i] = psum_R[0][i].sum;
		start_S[i] = psum_S[0][i].sum;
		end_R[i] = sumR + start_R[i] -1;
		end_S[i] = sumS + start_S[i] -1;

		// prepare args for job i
		args[i][0] = &(start_R[i]);
		args[i][1] = &(end_R[i]);
		args[i][2] = new_rel_R;
		args[i][3] = &(start_S[i]);
		args[i][4] = &(end_S[i]);
		args[i][5] = new_rel_S;
		args[i][6] = &(res[i]);

		job_queue_push(pool, &JoinJobs, args[i]);
	}
	int j;
	struct node *current_node;
	int * temp;
	int row1, row2;

	free(results.start_list->buffer_start);
	free(results.start_list);

	thrd_pool_wait(pool);

	results.start_list = res[0].start_list;
	results.list_size = res[0].list_size;
	results.counter = res[0].counter;
	results.current_node = res[0].current_node;

	for (j = 1; j < jobs; ++j){
		if(res[j].counter==0){
			free_result(&(res[j]));
			continue;
		}
		results.list_size += res[j].list_size;
		results.counter += res[j].counter;
		results.current_node->next = res[j].start_list;
		results.current_node = res[j].current_node;
	}
// int i;
// 	for (int j = 0; j < jobs; ++j){
// 		if(res[j].counter == 0){
// 			free_result(&res[j]);
// 			continue;
// 		}

// 		current_node = res[j].start_list;
// 		temp = current_node->buffer_start;

// 		for(i = 0; i < res[j].list_size; i++){
// 			while((void*)temp < current_node->buffer){
// 				row1 = *temp;
// 				temp = temp + 1;
// 				row2 = *temp;

// 				insert_result(row1, row2, &results);
// 				temp = temp + 1;
// 			}
// 			if(current_node->next != NULL){
// 				current_node = current_node->next;
// 				temp = current_node->buffer_start;
// 			}
// 		}

// 		free_result(&res[j]);
// 	}
}

  /***********************************************************/
 /*******************    Radix Hash Join     ****************/
/***********************************************************/

result* RadixHashJoin(relation *relR, relation* relS, struct thrd_pool *pool){
	result_init(&results);
	if(relR->num_tuples==0 || relS->num_tuples==0){
		return &results;
	}

	int hist_rows = h1_rows;
	struct histogram hist_R[THREADS][hist_rows];			struct histogram hist_S[THREADS][hist_rows];
	struct histogram Psum_R[THREADS][hist_rows];			struct histogram Psum_S[THREADS][hist_rows];
	struct relation ReOrdered_R;							struct relation ReOrdered_S;

thrd_pool_wake(pool);
	Hist_and_Psum(pool, hist_R, Psum_R, relR);				Hist_and_Psum(pool, hist_S, Psum_S, relS);
	ReOrdered(pool, &ReOrdered_R, relR, hist_R, Psum_R);	ReOrdered(pool, &ReOrdered_S, relS, hist_S, Psum_S);
	Join(pool, &ReOrdered_R, &ReOrdered_S, Psum_R, Psum_S, hist_R, hist_S);
thrd_pool_sleep(pool);

	free(ReOrdered_R.tuples);
	free(ReOrdered_S.tuples);

	return &results;
}

