#include "str.h"
#include "string.h"

result results; 

int h1_hash(uint32_t n){                //////////////////////////////////
    return (n & (1 << h1)-1);           //                              //
}                                       //      an h1=4 kai h2=6        //
                                        //                              //
int h2_hash(uint32_t n){                //  ... 1001 1101 1111 0011     //
    n = n >> h1;                        //             ^     ^ ^  ^     //
    return (n & (1 << h2)-1);           //             < h2  > <h1>     //
}                                       //////////////////////////////////


void create_histogram(histogram* histogram, relation *rel){
	int i, h_val;
	int hist_rows = (int)pow(2,h1);
	uint32_t n;

	//initialize histogram
	for (i = 0; i < hist_rows; i++){
		histogram[i].value = i;
		histogram[i].sum = 0;
	}

	for(i = 0; i < rel -> num_tuples; i++){
		h_val = h1_hash((uint32_t)rel->tuples[i].payload);
		histogram[h_val].value = h_val;
		histogram[h_val].sum++;
	}
}

void create_psum(histogram* psum,histogram * histogram, relation *rel){
	int i, h_val;
	int hist_rows = (int)pow(2,h1);
	uint32_t n;

	//initialize psum
	for (i = 0; i < hist_rows; i++){
		psum[i].sum = 0;
	}

	psum[0].value = histogram[0].value; //first element
	for (i = 1; i < hist_rows; i++){
		psum[i].value = histogram[i].value;
		psum[i].sum = psum[i-1].sum + histogram[i-1].sum;
	}

}

void reorder(relation * ord_rel, relation *rel, histogram* hist, histogram* psum){
	int i, j, val, n, pos;
	int tuples = rel->num_tuples;
	ord_rel -> num_tuples = tuples;

	int hist_rows = (int)pow(2,h1);

	ord_rel -> tuples = (struct tuple*)malloc(tuples*sizeof(struct tuple));
	if(ord_rel->tuples == NULL){
		printf("Error! memory not allocated\n");
		exit(0);
	}

    int* next = (int*)calloc(hist_rows, sizeof(int));
 
	for(i = 0; i < tuples; i++){
		val = h1_hash(rel -> tuples[i].payload); 

		pos = psum[val].sum;
		ord_rel->tuples[pos+next[val]].key = rel->tuples[i].key;
		ord_rel->tuples[pos+next[val]].payload = rel -> tuples[i].payload; 
        next[val]++;
	}
	free(next);

}

/*************************************************************/
/*******************    Radix Hash Join     *****************/
/***********************************************************/

result* RadixHashJoin(relation *relR, relation* relS){

/////////////////////////////
//  PART 1 - tmhmatopoihsh //
/////////////////////////////	
	if(relR -> num_tuples == 0 || relS->num_tuples == 0){
		result_init(&results);
		return &results;
	}

	int hist_rows = (int)pow(2,h1);

	/*	S 	*/

	histogram histS[hist_rows];
	create_histogram(histS, relS);

	histogram psumS[hist_rows];
	create_psum(psumS, histS, relS);

	struct relation orderedS;
	reorder(&orderedS, relS, histS, psumS);



	histogram histR[hist_rows];
	create_histogram(histR, relR);

	histogram psumR[hist_rows];
	create_psum(psumR, histR, relR);

	struct relation orderedR;
	reorder(&orderedR, relR, histR, psumR);


	int num_h2 = (int)pow(2,h2);

	struct relation temp_small_bucket;

	struct bucket temp_bucket;

	struct chain temp_chain;



	int i;
	int n=0;
	int h_val=0;
	int32_t* temp_key;

	int32_t* temp_k;
	int l=0;

	int b=0 ;  // trexon bucket ths h1

	int chain_count=0;

	temp_bucket.key = (int32_t*)malloc(num_h2*sizeof(int32_t));

	result_init(&results);	//////////////////////////////////////////////////////

	int counter=0;

	for( b=0 ; b<hist_rows ; b++){
	//////////////////////////////////
	//      Arxikopoihsh            //
	//      -temp_small_bucket      //
	//      -temp_bucket            //
	//      -temp_chain             //
	//////////////////////////////////

        memset(temp_bucket.key, -1, num_h2*sizeof(int32_t));

		if( histS[b].sum <= histR[b].sum ){
			// arxikopoihsh temp_small_bucket S
			temp_small_bucket.num_tuples = histS[b].sum;
			temp_small_bucket.tuples = (struct tuple*)malloc( (histS[b].sum)*sizeof(struct tuple));
			for(int j=0 ; j<temp_small_bucket.num_tuples ; j++){
				temp_small_bucket.tuples[j].key = orderedS.tuples[j+psumS[b].sum].key;
				temp_small_bucket.tuples[j].payload = orderedS.tuples[j+psumS[b].sum].payload;
			}

			// arxikopoihsh temp_chain S
			temp_chain.num_tuples = histS[b].sum;
			temp_chain.key = (int32_t*)malloc( (temp_chain.num_tuples) *sizeof(int32_t));

			// for(int k=0; k<temp_chain.num_tuples ; k++){
			// 	temp_chain.key[k] = -1;
			// }
            memset(temp_chain.key, -1, temp_chain.num_tuples*sizeof(int32_t));

			for( chain_count = temp_small_bucket.num_tuples-1 ; chain_count>=0 ; chain_count--){  // gia to bucket b(h1) ths S
				h_val = h2_hash((uint32_t)temp_small_bucket.tuples[chain_count].payload);
				temp_key = & ( temp_bucket.key[h_val] );
				while( (*temp_key) != -1){
					temp_key = &( temp_chain.key[(*temp_key)] );
				}
				*temp_key = chain_count;
			}

            //////////////////     JOIN     ///////////////////

			for(int r=psumR[b].sum ; r<(psumR[b].sum)+(histR[b].sum) ; r++){
				h_val = h2_hash((uint32_t)orderedR.tuples[r].payload);
				temp_k = & ( temp_bucket.key[h_val] ) ;
				while( (*temp_k) != -1){
					if( temp_small_bucket.tuples[*temp_k].payload == orderedR.tuples[r].payload ){
						insert_result( orderedR.tuples[r].key, temp_small_bucket.tuples[(*temp_k)].key , &results);			
						counter++;
					}
					temp_k = &( temp_chain.key[(*temp_k)] );
				}
			}
			// free temp_small_bucket S
			free( temp_small_bucket.tuples );

			// free temp_chain S
			free( temp_chain.key );
		}
		else{   // S < R
			// arxikopoihsh temp_small_bucket R
			temp_small_bucket.num_tuples = histR[b].sum;
			temp_small_bucket.tuples = (struct tuple*)malloc( (histR[b].sum)*sizeof(struct tuple));
			for(int j=0 ; j<temp_small_bucket.num_tuples ; j++){
				temp_small_bucket.tuples[j].key = orderedR.tuples[j+psumR[b].sum].key;
				temp_small_bucket.tuples[j].payload = orderedR.tuples[j+psumR[b].sum].payload;
			}

			// arxikopoihsh temp_chain R
			temp_chain.num_tuples = histR[b].sum;
			temp_chain.key = (int32_t*)malloc( (temp_chain.num_tuples) *sizeof(int32_t));
			// for(int k=0; k<temp_chain.num_tuples ; k++){
			// 	temp_chain.key[k] = -1;
			// }
            memset(temp_chain.key, -1, temp_chain.num_tuples*sizeof(int32_t));


			for( chain_count = temp_small_bucket.num_tuples-1 ; chain_count>=0 ; chain_count--){  // gia to bucket b(h1) ths S
				h_val = h2_hash((uint32_t)temp_small_bucket.tuples[chain_count].payload);
				temp_key = & ( temp_bucket.key[h_val] );
				while( (*temp_key) != -1){
					temp_key = &( temp_chain.key[(*temp_key)] );
				}
				*temp_key = chain_count;
			}

			//////////////////     JOIN     ///////////////////


			for(int r=psumS[b].sum ; r<(psumS[b].sum)+(histS[b].sum) ; r++){
				h_val = h2_hash((uint32_t)orderedS.tuples[r].payload);
				temp_k = & ( temp_bucket.key[h_val] ) ;
				while( (*temp_k) != -1){
					if( temp_small_bucket.tuples[*temp_k].payload == orderedS.tuples[r].payload ){
						insert_result( temp_small_bucket.tuples[(*temp_k)].key, orderedS.tuples[r].key, &results);
						counter++;
					}
					temp_k = &( temp_chain.key[(*temp_k)] );
				}
			}
			// free temp_small_bucket R
			free( temp_small_bucket.tuples );

			// free temp_chain R
			free( temp_chain.key );
		}

	}  // b
	free(temp_bucket.key);
	free(orderedS.tuples);
	free(orderedR.tuples);


	return &results;

}


