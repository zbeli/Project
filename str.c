#include "str.h"
// #include "result.h"


uint32_t tobinary(uint32_t x){
	if(x==0) return 0;
	if(x==1) return 1;
	return (x%2 + 10*tobinary(x/2));
}

int h1_hash(uint32_t n){				//////////////////////////////////
	return (n & (1 << h1)-1);  // shift left	//				//
}							//      an h1=4 kai h2=6	//
							//				//
int h2_hash(uint32_t n){				//  ... 1001 1101 1111 0011	//
	n = n >> h1;					//	       ^     ^ ^  ^	//
	return (n & (1 << h2)-1);  // shift right	//             < h2  > <h1>	//
}							//////////////////////////////////

// struct relation* create_relation(){

// }

void create_histogram(histogram* histogram, relation *rel){
	int i, h_val;
	int hist_rows = (int)pow(2,h1);
	uint32_t n;
	
	//initialize histogram
	for (i = 0; i < hist_rows; i++){
		histogram[i].value = 0;
		histogram[i].sum = 0;
	}
	
	for(i = 0; i < rel -> num_tuples; i++){
		n = tobinary((uint32_t)rel->tuples[i].payload);
		h_val = h1_hash(n);
		histogram[h_val].value = '0'+ h_val;
		histogram[h_val].sum++;
	}

	printf(" Histogram %d tuples\n",rel->num_tuples);
	for (i = 0; i < hist_rows; i++){
		printf("%c| %d\n", histogram[i].value, histogram[i].sum);		
	}
}

void create_psum(histogram* psum,histogram * histogram, relation *rel){
	int i, h_val;
	int hist_rows = (int)pow(2,h1);
	uint32_t n;

	//initialize psum
	for (i = 0; i < hist_rows; i++){
		psum[i].value = 0;
		psum[i].sum = 0;
	}

	psum[0].value = histogram[0].value; //first element
	for (i = 1; i < hist_rows; i++){

		psum[i].value = histogram[i].value;
		psum[i].sum = psum[i-1].sum + histogram[i-1].sum;
	}

	//Print Psum
	printf(" PSUM\n");
	for (int i = 0; i < hist_rows; i++){
		printf("%c | %d\n", psum[i].value, psum[i].sum);
	}
}

void reorder(relation * ord_rel, relation *rel, histogram* hist, histogram* psum){
	int i,j,val,n,pos,next_bucket;
	int tuples = rel->num_tuples;
	ord_rel -> num_tuples = tuples;

	int hist_rows = (int)pow(2,h1);

	ord_rel -> tuples = (struct tuple*)malloc(tuples*sizeof(struct tuple));
	if(ord_rel->tuples == NULL){
		printf("Error! memory not allocated\n");
		exit(0);
	}

	for (i = 0; i < tuples; i++){
		ord_rel -> tuples[i].key = -1;
		ord_rel -> tuples[i].payload = -1;
	}


	for(i = 0; i < tuples; i++){
		n = tobinary(rel -> tuples[i].payload);
		val = h1_hash(n); 

		pos = psum[val].sum;
		next_bucket = pos + hist[val].sum;

		for(j = pos; j < next_bucket; j++){
			if(ord_rel->tuples[j].payload == -1){				
				// ord_rel->tuples[j].payload == n; 
				ord_rel->tuples[j].key = rel->tuples[i].key;
				ord_rel->tuples[j].payload = rel -> tuples[i].payload; 
				break;
			}
		}
	}
/*
	printf(" ORDERED \n");
	for (i = 0; i < tuples; i++){
		printf("%d | %c\n", ord_rel->tuples[i].key,ord_rel->tuples[i].payload);
	}
*/
}

/*******************	Radix Hash Join 	*****************/
// result* RadixHashJoin(relation *relR, relation* relS){
void RadixHashJoin(relation *relR, relation* relS){

//////////////
//  PART 1  //
//////////////	
	int hist_rows = (int)pow(2,h1);

	//  S  //
	printf("Relation S\n");

	histogram histS[hist_rows];
	create_histogram(histS, relS);

	histogram psumS[hist_rows];
	create_psum(psumS, histS, relS);

	struct relation orderedS[relS->num_tuples];
	reorder(orderedS, relS, histS, psumS);

printf(" S            ORDERED_S \n");
    for (int i = 0; i < relS->num_tuples; i++){
    	printf("%d | %c   <--->   %d | %c\n", relS->tuples[i].key, relS->tuples[i].payload, orderedS->tuples[i].key , orderedS->tuples[i].payload );
    }

printf("<------------------->\n");

	//  R  //
	printf("Relation R\n");

	histogram histR[hist_rows];
	create_histogram(histR, relR);

	histogram psumR[hist_rows];
	create_psum(psumR, histR, relR);

	struct relation orderedR[relR->num_tuples];
	reorder(orderedR, relR, histR, psumR);
printf(" R            ORDERED_R \n");
    for (int i = 0; i < relR->num_tuples; i++){
    	printf("%d | %c   <--->   %d | %c\n", relR->tuples[i].key, relR->tuples[i].payload, orderedR->tuples[i].key, orderedR->tuples[i].payload);
    }
	
printf("<------------------->\n");
//////////////
//  PART 2  //
//////////////
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

int b=0 ;  // trexon bucket ths h1 ths S

int chain_count=0;

temp_bucket.key = (int32_t*)malloc(num_h2*sizeof(int32_t));


////////////////////////
result result;
result_init(&result);
////////////////////////

int counter=0;

for( b=0 ; b<hist_rows ; b++){
//////////////////////////////////
//	Arxikopoihsh 		//
//	-temp_small_bucket	//
//	-temp_bucket		//
//	-temp_chain		//
//////////////////////////////////


printf("<------------------------------------------------------------------------------------>\n");


// arxikopihsh temp_bucket
for(int j=0 ; j<num_h2 ; j++){
	temp_bucket.key[j] = -1;
}

if( histS[b].sum <= histR[b].sum ){
	// arxikopoihsh temp_small_bucket S
	temp_small_bucket.num_tuples = histS[b].sum;
	temp_small_bucket.tuples = (struct tuple*)malloc( (histS[b].sum)*sizeof(struct tuple));
	for(int j=0 ; j<temp_small_bucket.num_tuples ; j++){
		temp_small_bucket.tuples[j].key = orderedS->tuples[j+psumS[b].sum].key;
		temp_small_bucket.tuples[j].payload = orderedS->tuples[j+psumS[b].sum].payload;
		
	}

	// arxikopoihsh temp_chain S
	temp_chain.num_tuples = histS[b].sum;
	temp_chain.key = (int32_t*)malloc( (temp_chain.num_tuples) *sizeof(int32_t));
	for(int k=0; k<temp_chain.num_tuples ; k++){
		temp_chain.key[k] = -1;
	}

	// ONLY FOR PRINTING
	printf("S temp small bucket %d  --  [S(%d)<=R(%d)]\n", b, histS[b].sum, histR[b].sum);
	for(int j=0 ; j<temp_small_bucket.num_tuples ; j++){
		printf("(%d,%c)",temp_small_bucket.tuples[j].key, temp_small_bucket.tuples[j].payload );
	}
	printf("\n");

	for( chain_count = temp_small_bucket.num_tuples-1 ; chain_count>=0 ; chain_count--){  // gia to bucket b(h1) ths S
		n = tobinary((uint32_t)temp_small_bucket.tuples[chain_count].payload);
		h_val = h2_hash(n);

		temp_key = & ( temp_bucket.key[h_val] );

		while( (*temp_key) != -1){
			temp_key = &( temp_chain.key[(*temp_key)] );
		}
		*temp_key = chain_count;
	}

	// ONLY FOR PRINTING
	for(int k=0 ; k<num_h2 ; k++){  // MONO GIA PRINT TOU BUCKET
		temp_k = & ( temp_bucket.key[k] ) ;
		while( (*temp_k) != -1){
			l++;
			temp_k = &( temp_chain.key[(*temp_k)] );
		}
		printf("\t H2 S Bucket %d [%d]\n", k, l);

		l=0;
		temp_k = & ( temp_bucket.key[k] ) ;
		while( (*temp_k) != -1){
			l++;
			printf("(%d-%c)", temp_small_bucket.tuples[(*temp_k)].key, temp_small_bucket.tuples[(*temp_k)].payload);
			temp_k = &( temp_chain.key[(*temp_k)] );
		}
		l=0;
		printf("\n");
	}


	// ONLY FOR PRINTING
	printf("\n");
	printf("H1 R Bucket %d\n", b);
	for(int r=psumR[b].sum ; r<(psumR[b].sum)+(histR[b].sum) ; r++){
		n = tobinary((uint32_t)orderedR->tuples[r].payload);
		h_val = h2_hash(n);
		printf("(%d-%c)%d / ", orderedR->tuples[r].key, orderedR->tuples[r].payload,h_val);
	}
	printf("\n");

//////////////////     JOIN     ///////////////////

	printf("\n");
	printf("JOIN %d\n", b);
	for(int r=psumR[b].sum ; r<(psumR[b].sum)+(histR[b].sum) ; r++){
		n = tobinary((uint32_t)orderedR->tuples[r].payload);
		h_val = h2_hash(n);
		temp_k = & ( temp_bucket.key[h_val] ) ;
		while( (*temp_k) != -1){
			if( temp_small_bucket.tuples[*temp_k].payload == orderedR->tuples[r].payload ){
			//if( tobinary((uint32_t) temp_small_bucket.tuples[*temp_k].payload ) == tobinary((uint32_t) orderedR->tuples[r].payload) ){
				printf("%d) R(%d %c)-S(%d %c) \n", h_val, orderedR->tuples[r].key , orderedR->tuples[r].payload , temp_small_bucket.tuples[(*temp_k)].key, temp_small_bucket.tuples[(*temp_k)].payload);
				insert_result( orderedR->tuples[r].key, temp_small_bucket.tuples[(*temp_k)].key , &result);
				counter++;
			}
			temp_k = &( temp_chain.key[(*temp_k)] );
		}
	}
	printf("\n");


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
		temp_small_bucket.tuples[j].key = orderedR->tuples[j+psumR[b].sum].key;
		temp_small_bucket.tuples[j].payload = orderedR->tuples[j+psumR[b].sum].payload;
	}

	// arxikopoihsh temp_chain R
	temp_chain.num_tuples = histR[b].sum;
	temp_chain.key = (int32_t*)malloc( (temp_chain.num_tuples) *sizeof(int32_t));
	for(int k=0; k<temp_chain.num_tuples ; k++){
		temp_chain.key[k] = -1;
	}

	// ONLY FOR PRINTING
	printf("R temp small bucket %d  --  [S(%d)>R(%d)]\n", b, histS[b].sum, histR[b].sum);
	for(int j=0 ; j<temp_small_bucket.num_tuples ; j++){
		printf("(%d,%c)",temp_small_bucket.tuples[j].key, temp_small_bucket.tuples[j].payload );
	}
	printf("\n");

	for( chain_count = temp_small_bucket.num_tuples-1 ; chain_count>=0 ; chain_count--){  // gia to bucket b(h1) ths S
		n = tobinary((uint32_t)temp_small_bucket.tuples[chain_count].payload);
		h_val = h2_hash(n);

		temp_key = & ( temp_bucket.key[h_val] );

		while( (*temp_key) != -1){
			temp_key = &( temp_chain.key[(*temp_key)] );
		}
		*temp_key = chain_count;
	}


	// ONLY FOR PRINTING
	for(int k=0 ; k<num_h2 ; k++){  // MONO GIA PRINT TOU BUCKET
		temp_k = & ( temp_bucket.key[k] ) ;
		while( (*temp_k) != -1){
			l++;
			temp_k = &( temp_chain.key[(*temp_k)] );
		}
		printf("\t H2 S Bucket %d [%d]\n", k, l);

		l=0;
		temp_k = & ( temp_bucket.key[k] ) ;
		while( (*temp_k) != -1){
			l++;
			printf("(%d-%c)",temp_small_bucket.tuples[(*temp_k)].key, temp_small_bucket.tuples[(*temp_k)].payload);
			temp_k = &( temp_chain.key[(*temp_k)] );
		}
		l=0;
		printf("\n");
	}


	// ONLY FOR PRINTING
	printf("\n");
	printf("H1 S Bucket %d\n", b);
	for(int r=psumS[b].sum ; r<(psumS[b].sum)+(histS[b].sum) ; r++){
		n = tobinary((uint32_t)orderedS->tuples[r].payload);
		h_val = h2_hash(n);
		printf("(%d-%c)%d / ", orderedS->tuples[r].key, orderedS->tuples[r].payload ,h_val);
	}
	printf("\n");

//////////////////     JOIN     ///////////////////

	printf("\n");
	printf("JOIN %d\n", b);
	for(int r=psumS[b].sum ; r<(psumS[b].sum)+(histS[b].sum) ; r++){
		n = tobinary((uint32_t)orderedS->tuples[r].payload);
		h_val = h2_hash(n);
		temp_k = & ( temp_bucket.key[h_val] ) ;
		while( (*temp_k) != -1){
			if( temp_small_bucket.tuples[*temp_k].payload == orderedS->tuples[r].payload ){
			//if( tobinary((uint32_t) temp_small_bucket.tuples[*temp_k].payload ) == tobinary((uint32_t) orderedS->tuples[r].payload ) ){
				printf("%d) R(%d %c)-S(%d %c) \n", h_val, temp_small_bucket.tuples[(*temp_k)].key, temp_small_bucket.tuples[(*temp_k)].payload, orderedS->tuples[r].key , orderedS->tuples[r].payload);
				insert_result( temp_small_bucket.tuples[(*temp_k)].key, orderedS->tuples[r].key, &result);
				counter++;
			}
			temp_k = &( temp_chain.key[(*temp_k)] );
		}
	}
	printf("\n");

	// free temp_small_bucket R
	free( temp_small_bucket.tuples );

	// free temp_chain R
	free( temp_chain.key );
}

//printf("\n");

}  // b
	free(temp_bucket.key);

	printf("COUNTER %d\n",counter);

	print_result(&result);

	printf("\n");
	printf("\nEnd of Radix Hash Join\n");
	// return &result;

}


/////////////////////////////////////////
/////////////////////////////////////////
/////////////////////////////////////////

/*void result_init(result* result){

	result->start_list = (struct node*)malloc(sizeof(struct node));
	result->start_list -> buffer_start = (void*)malloc(BUCKET_SIZE);	
	result->start_list -> buffer_end = result->start_list -> buffer_start + BUCKET_SIZE; 
	result->start_list -> buffer = result->start_list -> buffer_start;		
	result->start_list -> next = NULL; 
	result->list_size = 1;
}


void insert_result(int rowR, int rowS ,result* result){
	
	int static call = 0;
	call++;
	// result.list_size++;

	
	int i;
	int * ptr;
	struct node *current_node;
	current_node = result->start_list;

	// printf("\n%d call of function: , List: %d\n", call, result->list_size );
	// printf("Beginning of the list:%p , Size: %d\n\n", result->start_list, result->list_size);

	//go to the bucket 
	for (i = 1; i < result->list_size; i++){
		if(current_node -> next != NULL){
			current_node = current_node -> next;
		}
	}

	//an o xwros tou buffer einai arketos apothikeuse alliws 
	//desmeuse xwro

	// Allocate new bucket
	if(current_node -> buffer_end - current_node->buffer < 2*sizeof(int)){
		//printf("New bucket\n");

		// if(current_node -> next == NULL)
		// 	printf("CORRECT\n");
		// else 
		// 	printf("WRONG\n");
		current_node -> next = (struct node*)malloc(sizeof(struct node));
		current_node -> next -> buffer_start = (void*)malloc(BUCKET_SIZE);
		
		current_node = current_node -> next;

		current_node -> buffer = current_node -> buffer_start;		
		current_node -> buffer_end = current_node -> buffer_start + BUCKET_SIZE; 	
		current_node -> next = NULL;

		result->list_size++;
		// printf("======================== > %d\n", result->list_size);

	}

	ptr = current_node -> buffer;

	*ptr = rowR;
	// *(current_node -> buffer) = rowR;
	current_node -> buffer = current_node -> buffer + sizeof(int);
	ptr = current_node -> buffer ;

	*ptr = rowS;
	// *(current_node -> buffer) = rowS;
	current_node -> buffer = current_node -> buffer + sizeof(int);

	// ptr = ptr + 4;
}

void print_result(result* result){

	printf("--------------- Result ---------------\n");
	printf("List Size:%d\n", result->list_size);

	int i;
	struct node *current_node;
	current_node = result->start_list;


	void * temp = current_node->buffer_start;
	for(i = 0; i < result->list_size; i++){
	
		printf("|");
		while(temp < current_node->buffer){
			
			printf("%d ", *(int*)temp);
			temp = temp + sizeof(int);
			// printf("%d\n", current_node->buffer);
		}
		printf("| ");
		if(current_node->next != NULL){
			current_node = current_node->next;
			temp = current_node->buffer_start;
		}
	}
	printf("\n");
}*/
















