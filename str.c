#include "str.h"


uint32_t tobinary(uint32_t x){
	if(x==0) return 0;
	if(x==1) return 1;
	return (x%2 + 10*tobinary(x/2));
}

int h1_hash(uint32_t n){
	return (n & (1 << h1)-1);
}

int h2_hash(char x){
	return (x % 9);
}

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
		histogram[h_val].value = rel->tuples[i].payload;
		histogram[h_val].sum++;
	}
/*
	printf("+++++++++++++++ Histogram +++++++++++++\n");
	for (i = 0; i < hist_rows; i++){
		printf("%c| %d\n", histogram[i].value, histogram[i].sum);		
	}
	printf(">>>>>>>>>>>>>>>>>>>>%d\n",rel->num_tuples);
	printf("++++++++++++++++++++++++++++++++++++++++\n");*/
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
	printf(" _____________	PSUM   _____________\n");
	for (int i = 0; i < hist_rows; i++){
		printf("%c | %d\n", psum[i].value, psum[i].sum);
	}
	printf(" ___________________________________\n");

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
		ord_rel -> tuples[i].key = i+1;
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
				ord_rel->tuples[j].payload = rel -> tuples[i].payload; 
				break;
			}
		}
	}

	printf("\n==================	ORDERED R    ===================\n");
	for (i = 0; i < tuples; i++){
		printf("%d | %c\n", ord_rel->tuples[i].key,ord_rel->tuples[i].payload);
	}
	printf("\n==================	ORDERED R    ===================\n");

}

/*Radix Hash Join*/
// result* RadixHashJoin(relation *relR, relation* relS){}

void RadixHashJoin(relation *relR, relation* relS){
	
	int hist_rows = (int)pow(2,h1);

	//Histogram creation of the two relations R,S
	histogram histS[hist_rows];
	create_histogram(histS, relS);

	histogram histR[hist_rows];
	create_histogram(histR, relR);

	//Psum creation
	histogram psumR[hist_rows];
	create_psum(psumR, histR, relR);

	histogram psumS[hist_rows];
	create_psum(psumS, histS, relS);

	struct relation orderedR[relR->num_tuples];
	reorder(orderedR, relR, histR, psumR);

	struct relation orderedS[relS->num_tuples];
	reorder(orderedS, relS, histS, psumS);

	
////////////
// PART 2 //
////////////

	printf("End of Radix Hash Join\n");

}
