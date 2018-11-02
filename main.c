#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <math.h>

#define h1 3
// #define h1 12


uint32_t tobinary(uint32_t x){
	if(x==0) return 0;
	if(x==1) return 1;
	return (x%2 + 10*tobinary(x/2));
}


int main(void){


// uint64_t numTuples|uint64_t numColumns|uint64_t T0C0|
// uint64_t T1C0|..|uint64_t TnC0|uint64_t T0C1|..|
// uint64_t TnC1|..|uint64_t TnCm

	//Table R
	char R[10][3] = {
		{'a', 'e', 'i'},
		{'b', 'f', 'j'},
		{'c', 'g', 'k'},
		{'a', 'h', 'k'},
		{'e', 'h', 'k'},
		{'f', 'h', 'k'},
		{'c', 'h', 'k'},
		{'b', 'h', 'k'},
		{'h', 'h', 'k'},
		{'a', 'h', 'k'},
	};

	char S[10][2] = {
		{'a','e'},
		{'b','s'},
		{'w','e'},
		{'d','e'},
		{'c','e'},
		{'e','e'},
		{'l','e'},
		{'q','e'},
		{'p','e'},
		{'q','e'},
	};

	int numTuples = 10;
	int numColumns = 3;
	int rowldRcolumns = 2;
	
	int i,j;
	// int *rowldR = (int*) malloc(numTuples * numColumns * sizeof(int));


    int **rowldR = (int **)malloc(numTuples * sizeof(int *)); 
    for (i = 0; i < numTuples; i++)
    {
    	rowldR[i] = (int*) malloc(numColumns* sizeof(int));	
    }

    j=0;
	for(i=0; i < numTuples; i++){

		rowldR[i][0] = i+1;
		rowldR[i][1] = R[i][0];		
	}

	for (i = 0; i < numTuples; i++){
		printf("%d %c %d\n", rowldR[i][0], rowldR[i][1], tobinary(rowldR[i][1]));
	}

	printf(" ------------ END OF PROGRAM ---------------\n");

	return 0;
}
