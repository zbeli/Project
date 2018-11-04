#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <math.h>

#include "str.h"

#define R_SIZE 10
#define S_SIZE 72
#define S_SIZE_0 216

int main(void){

// uint64_t numTuples|uint64_t numColumns|uint64_t T0C0|
// uint64_t T1C0|..|uint64_t TnC0|uint64_t T0C1|..|
// uint64_t TnC1|..|uint64_t TnCm

	//test_case_0
	//Table R
int r_size = 10;
	char R[10] = {'1','a','Z','a','d','d','c','b','G','a'};
int s_size = 216;
	char S[216] = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z',
		'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z',
		'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z',
		'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z',
		'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','e','t','u','v','w','x','y','z',
		'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','e','t','u','v','w','x','y','z'};

printf("-------- S ---------\n");
for(int i=0 ; i<S_SIZE ; i++){
	printf("%c ",S[i]);
}
printf("---------------------\n");

/*	//test_case_1
	//Table R
int r_size = 10;
	char R[R_SIZE] = {'a','a','a','a','d','d','c','b','a','a'};
int s_size = 72;
	char S[S_SIZE] = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z',
		'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','e','t','u','v','w','x','y','z'};

for(int i=0 ; i<S_SIZE ; i++){
	printf("%c ",S[i]);
}
*/

/*	//test_case_2
	//Table R
int r_size = 16;
	char R[16] = {'0','1','2','3','4','5','6','7','8','9',':',';','<', '=','>','?'};
int s_size = 16;
	char S[16] = {'9','8','7','6','5','4','3','2','1','0',':',';','<','=','>','?'};
*/

/*	//test_case_7
	//Table R
int r_size = 20;
	char R[20] = {'0','1','2','3','4','5','6','7','8','9',':',';','<', '=','>','?','a','b','c','='};
int s_size = 16;
	char S[16] = {'9','8','7','6','5','4','3','2','1','0',':',';','<','=','>','?'};
*/

/*	//test_case_3
int r_size = 92;
	char R[r_size];
	R[0] = '!';
	for(int i=1 ; i<r_size ; i++){
		R[i] = R[i-1]+1;
	} 
	for(int i=0 ; i<r_size ; i++){
		printf("%c ",R[i] );
	}
	printf("\n");

int s_size = 92;
	char S[s_size];
	S[s_size-1] = '!';
	for(int i=s_size-2 ; i>=0 ; i--){
		S[i] = S[i+1]+1;
	} 
	for(int i=0 ; i<s_size ; i++){
		printf("%c ",S[i]) ;
	}
	printf("\n");
*/

/*	//test_case_3
int r_size = 32;
	char R[r_size];
	for(int i=0 ; i<r_size ; i++){
		R[i] = 'A' + (random()%26);
	} 
	for(int i=0 ; i<r_size ; i++){
		printf("%c ",R[i] );
	}
	printf("\n");

int s_size = 32;
	char S[s_size];
	for(int i=0 ; i<s_size ; i++){
		S[i] = 'A' + (random()%26);
	} 
	for(int i=0 ; i<s_size ; i++){
		printf("%c ",S[i]) ;
	}
	printf("\n");

*/

/*	//test_case_4	//monadiaioi pinakes
int r_size = 100;
	char R[r_size];
	for(int i=0 ; i<r_size ; i++){
		R[i] = '1';
	} 
	for(int i=0 ; i<r_size ; i++){
		printf("%c ",R[i] );
	}
	printf("\n");

int s_size = 10;
	char S[s_size];
	for(int i=0 ; i<s_size ; i++){
		S[i] = '1';
	} 
	for(int i=0 ; i<s_size ; i++){
		printf("%c ",S[i]) ;
	}
	printf("\n");	
*/

/*	//test_case_5	//perittoi artioi
int r_size = 5;
	char R[r_size];
	for(int i=0 ; i<r_size ; i++){
		R[i] = '1'+i*2;
	} 
	for(int i=0 ; i<r_size ; i++){
		printf("%c ",R[i] );
	}
	printf("\n");

int s_size = 5;
	char S[s_size];
	for(int i=0 ; i<s_size ; i++){
		S[i] = '0'+i*2;
	} 
	for(int i=0 ; i<s_size ; i++){
		printf("%c ",S[i]) ;
	}
	printf("\n");	 
*/


/*	//test_case_6  // R==S
int r_size = 10;
	char R[r_size];
	for(int i=0 ; i<r_size ; i++){
		R[i] = '0'+i;
	} 
	for(int i=0 ; i<r_size ; i++){
		printf("%c ",R[i] );
	}
	printf("\n");

int s_size = 10;
	char S[s_size];
	for(int i=0 ; i<s_size ; i++){
		S[i] = '0'+i;
	} 
	for(int i=0 ; i<s_size ; i++){
		printf("%c ",S[i]) ;
	}
	printf("\n");	
*/

/*	//test_case_8
	//Table R
int r_size = 2;
	char R[2] = {'0','1'};
int s_size = 16;
	char S[16] = {'9','8','1','6','5','4','3','2','1','1',':',';','<','1','>','1'};
*/


 
    /////////////////////////////////////////
    /////////////////////////////////////////
    int i,j;

    struct relation relR;
    relR.num_tuples = r_size;
    relR.tuples = (struct tuple*)malloc(relR.num_tuples*sizeof(struct tuple));
    for (i = 0; i < relR.num_tuples; i++){
    	relR.tuples[i].key = i+1;
    	relR.tuples[i].payload = R[i];
    }

    struct relation relS;
    relS.num_tuples = s_size;
    relS.tuples = (struct tuple*)malloc(relS.num_tuples*sizeof(struct tuple));
    for (i = 0; i < relS.num_tuples; i++){
    	relS.tuples[i].key = i+1;
    	relS.tuples[i].payload = S[i];
    }


    RadixHashJoin(&relR, &relS);


    printf("End of Program.\n");
	return 0;
}

