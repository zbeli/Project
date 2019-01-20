#ifndef STR_H
#define STR_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <math.h>
#include <string.h>

#include "result.h"
#include "job_scheduler.h"
#include "thread_pool.h"


#define h1 8
#define h2 6

result results;
/*MAX number of relations in a predicate is 4!?*/
result result_lists[4];

result sum_result ;

typedef struct relation relation;
/*Type definition for tuple*/
struct tuple{
	int32_t key;
	int32_t payload;
};

/*Type definition for relation
it consists of an array of tuples and a size of the relation*/

struct relation{
	struct tuple *tuples;
	uint32_t num_tuples;
};

typedef struct histogram{
	uint32_t value;
	uint32_t sum;
}histogram;

struct bucket{
	int32_t* key;
};

struct chain{
	int32_t* key;
	int32_t num_tuples;
};

/*Hash functions*/
int h1_hash(uint32_t n);
int h2_hash(uint32_t n);

// uint32_t tobinary(uint32_t x);


/*Radix Hash Join*/
result* RadixHashJoin(relation *relR, relation* relS, struct thrd_pool *temp);

/* kanei push ta jobs  gia thn dhmiourgia twn hist*/
int Hist_and_Psum();
/* kanei push ta jobs gia thn sumplhrwsh tou ordered relation*/
int ReOrdered();
/* kanei push ta jobs pou kanoun to radix hash join gia kathe bucket */
int Join();

#endif /*str.h*/
