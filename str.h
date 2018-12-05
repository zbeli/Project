#ifndef STR_H
#define STR_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <math.h>

#include "result.h"


// #define h1 12
// #define h2 8

#define h1 3
#define h2 2

/*MAX number of relations in a predicate is 4!?*/
result result_lists[4];


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


	int rel_id; //////////////////////???
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


void create_histogram(histogram* histogram, relation *rel);
void create_psum(histogram* psum,histogram * histogram, relation *rel);
void reorder(relation * ord_rel, relation *rel, histogram* hist, histogram* psum);


/*Hash functions*/

int h1_hash(uint32_t n);
int h2_hash(uint32_t n);

uint32_t tobinary(uint32_t x);


/*Radix Hash Join*/
result* RadixHashJoin(relation *relR, relation* relS);


#endif /*str.h*/
