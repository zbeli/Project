#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

/*Type definition for tuple*/
struct tuple
{
	int32_t key;
	int32_t payload;
	
};

/*Type definition for relation
it consists of an array of tuples and a size of the relation*/

struct relation
{
	struct tuple *tuples;
	uint32_t num_tuples;
};

struct result{
	int something;// diko mou
};

/*Radix Hash Join*/

// result* RadixHashJoin(relation *relR, relation* relS){}

typedef struct histogram{
	uint32_t value;
	uint32_t sum;
}histogram;

void create_histogram(histogram* histogram, relation *rel);
void create_psum(histogram* psum,histogram * histogram, relation *rel);
void reorder(relation * ord_rel, relation *rel, histogram* hist, histogram* psum);


int h2_hash(char x);

uint32_t tobinary(uint32_t x);


/*Radix Hash Join*/
// result* RadixHashJoin(relation *relR, relation* relS);
void RadixHashJoin(relation *relR, relation* relS);
