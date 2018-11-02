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
