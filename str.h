#ifndef STR_H
#define STR_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <math.h>

#define h1 3
#define h2 2

#define BUCKET_SIZE 16

typedef struct relation relation;
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


int h2_hash(uint32_t n);

uint32_t tobinary(uint32_t x);


/*Radix Hash Join*/
// result* RadixHashJoin(relation *relR, relation* relS);
void RadixHashJoin(relation *relR, relation* relS);







//////////////////
//////////////////
/////////////////

struct node{

	void* buffer;			//free space in the begining of the buffer
	void* buffer_start;	   //pointer to the start of the buffer
	void* buffer_end;	  //end of the buffer
	struct node* next;	 //pointer to the next buffer
};

typedef struct result{
	struct node *start_list; //pointer to the first 
							 //node of the list
	int list_size;
}result;


void result_init(result* result);
void insert_result(int rowR, int rowS ,result* result);
void print_result(result* result);

#endif /*str.h*/
