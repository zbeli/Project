#ifndef RESULT_H
#define RESULT_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <math.h>

#define BUFFER 1048576	/* 1024*1024 */

struct node{

	void* buffer;			//free space in the begining of the buffer
	void* buffer_start;	   //pointer to the start of the buffer
	void* buffer_end;	  //end of the buffer
	struct node* next;	 //pointer to the next buffer
};

typedef struct result{
	struct node *start_list; //pointer to the first node of the list 
	int list_size;			 //the size of the list
}result;

/*Initialization of the results list*/
void result_init(result* result);
/*Insertion of the results(rowIdR, rowIdS) in the list*/
void insert_result(int rowR, int rowS ,result* result);
/*Printing of the results list*/
void print_result(result* result);

void free_result(result* res);

void free_result2(struct node* temp);

#endif /*result.h*/
