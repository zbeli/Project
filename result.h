#ifndef RESULT_H
#define RESULT_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <math.h>


#define BUFFER 1048576	 //1024*1024 
// #define BUFFER 128000	


struct node{

	void* buffer;			//free space in the begining of the buffer
	void* buffer_start;	   //pointer to the start of the buffer
	void* buffer_end;	  //end of the buffer
	struct node* next;	 //pointer to the next buffer
};

typedef struct result{
	struct node *start_list;   //pointer to the first node of the list 
	int list_size;			   //the size of the list
    struct node* current_node; //node that we are writing the results currently
	int counter;               //number of elements in the list
}result;

/*Initialization of the results list*/
void result_init(result* result);
/*Insertion of the results(rowIdR, rowIdS) in the list*/
void insert_result(int rowR, int rowS ,result* result);
/*Printing of the results list*/
void print_result(result* result);
/*Free memory used for keeping the results*/
void free_result(result* res);
/*Insert one row in the results*/
void insert_inter(int row, result* result);


#endif /*result.h*/
