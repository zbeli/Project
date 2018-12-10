#ifndef UTILS_H
#define UTILS_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include "str.h"
#include "result.h"

/*information about relations*/
struct file_info {
	uint64_t num_tup;   //number of tuples
	uint64_t num_col;   //number of columns

	uint64_t **col_array; //array of pointers to the columns of the relation

};

struct priority{
	uint64_t value;
	int key;
};		

struct rel_col_tuple {
	int rel;   //id of relation
	uint64_t col;	//id of column
};

/*predicate information*/
struct predicate {
	struct rel_col_tuple tuple_1;   // tuple1
	struct rel_col_tuple tuple_2;   // tuple2

	char op;		//operator

	int flag;		// -1 if there is a second rel_col_tuple
	uint64_t value;		//  if there is no a second rel_col_tuple holds the int value

};

/*query information*/
struct query_info {
	int rel_count;
	int pred_count;
	int cols_count;

	int* rels;   						//relations involved in query

	struct predicate* preds;   				//predicates
	struct rel_col_tuple* cols_to_print;	//columns to print
};

struct rel_info{
	int	rel_id;
	int rel_key;	// 0 1 2 3
};


void insert_inter(int row, result* result);

void update_results(result *result_lists, result *tmp_list1, uint64_t index_1, result *tmp_list2, uint64_t index_2, result* res, struct file_info* info);

void create_relation(struct relation* rel, struct file_info *info, int rel_id, uint64_t column);

void create_rel_from_list(struct relation* rel, struct result* result, struct file_info *info, int rel_id, uint64_t column);

void create_interlist(struct result *result, struct result* list1, struct result* list2, struct file_info* info);

void list_to_rel_with_indexes(struct relation* rel, struct result* result);

void copy_result(result *dest, result *source);

void update_interlists(result *result_lists, result *combined_result, result *tmp_list1, uint64_t index_1, result *tmp_list2, uint64_t index_2, struct file_info* info);

void create_rel_from_list_1(struct relation* rel, struct result* result, struct file_info *info, int rel_id, uint64_t column);

#endif /*utils.h*/

