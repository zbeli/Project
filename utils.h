#ifndef UTILS_H
#define UTILS_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include "str.h"
#include "result.h"

typedef struct file_info finfo;
typedef struct query_info qinfo;

/*information about relations*/
struct file_info {
	uint64_t num_tup;   //number of tuples
	uint64_t num_col;   //number of columns

	uint64_t **col_array; //array of pointers to the columns of the relation
};

struct file_stats{
    uint64_t *min_elem;    //minimum element of the file
    uint64_t *max_elem;    //maximum element of the file
};

	

struct rel_col_tuple {
	int rel;        //id of relation
	uint64_t col;	//id of column
};

/*predicate information*/
struct predicate {
	struct rel_col_tuple tuple_1;   // tuple1
	struct rel_col_tuple tuple_2;   // tuple2

	char op;		    //operator

	int flag;		    // -1 if there is a second rel_col_tuple
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

void update_results(result *result_lists, result *tmp_list1, int index_1,
	 result *tmp_list2, int index_2, result* res, struct file_info* info, struct query_info *temp_q, int pred_index);

void create_relation(struct relation* rel, struct file_info *info, int rel_id, uint64_t column);


void create_rel_from_list(struct relation* rel, struct result* result, struct file_info *info, int rel_id, uint64_t column);

/*Creation of  the two lists from the result of RHJ*/
void create_interlist(struct result *result, struct result* list1, struct result* list2, struct file_info* info);

void list_to_rel_with_indexes(struct relation* rel, struct result* result);

void copy_result(result *dest, result *source);

void update_interlists(result *result_lists, result *combined_result, result *tmp_list1, int index_1, result *tmp_list2, int index_2, struct file_info* info, struct query_info *temp_q, int pred_index);

void create_rel_from_list_distinct(struct relation* rel, struct result* result, struct file_info *info, int rel_id, uint64_t column);

void update_results_filter(result *result_lists, result *tmp_list1, int index, finfo * info, qinfo *query, int pred_index);

void update_interlists_filter(result *result_lists, result *combined_result, result *tmp_list1, int index, finfo *info, qinfo *temp_q, int pred_index);

void update_existing_interlists(relation *rel_R, relation * rel_S, result *result_lists, finfo *info, qinfo *temp_q, int current_pred);

/*Function for same relation in the predicate*/
void relation_similarity(relation *relR, relation * relS, result *result_lists, finfo *info, qinfo *query, int current_pred);

void updateDifferCol(relation *relR, relation *relS, result * result_lists, finfo *info, qinfo *query, int current_pred);


//tessting///////////////////////////
void update_interlists_new(result *result_lists, result *combined_result, result *tmp_list1, int index_1, result *tmp_list2, int index_2, struct file_info* info, struct query_info *temp_q, int pred_index);

/*Creation of relation from list*/
void list_to_relation(struct relation* rel, struct result* result, struct file_info *info, int rel_id, uint64_t column);

//tessting

#endif /*utils.h*/

