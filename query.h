#ifndef QUERY_H
#define QUERY_H

#include "utils.h"


struct priority{
	uint64_t value;
	int key;
};	

void calculate_query(struct query_info *temp_q,  struct file_info* info);

result* comparison_query(struct file_info *info, int rel, uint64_t col, int value, char comp_op, result *results);

int calculate_priority(struct priority *priority, qinfo *query, finfo *info, int index);

void calculate_sum(struct result* result, struct query_info *query, struct file_info *info, int rel_key, uint64_t column);

void print_sum(struct result* result, struct query_info *query, struct file_info *info);

/*Check if the same predicate exists in the query*/
int predicate_exists(qinfo * query, int num_pred, int current_pred);

#endif /*query.h*/