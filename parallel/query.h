#ifndef QUERY_H
#define QUERY_H

#include "utils.h"
#include "parse.h"

/*Information for keeping the priority of the predicates in the query*/
struct priority{
	uint64_t value;
	int key;
};	

/*Calculation of the query*/
void calculate_query(struct query_info *temp_q,  struct file_info* info, struct thrd_pool *thread_pool, fstats *stats);
/*Function for calculating filter type of predicate*/
result* comparison_query(struct file_info *info, int rel, uint64_t col, int value, char comp_op, result *results);


/*Calculation of the sums for the current query*/
void calculate_sum(struct result* result, struct query_info *query, struct file_info *info, int rel_key, uint64_t column);
/*Function for printing the results of the query*/
void print_sum(struct result* result, struct query_info *query, struct file_info *info, struct thrd_pool *pool);

/*Function(Greedy) for finding the priority of the predicates in the query*/
int calculate_priority(struct priority *priority, qinfo *query, finfo *info, int index);
/*Function for checking if the same predicate exists in the query*/
int predicate_exists(qinfo * query, int num_pred, int current_pred);

#endif /*query.h*/