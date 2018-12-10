#ifndef QUERY_H
#define QUERY_H

#include "utils.h"

void calculate_query(struct query_info *temp_q,  struct file_info* info);

result* comparison_query(struct file_info *info, uint64_t rel, uint64_t col, int value, char comp_op, result *results);

void calculate_priority(struct priority *priority, struct query_info *query, struct file_info *info);

void calculate_sum(struct result* result, struct query_info *query, struct file_info *info, int rel_key, uint64_t column);

#endif /*query.h*/