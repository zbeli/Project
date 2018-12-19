#ifndef PARSE_H
#define PARSE_H

#include "utils.h"

void information_storing(struct file_info* info, uint64_t * data, struct file_stats * fstats);
void print_query_info(struct query_info* query);
void insert_pred(struct query_info* query, char* pred, int index);


#endif /*parse.h*/