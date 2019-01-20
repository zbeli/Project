#ifndef PARSE_H
#define PARSE_H

#include "utils.h"

/*stats about the relations*/
typedef struct file_stats{
	uint64_t num_col;      //number of columns 
    uint64_t *min_elem;    //minimum element of every column 
    uint64_t *max_elem;    //maximum element of every column

    uint64_t f;            //number of values of the relations(num tuples)
    uint64_t *d;           //number of distinct values of every column
}fstats;

void information_storing(struct file_info* info, uint64_t * data, struct file_stats * fstats);
void print_query_info(struct query_info* query);
void insert_pred(struct query_info* query, char* pred, int index);



#endif /*parse.h*/