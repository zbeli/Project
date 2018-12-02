#ifndef UTILS_H
#define UTILS_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

/*information about relations*/
struct file_info {
	uint64_t num_tup;   //number of tuples
	uint64_t num_col;   //number of columns

	uint64_t **col_array; //array of pointers to the columns of the relation

};

void create_col_array(struct file_info* info, uint64_t * data);
#endif /*utils.h*/

