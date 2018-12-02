#ifndef KATI_H
#define KATI_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#include "str.h"

typedef struct rel_list{
	uint64_t columns;
	uint64_t rows;
	relation* rels;
	struct rel_list* next;
}rel_list;

typedef struct rel_list_head{
	int count;
	rel_list* first;
}rel_list_head;

void insert_rel(rel_list_head* list, uint64_t columns, uint64_t rows);

void print_list(rel_list_head* list);

void free_list(rel_list_head* list);

void free_l(rel_list* l);

#endif /*kati.h*/
