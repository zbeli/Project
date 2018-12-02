#include "kati.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

void insert_rel(rel_list_head* list, uint64_t columns, uint64_t rows){
	rel_list* temp;
	if(list->first==NULL){
		list->first = (rel_list*)malloc(sizeof(rel_list));
		list->first->columns = columns;
		list->first->rows = rows;
		list->first->next = NULL;
		list->first->rels = NULL;
	}
	else{
		temp = list->first;
		while(temp->next!=NULL){
			temp = temp->next;
		}
		temp->next = (rel_list*)malloc(sizeof(rel_list));
		temp->next->columns = columns;
		temp->next->rows = rows;
		temp->next->next = NULL;
		temp->next->rels = NULL;
	}
	list->count++;
}

void print_list(rel_list_head* list){
	rel_list* temp;
	temp = list->first;
	printf("COUNT : %d\n",list->count);
	while(temp!=NULL){
		printf("%" PRIu64 " - %" PRIu64 "\n",temp->rows,temp->columns);
		temp = temp->next;
	}
}

void free_list(rel_list_head* list){
	if(list->first != NULL){
		free_l(list->first);
	}
	free(list->first);
}

void free_l(rel_list* l){
	if(l->next!=NULL){
		free_l(l->next);
	}
	free(l);
}