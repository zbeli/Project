#include "result.h"

void result_init(result* result){

	result->start_list = (struct node*)malloc(sizeof(struct node));
	result->start_list -> buffer_start = (void*)malloc(BUFFER);	
	result->start_list -> buffer_end = result->start_list -> buffer_start + BUFFER; 
	result->start_list -> buffer = result->start_list -> buffer_start;		
	result->start_list -> next = NULL; 
	result->list_size = 1;

	result->counter = 0;
}


void insert_result(int rowR, int rowS ,result* result){
	
	int i;
	int * ptr;
	struct node *current_node;
	current_node = result->start_list;

	//Go to the current bucket 
	for (i = 1; i < result->list_size; i++){
		if(current_node -> next != NULL){
			current_node = current_node -> next;
		}
	}

	// Allocate new bucket if necessary
	if(current_node -> buffer_end - current_node->buffer < 2*sizeof(int)){
		//printf("New bucket\n");

		current_node -> next = (struct node*)malloc(sizeof(struct node));
		current_node -> next -> buffer_start = (void*)malloc(BUFFER);
		
		current_node = current_node -> next;

		current_node -> buffer = current_node -> buffer_start;		
		current_node -> buffer_end = current_node -> buffer_start + BUFFER; 	
		current_node -> next = NULL;

		result->list_size++;

	}

	ptr = current_node -> buffer;

	*ptr = rowR;
	current_node -> buffer = current_node -> buffer + sizeof(int);
	ptr = current_node -> buffer;
	*ptr = rowS;
	current_node -> buffer = current_node -> buffer + sizeof(int);

	result -> counter = result -> counter + 2; 

}

/////////////////////////////////////////////////////////////////////
void insert_inter(int row, result* result){
	int i;
	int * ptr;
	struct node *current_node;
	current_node = result->start_list;

	//Go to the current bucket 
	for (i = 1; i < result->list_size; i++){
		if(current_node -> next != NULL){
			current_node = current_node -> next;
		}
	}

	// Allocate new bucket if necessary
	if(current_node -> buffer_end - current_node->buffer < sizeof(int)){
		// printf("New bucket\n");

		current_node -> next = (struct node*)malloc(sizeof(struct node));
		current_node -> next -> buffer_start = (void*)malloc(BUFFER);
		
		current_node = current_node -> next;

		current_node -> buffer = current_node -> buffer_start;		
		current_node -> buffer_end = current_node -> buffer_start + BUFFER; 	
		current_node -> next = NULL;

		result->list_size++;

	}

	ptr = current_node -> buffer;

	*ptr = row;
	current_node -> buffer = current_node -> buffer + sizeof(int);
	result -> counter++;
}

void print_result(result* result){

	printf("--------------- Result ---------------\n");
	printf("List Size:%d\n", result->list_size);

	int i;
	struct node *current_node;
	current_node = result->start_list;


	void * temp = current_node->buffer_start;
	for(i = 0; i < result->list_size; i++){
	
		printf("|");
		while(temp < current_node->buffer){
			
			printf("%d ", *(int*)temp);
			temp = temp + sizeof(int);
		}
		printf("| ");
		if(current_node->next != NULL){
			current_node = current_node->next;
			temp = current_node->buffer_start;
		}
	}
	printf("\n");
}

void free_result(result* res){
	struct node *cur_node, *next_n;

    cur_node = res->start_list;
    while(cur_node != NULL){
    	next_n = cur_node->next;

    	free(cur_node->buffer_start);
    	cur_node->buffer_start = NULL;
    	free(cur_node);
    	cur_node = NULL;
    	cur_node = next_n;
    }
}