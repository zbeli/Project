#include "utils.h"


void create_col_array(struct file_info *info, uint64_t * data){

	int i;

	// printf("---------->>>>> info: %llu, \tdata: %llu\n", info->num_tup, *data);

	info -> col_array = (uint64_t**)malloc((info -> num_col)*sizeof(uint64_t*));

	for(i = 0; i < info -> num_col; i++){
		info -> col_array[i] = (data + i*info->num_tup + 2);
	}
}

void print_query_info(struct query_info* query){
	printf("Sxeseis %d\n", query->rel_count);
	for(int i=0 ; i<query->rel_count ; i++){
		printf("rel:%llu\n",query->rels[i]);
	}
	printf("--------------------\n");
	printf("Predicates %d\n", query->pred_count);
	for(int i=0 ; i<query->pred_count ; i++){
		if(query->preds[i].value==-1){
			printf("rel1:%llu col1:%llu - operator:%c - rel2:%llu col2:%llu\n",query->preds[i].tuple_1.rel, query->preds[i].tuple_1.col, query->preds[i].op , query->preds[i].tuple_2.rel, query->preds[i].tuple_2.col);
		}
		else{
			printf("rel1:%llu col1:%llu - operator:%c - value:%d\n",query->preds[i].tuple_1.rel, query->preds[i].tuple_1.col, query->preds[i].op , query->preds[i].value);
		}
	}
	printf("--------------------\n");
	printf("Provoles %d\n", query->cols_count);
	for(int i=0 ; i<query->cols_count ; i++){
		printf("rel:%llu col:%llu\n", query->cols_to_print[i].rel, query->cols_to_print[i].col);
	}
}

void insert_pred(struct query_info* query, char* pred, int index){
	char delimeter[2];
	for(int i=0 ; i<strlen(pred) ; i++){
		if(pred[i]=='=' || pred[i]=='<' || pred[i]=='>'){
			delimeter[0] = pred[i];
			delimeter[1] = '\0';
		}
	}
	char *token;

	token = strtok_r(pred, ".",&pred);
	query->preds[index].tuple_1.rel = atoi(token);

	token = strtok_r(pred, delimeter,&pred);
	query->preds[index].tuple_1.col = atoi(token);

	query->preds[index].op = delimeter[0];

	char *tok;
	token = strtok_r(pred, ".",&pred);
	tok = strtok_r(pred, ".",&pred);

	if(tok==NULL){
		query->preds[index].value = atoi(token);
	}
	else{
		query->preds[index].tuple_2.rel = atoi(token);
		query->preds[index].tuple_2.col = atoi(tok);

		query->preds[index].value = -1;
	}
}

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

}


