#include "utils.h"

#include <string.h>

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
		// printf("rel:%lu\n",query->rels[i]);
	}
	printf("--------------------\n");
	printf("Predicates %d\n", query->pred_count);
	for(int i=0 ; i<query->pred_count ; i++){
		if(query->preds[i].value==-1){
			// printf("rel1:%lu col1:%lu - operator:%c - rel2:%lu col2:%lu\n",query->preds[i].tuple_1.rel, query->preds[i].tuple_1.col, query->preds[i].op , query->preds[i].tuple_2.rel, query->preds[i].tuple_2.col);
		}
		else{
			// printf("rel1:%lu col1:%lu - operator:%c - value:%d\n",query->preds[i].tuple_1.rel, query->preds[i].tuple_1.col, query->preds[i].op , query->preds[i].value);
		}
	}
	printf("--------------------\n");
	printf("Provoles %d\n", query->cols_count);
	for(int i=0 ; i<query->cols_count ; i++){
		// printf("rel:%lu col:%lu\n", query->cols_to_print[i].rel, query->cols_to_print[i].col);
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
		query->preds[index].flag = 0;
		query->preds[index].value = atoi(token);
	}
	else{
		query->preds[index].tuple_2.rel = atoi(token);
		query->preds[index].tuple_2.col = atoi(tok);

		query->preds[index].flag = -1;
	}
}


result* comparison_query(struct file_info *info, uint64_t rel, uint64_t col, int value, char comp_op, result *results){
	result_init(results);

	int count=0;
	for(int i=1 ; i<=info[rel].num_tup ; i++){
		if(comp_op=='='){
			if(info[rel].col_array[col][i] == value){
				//printf(" -> %d) %lu \n", i, info[rel].col_array[col][i]);
				count++;
				//insert_result(-1, info[rel].col_array[col][i], results);
				insert_inter(i, results);			
			}

		}
		else if(comp_op=='>'){
			if(info[rel].col_array[col][i] > value){
				//printf(" -> %d) %lu \n", i, info[rel].col_array[col][i]);
				count++;
				//insert_result(-1, info[rel].col_array[col][i], results);
				insert_inter(i, results);		
			}
		}
		else if(comp_op=='<'){
			if(info[rel].col_array[col][i] < value){
				//printf(" -> %d) %lu \n", i, info[rel].col_array[col][i]);
				count++;
				//insert_result(-1, info[rel].col_array[col][i], results);
				insert_inter(i, results);					
			}
		}

	}
	// printf("COUNT:%d  NUM_TUP:%lu\n",count,info[rel].num_tup);
}

void print_sums(result *res, struct query_info *query){
	struct node *current_node;
	void * temp;

	int index;
	uint64_t sum;
	for(int i = 0; i < query->cols_count ; i++){
		for(index=0 ; index<query->cols_count ; index++){
			if(query->cols_to_print[i].rel == query->rels[index]){
				break;		// index keep the position of current relation in res
			}
		}

		sum = 0;
		current_node = res[index].start_list;
		temp = current_node->buffer_start;

		for(int j=0 ; j<res[index].list_size ; j++){
			while(temp < current_node->buffer){
				sum += *(int*)temp;
				temp = temp + sizeof(int);
			}
			if(current_node->next != NULL){
				current_node = current_node->next;
				temp = current_node->buffer_start;
			}
		}

		printf("rel:%llu col:%llu %llu\n", query->cols_to_print[i].rel, query->cols_to_print[i].col, sum);
	}
}

void update_results(result *result_lists, result *tmp_list1, result *tmp_list2){
	if(tmp_list2==NULL){
		printf("Update_Results FILTER\n");
	}
	else{
		printf("Update_Results JOIN\n");
	}
}

