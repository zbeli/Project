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
		/*for(index=0 ; index<query->cols_count ; index++){
			if(query->cols_to_print[i].rel == query->rels[index]){
				break;		// index keep the position of current relation in res
			}//////////////////////////////////////////////////////////
		}*/

		sum = 0;
		current_node = res[index].start_list;
		temp = current_node->buffer_start;

		for(int j=0 ; j<res[index].list_size ; j++){
			while(temp < current_node->buffer){
				sum += *(int*)temp;//////////////////////////////////////////
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

void update_results(result *result_lists, result *tmp_list1, uint64_t index_1, result *tmp_list2, uint64_t index_2){
	if(tmp_list2 == NULL){
		printf("Update_Results FILTER\n");

		result combined_result;
		result_init(&combined_result);

		struct node *current_node = tmp_list1->start_list;
		int* temp = current_node->buffer_start;

		struct node *current_node_inner = result_lists[index_1].start_list;
		int* temp_inner = current_node_inner->buffer_start;

		for(int i=0 ; i< tmp_list1->list_size; i++){
			while((void*)temp < current_node->buffer){


				for(int j=0 ; j<result_lists[index_1].list_size ; j++){
					while((void*)temp_inner < current_node_inner->buffer){



///////////////////////////////////////////////////////



						//////////////////////////////////////////////////





						temp_inner = temp_inner + 1;
					}
					if(current_node_inner->next != NULL){
						current_node_inner = current_node_inner->next;
						temp_inner = current_node_inner->buffer_start;
					}
				}



				temp = temp + 1;
			}
			if(current_node->next != NULL){
				current_node = current_node->next;
				temp = current_node->buffer_start;
			}
		}
	}
	else if((tmp_list2 != NULL)){

		///find the correct index1
		struct node *cur_node = result_lists[index_1].start_list;
		int* tmp = cur_node -> buffer_start;

		struct node *cur_node_inner = tmp_list1 -> start_list;
		// int* temp_inner = current_node_inner->buffer_start;

	    for(int i = 0 ; i < result_lists[index_1].list_size; i++){

	    }



		printf("Update_Results JOIN\n");
	}


}

void create_relation(struct relation* rel, struct file_info *info, int rel_id, uint64_t column){
	/*Create relation from file*/
	// printf("Create relation from file!\n");
	int i,j;
	uint64_t *col_ptr; /*pointer to the column of the relation*/

    rel->num_tuples = info[rel_id].num_tup;
    col_ptr = info[rel_id].col_array[column];

    printf("REL: %d COL: %llu rows: %d \n", rel_id, column, rel->num_tuples);
    
    printf("    -->%llu\n", *col_ptr);
    
    rel->tuples = (struct tuple*)malloc(rel->num_tuples*sizeof(struct tuple));

    for (i = 0; i < rel -> num_tuples; i++){
    	rel -> tuples[i].key = i;
    	// printf("%llu ", *(col_ptr+i));
    	rel -> tuples[i].payload = *(col_ptr+i);
    	// printf("%llu ", rel->tuples[i].payload);
    }
}

void create_rel_from_list(struct relation* rel, struct result* result, struct file_info *info, int rel_id, uint64_t column){

	int i, j;
	uint64_t *col_ptr; /*pointer to the column of the relation*/
	struct node *current_node;
	current_node = result -> start_list;
	int * temp = current_node->buffer_start;

	rel -> num_tuples = result -> counter;
    col_ptr = info[rel_id].col_array[column];

	rel -> tuples = (struct tuple*)malloc(rel->num_tuples*sizeof(struct tuple));
    j = 0;
	for(i = 0; i < result -> list_size; i++){
    
    	while((void*)temp < current_node->buffer){

		    rel -> tuples[j].key = *temp;   					//!!!!!!!!!!!!!!!!!!!!!??
	    	rel -> tuples[j].payload = *(col_ptr + *(temp));
            temp = temp + 1;
	    	j++;
		}

		if(current_node->next != NULL){
			current_node = current_node->next;
			temp = current_node->buffer_start;
		}
    }

}

// void create_interlist(struct result *result, struct result* list1, struct result* list2, int rel1, int rel2, struct file_info* info){
void create_interlist(struct result *result, struct result* list1, struct result* list2, struct query_info * query, struct file_info* info, int pred_id){

	// printf("\t\t ----------------CREATE INTERLIST------------------\n");
	int i;
	struct node *current_node;
	current_node = result -> start_list;
	int row_1, row_2;
	int* temp = current_node -> buffer_start;

	int rel1 = query->rels[query -> preds[pred_id].tuple_1.rel];
	uint64_t col1 = query -> preds[pred_id].tuple_1.col;
	int rel2 = query->rels[query -> preds[pred_id].tuple_2.rel];
	uint64_t col2 = query -> preds[pred_id].tuple_2.col;

	int j = 0; //debug

	for(i = 0; i < result -> list_size; i++){

		while((void*)temp < current_node -> buffer ){
			row_1 = *(int*)temp;
			row_2 = *(int*)(temp+1);
			temp = temp + 2;
			/*Check if the values exist in the respective lists*/
			// item_exists(list1, row_1, info, rel1, col1);
			// item_exists(list2, row_2, info, rel2, col2);

			insert_inter(row_1, list1);
			insert_inter(row_2, list2);
		}

		if(current_node -> next != NULL){
			current_node = current_node -> next;
			temp = current_node -> buffer_start;
		}
	}	

}

void calculate_sum(struct result* result, struct query_info *query, struct file_info *info, int rel_key, uint64_t column){
    int i;
    uint64_t sum = 0;
    uint64_t *col_ptr; /*pointer to the column of the relation*/


    struct node *current_node;
	current_node = result -> start_list;
	int * temp = current_node -> buffer_start;

    col_ptr = info[rel_key].col_array[column];

    for(i = 0; i < result -> list_size; i++){

   		while((void*)temp < current_node->buffer){
            sum = sum + *(col_ptr + *(temp));   	//////!!!!!!!			
			temp = temp + 1;
		}

		if(current_node -> next != NULL){
			current_node = current_node->next;
			temp = current_node->buffer_start;
		}   
    }
    printf("\t\t\t------------------SUM-------------------------\n");
    printf("\t\t\t REL: %d, COLUMN: %llu SUM: %llu\n", rel_key, column, sum);
    printf("\t\t\t----------------------------------------------\n");

}

void calculate_priority(struct priority *priority, struct query_info *query, struct file_info *info){
	printf("___________________NEW CALCULATION_________________\n");
	int i, j;

	int rel1, rel2, col1;	///////////////////////
	int id_rel1, id_rel2;

	uint64_t num_tup1 = 0, num_tup2 = 0;
	struct priority temp;

	int num_pred;   //number of predicates in the query
	int pred_type;  //type of predicate
	int num_rel;    //number of relations in the query

	num_rel = query->rel_count;
	num_pred = query->pred_count;

	for(i = 0; i < num_pred; i++){
		priority[i].key = i;
	}


	/*for every predicate in the query find the priority value*/
	for(i = 0; i < num_pred; i++){

		// printf("$$$$$$$$$$$$$$$$> %d \n ", query->preds[i].flag);

		// ?????? not sure ?
		rel1 = query->preds[i].tuple_1.rel;

		id_rel1 = query->rels[rel1];
		num_tup1 = info[id_rel1].num_tup;

		// printf("NUM_TUPLES: %llu\n", num_tup1);

		// printf("############: => %d\n", id_rel1);

		pred_type = query->preds[i].flag; 		
		

		if(pred_type == -1){ 
			/*Two relations in the predeicate*/
			rel2 = query->preds[i].tuple_2.rel;
			id_rel2 = query->rels[rel2];
			num_tup2 = info[id_rel2].num_tup;
			printf("PREDICATE %d\n", i);
			printf("     rel1: %d rel2: %d\n", id_rel1, id_rel2);


			priority[i].value = num_tup1*num_tup2 ;
		}else{

			/*Only one relation in the predicate*/
			priority[i].value = num_tup1;

			printf("PREDICATE %d\n", i);
			printf("     rel1: %d rel2: %d\n", id_rel1, id_rel2);			

		}
		id_rel1 = -1;
		id_rel2 = -1;

	}


	for(i = 0; i < num_pred; i++){
		for(j = 0; j < num_pred; j++){
			if(priority[i].value < priority[j].value){
				temp = priority[j];
				priority[j] = priority[i];
				priority[i] = temp;
			}
		}
	}

	/*Print priorities*/
	for(i=0; i < num_pred; i++){
		printf("Priorities: %d - value: %llu\n", priority[i].key, priority[i].value);
	}


}