#include "utils.h"
#include "result.h"

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
	for(int i=0 ; i<info[rel].num_tup ; i++){
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

		printf("rel:%lu col:%lu %lu\n", query->cols_to_print[i].rel, query->cols_to_print[i].col, sum);
	}
}

void update_results(result *result_lists, result *tmp_list1, uint64_t index_1, result *tmp_list2, uint64_t index_2, result* res){
	if(tmp_list2 == NULL){
		printf("Update_Results FILTER\n");

		result *combined_result;
		//result_init(combined_result);

		relation relR, relS;
		if (result_lists[index_1].start_list!=NULL){
			list_to_rel_with_indexes(&relR, &result_lists[index_1]);
			list_to_rel_with_indexes(&relS, tmp_list1);

/*for (int i = 0; i < relR.num_tuples; ++i){
	printf("%d %d %d\n",i,relR.tuples[i].key,  relR.tuples[i].payload);									//	DEBUG
}
for (int i = 0; i < relS.num_tuples; ++i){
	printf("%d %d %d\n",i,relS.tuples[i].key,  relS.tuples[i].payload);
}*/

			combined_result = RadixHashJoin(&relR, &relS);
	printf("combined_result list_size %d\n",combined_result->list_size);

			update_interlists(result_lists, combined_result, tmp_list1, index_1, NULL, -1);
		}
		else{
			result_init(&result_lists[index_1]);
			copy_result(&result_lists[index_1], tmp_list1);
			return;
		}
	}
	else if((tmp_list2 != NULL)){
		printf("Update_Results JOIN\n");

		result *combined_result;

		if( result_lists[index_1].start_list==NULL && result_lists[index_2].start_list==NULL){
			result_init(&result_lists[index_1]);
			result_init(&result_lists[index_2]);
			copy_result(&result_lists[index_2], tmp_list2);
			copy_result(&result_lists[index_1], tmp_list1);
		}
		else if(result_lists[index_1].start_list!=NULL  && result_lists[index_2].start_list==NULL){
/*			relation relR, relS;
			list_to_rel_with_indexes(&relR, &result_lists[index_1]);
printf("result index1 counter %d 	relR counter %d\n",result_lists[index_1].counter, relR.num_tuples);
			list_to_rel_with_indexes(&relS, tmp_list1);
printf("result index2 counter %d 	relS counter %d\n",tmp_list1->counter, relS.num_tuples);
	calculate_sum(tmp_list1, &temp_q, info, 1, 0);
			combined_result = RadixHashJoin(&relR, &relS);	
	printf(" _______________combined_result list_size %d counter%d\n",combined_result->list_size, combined_result->counter);*/		
			update_interlists(result_lists, res, tmp_list1, index_1,tmp_list2, index_2);
/*			free_result(combined_result);
						combined_result->start_list=NULL;
			combined_result->list_size=0;
			combined_result->counter=0;*/
		}
	}

	printf("END update_results\n");
}

void update_interlists(result *result_lists, result *combined_result, result *tmp_list1, uint64_t index_1, result *tmp_list2, uint64_t index_2){
	result temp_lists[4];
	for (int i = 0; i < 4; ++i){
		if( result_lists[i].start_list!=NULL){
			printf("%d\n", i);
			result_init(&temp_lists[i]);
			copy_result(&temp_lists[i], &result_lists[i]);

			free_result(&result_lists[i]);
			result_lists[i].start_list=NULL;
			result_lists[i].list_size=0;
			result_lists[i].counter=0;

			result_init(&result_lists[i]);
			// print_result(&temp_lists[i]);
		}
		else{
			temp_lists[i].start_list=NULL;
		}
	}

	struct node *current_node;
	current_node = combined_result-> start_list;
	int * temp = current_node->buffer_start;	

	struct node *current_node_inner;
	int * temp_inner;

	int row;
 	int buffer;		int offset;

	if(tmp_list2==NULL){		//	filter

		for (int i = 0; i < combined_result->list_size; ++i){   	
			while((void*)temp < current_node->buffer){
				for (int j = 0; j < 4; ++j){
					if (temp_lists[j].start_list==NULL){
						continue;	
					}
					int index = *temp;
					buffer = index/BUFFER;		offset = index%BUFFER;

					current_node_inner = temp_lists[j].start_list;
					for(int k=0; k<buffer ; k++){
						printf("PARADEIGMA ME POLLOUS BUFFER\n");
						if(current_node_inner->next!=NULL){
							current_node_inner =current_node_inner->next;
						}
					}
					temp_inner = (current_node_inner->buffer_start)+offset*sizeof(int);	// ama alla3oume ta int kai auta 


					row = *temp_inner;
					insert_inter(row, &result_lists[j]);
					//print_result(&result_lists[j]);

				}
								//return;
				// temp=temp+1;
	    		 temp=temp+2;
			}
			if(current_node->next != NULL){
				current_node = current_node->next;
				temp = current_node->buffer_start;
			}
		}
		printf("combined_result counter %d\n", (combined_result->counter)/2);
		for (int i = 0; i < 4; ++i){
			if( result_lists[i].start_list!=NULL){
				printf("Result %d counter %d\n",i,result_lists[i].counter);
			}
		}
	}
	else{
		result_init(&result_lists[index_2]);
		int row_2;
		current_node = tmp_list1-> start_list;
		temp = current_node->buffer_start;
		for (int i = 0; i < tmp_list1->list_size; ++i){   	
			while((void*)temp < current_node->buffer){
				for (int j = 0; j < 4; ++j){
					if (temp_lists[j].start_list==NULL || j==index_2 || j==index_1){
						continue;	
					}
					

					
				}
								//return;
				// temp=temp+1;
	    		 temp=temp+1;
			}
			if(current_node->next != NULL){
				current_node = current_node->next;
				temp = current_node->buffer_start;
			}
		}

		copy_result(&result_lists[index_1], tmp_list1);
		copy_result(&result_lists[index_2], tmp_list2);
		// result_init(&result_lists[index_2]);
		// int row_2;

/*		current_node = combined_result-> start_list;
		temp = current_node->buffer_start;
		for (int i = 0; i < combined_result->list_size; ++i){   	
			while((void*)temp < current_node->buffer){
				//printf("%d\n",i );
				int index = *(temp+1);
				buffer = index/BUFFER;		offset = index%BUFFER;

				current_node_inner = tmp_list2->start_list;
				for(int k=0; k<buffer ; k++){
					printf("PARADEIGMA ME POLLOUS BUFFER\n");
					if(current_node_inner->next!=NULL){
						current_node_inner =current_node_inner->next;
					}
				}
				temp_inner = (current_node_inner->buffer_start)+offset*sizeof(int);	// ama alla3oume ta int kai auta 

				row_2 = *temp_inner;
				insert_inter(row_2, &result_lists[index_2]);

				temp=temp+2;
			}
			if(current_node->next != NULL){
				current_node = current_node->next;
				temp = current_node->buffer_start;
			}
		}*/

		printf("combined_result counter %d\n", (combined_result->counter)/2);
		for (int i = 0; i < 4; ++i){
			if( result_lists[i].start_list!=NULL){
				printf("Result %d counter %d\n",i,result_lists[i].counter);
			}
		}

	}
}

void item_exists(struct result * result, int row, struct result * dest){
	// printf("\t\t ITEM_EXISTS\n");
	int i;
	uint64_t *col_ptr;
	int flag_exists = 0;
	struct node *current_node;
	current_node = result -> start_list;

	int* temp = current_node->buffer_start;


	for(i = 0; i < result -> list_size; i++){
		while((void*)temp < current_node->buffer){

			if(*temp == row){
				insert_inter(row, dest);			
			}
			temp = temp + 1;    //keep searching
		}
		/*go to next node of the list*/
		if(current_node -> next != NULL){
			current_node = current_node->next;
			temp = current_node -> buffer_start;
		}
	}
}

void copy_result(result *dest, result *source){
	int i, j;
	struct node *current_node;
	current_node = source-> start_list;
	int * temp = current_node->buffer_start;

    j = 0;
	for(i = 0; i < source->list_size; i++){
    
    	while((void*)temp < current_node->buffer){
    		insert_inter(*temp,dest);
    		temp=temp+1;
		}

		if(current_node->next != NULL){
			current_node = current_node->next;
			temp = current_node->buffer_start;
		}
    }	
}

void create_relation(struct relation* rel, struct file_info *info, int rel_id, uint64_t column){
	/*Create relation from file*/
	// printf("Create relation from file!\n");
	int i,j;
	uint64_t *col_ptr; /*pointer to the column of the relation*/

    rel->num_tuples = info[rel_id].num_tup;
    col_ptr = info[rel_id].col_array[column];

    printf("REL: %d COL: %lu rows: %d \n", rel_id, column, rel->num_tuples);
    
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

void list_to_rel_with_indexes(struct relation* rel, struct result* result){

	int i, j;
	struct node *current_node;
	current_node = result -> start_list;
	int * temp = current_node->buffer_start;

	rel -> num_tuples = result -> counter;

	rel -> tuples = (struct tuple*)malloc(rel->num_tuples*sizeof(struct tuple));
    j = 0;
	for(i = 0; i < result -> list_size; i++){
    
    	while((void*)temp < current_node->buffer){

		    rel -> tuples[j].key = j;   					//!!!!!!!!!!!!!!!!!!!!!??
	    	rel -> tuples[j].payload = *temp;
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
    printf("\t\t\t REL: %d, COLUMN: %lu SUM: %lu\n", rel_key, column, sum);
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
		printf("Priorities: %d - value: %lu\n", priority[i].key, priority[i].value);
	}


}