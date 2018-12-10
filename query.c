#include "query.h"

void calculate_query(struct query_info *temp_q,  struct file_info* info){
	printf("==============================================================================\n");
	int num_rel = temp_q->rel_count; //number of predicates in the query
	int num_pred = temp_q->pred_count; //number of predicates in the query


	int rel_1=-1, rel_2=-1;
	uint64_t col_1, col_2; // !!!!! unint64_t or int
	int predicate_type;

	struct priority prior[num_pred]; //priority ekteleshs predicate
	struct result tmp_list1; // create_interlist
	struct result tmp_list2; // create_interlist

	/*arxikopoihsh twn endiameswn domwn*/
	int i;
	for(i = 0; i < num_rel; i++){
		result_lists[i].start_list = NULL;
	}

    sum_result.start_list = NULL;

    result_init(&sum_result);

	struct predicate pred;
	struct relation rel_R;
	struct relation rel_S;

	result* res;
	/*For every predicate*/
	for(i = 0; i < num_pred; i++){
		printf("\t\t\t...Predicate: %d/%d...\n",i+1, num_pred);
		pred = temp_q->preds[i];
		rel_1 = temp_q->rels[pred.tuple_1.rel];
		col_1 = pred.tuple_1.col;

		/*check if list of relation 1 exists*/
		if(result_lists[pred.tuple_1.rel].start_list == NULL){
			create_relation(&rel_R, info, rel_1, col_1);	

		}else{
			/*Create relation from list*/
			create_rel_from_list(&rel_R, &(result_lists[pred.tuple_1.rel]), info, rel_1, col_1);
		}

		/*Two relations in the current predicate*/
		if(pred.flag == -1){
			rel_2 = temp_q->rels[pred.tuple_2.rel];
			col_2 = pred.tuple_2.col;

		    /*check if list of relation 2 exists*/
			if(result_lists[pred.tuple_2.rel].start_list == NULL){
    	    	create_relation(&rel_S, info, rel_2, col_2);
			}else{
				/*Create relation from list*/
                create_rel_from_list(&rel_S, &(result_lists[pred.tuple_2.rel]), info, rel_2, col_2);

			}
			printf("RELATIONS: %d %d\n", rel_1, rel_2);

			res = RadixHashJoin(&rel_R, &rel_S);

			result_init(&tmp_list1);
			result_init(&tmp_list2);
			create_interlist(res, &tmp_list1, &tmp_list2, info);



			update_results(result_lists, &tmp_list1, pred.tuple_1.rel, &tmp_list2, pred.tuple_2.rel, res, info);
			free_result(res);
			res->start_list=NULL;
			res->list_size=0;
			res->counter=0;
            free_result(&tmp_list1);
            free_result(&tmp_list2);
            tmp_list1.start_list=NULL;
            tmp_list1.list_size=0;
            tmp_list1.counter=0;
            tmp_list2.start_list=NULL;
            tmp_list2.list_size=0;
            tmp_list2.counter=0;

		    if(i == 1 ){
		    	break;}	//debug
            // break;   //debug
		} /*Two relations in the current predicate*/

		else{/*Filter type of predicate*/
            
            printf("SOUROTIRIIIII => rel: %d col: %lu value: %lu\n", rel_1, col_1, temp_q->preds[i].value);	

            result_init(&tmp_list1);
			// filter(&rel_R, &tmp_list1, info ,rel_1, col_1, temp_q->preds[i].value, temp_q->preds[i].op);
            comparison_query(info, rel_1, col_1, temp_q->preds[i].value, temp_q->preds[i].op ,&tmp_list1);

			update_results(result_lists, &tmp_list1, temp_q->preds[i].tuple_1.rel, NULL, -1, NULL, info);

			free_result(&tmp_list1);
            tmp_list1.start_list=NULL;
            tmp_list1.list_size=0;
            tmp_list1.counter=0;			
		    
		    printf("Counter: %d\n", result_lists[pred.tuple_1.rel].counter);
		    if(i == 0 ){
		    	break;}	//debug
		}
	}	/*For every predicate*/


printf("size : %d\n", result_lists[1].counter);
	calculate_sum(&result_lists[0], temp_q, info, 6, 0);
	calculate_sum(&result_lists[0], temp_q, info, 6, 1);

	calculate_sum(&result_lists[1], temp_q, info, 1, 0);
	calculate_sum(&result_lists[1], temp_q, info, 1, 1);
	calculate_sum(&result_lists[1], temp_q, info, 1, 2);

	calculate_sum(&result_lists[2], temp_q, info, 11, 0);
	calculate_sum(&result_lists[2], temp_q, info, 11, 1);
	calculate_sum(&result_lists[2], temp_q, info, 11, 2);
}

result* comparison_query(struct file_info *info, uint64_t rel, uint64_t col, int value, char comp_op, result *results){
	result_init(results);
	int count=0;
	for(int i=0 ; i<info[rel].num_tup ; i++){
		if(comp_op=='='){
			if(info[rel].col_array[col][i] == value){
				count++;
				insert_inter(i, results);			
			}
		}
		else if(comp_op=='>'){
			if(info[rel].col_array[col][i] > value){
				count++;
				insert_inter(i, results);		
			}
		}
		else if(comp_op=='<'){
			if(info[rel].col_array[col][i] < value){
				count++;
				insert_inter(i, results);					
			}
		}
	}
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