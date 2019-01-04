#include "query.h"

void calculate_query(struct query_info *temp_q,  struct file_info* info){
	int i;
	int num_rel = temp_q->rel_count; //number of relations in the query
	int num_pred = temp_q->pred_count; //number of predicates in the query


	int rel_1=-1, rel_2=-1;
	uint64_t col_1, col_2;

	struct priority prior[num_pred]; //priority ekteleshs predicate
	for(i = 0; i < num_pred; i++){
		prior[i].key = i;            //initialization
	}

	struct result tmp_list1; // create_interlist
	struct result tmp_list2; // create_interlist

	/*arxikopoihsh twn endiameswn domwn*/
	for(i = 0; i < num_rel; i++){
		result_lists[i].start_list = NULL;
	}

	struct predicate pred;  //current predicate in loop
	struct relation rel_R;
	struct relation rel_S;

	result* res;
    int current_pred = -1;    //key of current predicate in every loop! 
    int flag_exists = 0;

	/*For every predicate*/
	for(i = 0; i < num_pred; i++){

	    if(i != num_pred - 1){
	    	current_pred = calculate_priority(prior, temp_q, info, i);
	    }else if(i == num_pred - 1){
            current_pred = prior[0].key;
	    }
	    // printf("\t\t\t...Predicate: %d/%d...(%d)\n", i+1, num_pred, current_pred);  //debug

        //check if the same predicate exists
        if(predicate_exists(temp_q, num_pred, current_pred) == 1){
        	if(flag_exists == 1)
        	     continue;
        	flag_exists = 1;
        }
		
		pred = temp_q->preds[current_pred];

		rel_1 = temp_q->rels[pred.tuple_1.rel];
		col_1 = pred.tuple_1.col;



		/*Two relations in the current predicate*/
		if(pred.flag == -1){
			if(result_lists[pred.tuple_1.rel].start_list == NULL){
				create_relation(&rel_R, info, rel_1, col_1);
			}else{
				/*Create relation from list*/

                list_to_relation(&rel_R, &(result_lists[pred.tuple_1.rel]), info, rel_1, col_1);	
			}
			rel_2 = temp_q->rels[pred.tuple_2.rel];
			col_2 = pred.tuple_2.col;

		    /*check if list of relation 2 exists*/
			if(result_lists[pred.tuple_2.rel].start_list == NULL){
    	    	create_relation(&rel_S, info, rel_2, col_2);
			}else{
				/*Create relation from list*/
                // create_rel_from_list(&rel_S, &(result_lists[pred.tuple_2.rel]), info, rel_2, col_2);			
                // printf("tell me about it!!!\n");	//debug	
                list_to_relation(&rel_S, &(result_lists[pred.tuple_2.rel]), info, rel_2, col_2);
			}

			if(result_lists[pred.tuple_1.rel].start_list != NULL && result_lists[pred.tuple_2.rel].start_list != NULL){
				// printf("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$\n");
                if(predicate_exists(temp_q, num_pred, current_pred) == 2){
                    free(rel_R.tuples);
                    free(rel_S.tuples);

			        create_rel_from_list(&rel_R, &(result_lists[pred.tuple_1.rel]), info, rel_1, col_1); //maybe avoid that?
                    create_rel_from_list(&rel_S, &(result_lists[pred.tuple_2.rel]), info, rel_2, col_2);				

                	updateDifferCol(&rel_R, &rel_S, result_lists, info, temp_q, current_pred);
                    free(rel_R.tuples);
		            free(rel_S.tuples);
                	continue;
                }
                free(rel_R.tuples);

                printf("\t\t query.c SOS 2\n"); //debug

			    create_rel_from_list(&rel_R, &(result_lists[pred.tuple_1.rel]), info, rel_1, col_1);
          
                update_existing_interlists(&rel_R, &rel_S, result_lists, info, temp_q, current_pred);
                free(rel_R.tuples);
	            free(rel_S.tuples);               
                continue;

			}
			// same relation in the predicate

			// if(rel_1 == rel_2){ 
			// 	if(result_lists[pred.tuple_1.rel].start_list != NULL){
			// 		free(rel_R.tuples);
			//         create_rel_from_list(&rel_R, &(result_lists[pred.tuple_1.rel]), info, rel_1, col_1);
			// 	}

			// 	relation_similarity(&rel_R, &rel_S, result_lists, info, temp_q, current_pred);
   //              free(rel_R.tuples);
	  //           free(rel_S.tuples);				
			// 	continue;
			// }

			res = RadixHashJoin(&rel_R, &rel_S);
			result_init(&tmp_list1);
			result_init(&tmp_list2);
			create_interlist(res, &tmp_list1, &tmp_list2, info);

			update_results(result_lists, &tmp_list1, pred.tuple_1.rel, &tmp_list2, pred.tuple_2.rel, res, info, temp_q, current_pred);

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

			free(rel_R.tuples);
			free(rel_S.tuples);
		    	
		} /*Two relations in the current predicate*/

		else{
            /*Filter type of predicate*/

            comparison_query(info, rel_1, col_1, pred.value, pred.op , &tmp_list1);

            update_results_filter(result_lists, &tmp_list1, pred.tuple_1.rel, info, temp_q, current_pred);
			free_result(&tmp_list1);
            tmp_list1.start_list=NULL;
            tmp_list1.list_size=0;
            tmp_list1.counter=0;	
		}
	}/*For every predicate*/

    print_sum(result_lists, temp_q, info); /*Calculate sum*/
		
	/*Free result lists*/
	for(int i = 0; i < 4; i++){
	    if(result_lists[i].start_list != NULL){
	    	free_result(&result_lists[i]);
	    	result_lists[i].start_list = NULL;
	    	result_lists[i].list_size = 1;
	    	result_lists[i].counter =0 ;
	    }
	}

}/*End of Calculate Query*/

int predicate_exists(qinfo * query, int num_pred, int current_pred){
    
    //check if the same predicate exists
	struct predicate *pred = &(query -> preds[current_pred]);
    int rel1=-1, rel2=-1;
    uint64_t col1=-1, col2=-1;

    for(int i = 0; i < num_pred; i++){
    	if(i == current_pred || query -> preds[i].flag != -1 ){continue;}
    	rel1 = query -> preds[i].tuple_1.rel;
    	rel2 = query -> preds[i].tuple_2.rel;
    	col1 = query -> preds[i].tuple_1.col;
    	col2 = query -> preds[i].tuple_2.col;

    	if((rel1 == pred->tuple_1.rel) && (rel2 == pred->tuple_2.rel)){
    		if(col1 == pred->tuple_1.col && col2 == pred->tuple_2.col){
                return 1;
    		}
    	}
    	else if((rel2 == pred->tuple_1.rel && rel1 == pred->tuple_2.rel)){
    		if(col2 == pred->tuple_1.col && col1 == pred->tuple_2.col){
    			return 1;
    		}
    	}

    	if((rel1 == pred->tuple_1.rel) && (rel2 == pred->tuple_2.rel)){
    		return 2;
    	}
    	if((rel2 == pred->tuple_1.rel) && (rel1 == pred->tuple_2.rel)){
    		return 2;
    	}
    }
    return 0;
}

result* comparison_query(struct file_info *info, int rel, uint64_t col, int value, char comp_op, result *results){
	result_init(results);
	int count=0;
	for(int i = 0 ; i < info[rel].num_tup ; i++){
		if(comp_op == '='){

			if(info[rel].col_array[col][i] == value){
				count++;
				insert_inter(i, results);			
			}
		}
		else if(comp_op == '>'){
			if(info[rel].col_array[col][i] > value){
				count++;
				insert_inter(i, results);		
			}
		}
		else if(comp_op == '<'){
			if(info[rel].col_array[col][i] < value){
				count++;
				insert_inter(i, results);					
			}
		}
	}
}

int calculate_priority(struct priority *prior, qinfo *query, finfo *info, int index){
	int i, j;

	int rel1, rel2;
	int id_rel1, id_rel2;

	uint64_t rel1_value = 0, rel2_value = 0;
	struct priority temp;

	int num_pred = query->pred_count;   //number of predicates in the query
	int num_rel =  query->rel_count;    //number of relations in the query
	int pred_type;                      //type of predicate
    
    int current_pred;

	/*for every predicate in the query find the priority value*/
	for(i = 0; i < num_pred - index; i++){

        current_pred = prior[i].key;

	    rel1 = query -> preds[current_pred].tuple_1.rel;
        id_rel1 = query -> rels[rel1];
		if(result_lists[rel1].start_list == NULL){
		    rel1_value = info[id_rel1].num_tup;
		}else{
            rel1_value = result_lists[rel1].counter;
		}

		pred_type = query -> preds[current_pred].flag; 		

		if(pred_type == -1){ 
			/*Two relations in the predicate*/
			rel2 = query -> preds[current_pred].tuple_2.rel;
			id_rel2 = query -> rels[rel2];
			if(result_lists[rel2].start_list == NULL){
			    rel2_value = info[id_rel2].num_tup;
			}else{
	            rel2_value = result_lists[rel2].counter;
			}
			prior[i].value = rel1_value * rel2_value ;
		}else{

			/*Only one relation in the predicate*/
			prior[i].value = rel1_value;
		}

		id_rel1 = -1;
		id_rel2 = -1;
	}

    uint64_t min = prior[num_pred - index - 1].value;
    int pos_min = num_pred - index - 1;               //position of min

    //no cross products please!!!
    for(i = 0; i <= num_pred - index - 1; i++){
	    int pred = prior[i].key;
    	int rel1 = query -> preds[pred].tuple_1.rel;
    	int rel2 = query -> preds[pred].tuple_2.rel;

    	if(result_lists[rel1].start_list == NULL && result_lists[rel2].start_list == NULL){
    		continue;}

    	min = prior[i].value;
        pos_min = i;               
    }
    //no cross products please!!!

	for(i = 0; i <= num_pred - index - 1; i++){
        if(prior[i].value < min){
        	//no cross products please!!!
        	int pred = prior[i].key;
        	int rel1 = query -> preds[pred].tuple_1.rel;
        	int rel2 = query -> preds[pred].tuple_2.rel;

        	if(result_lists[rel1].start_list == NULL && result_lists[rel2].start_list == NULL){
        	// if(index != 0 && result_lists[rel1].start_list == NULL && result_lists[rel2].start_list == NULL){
        		continue;}
        	//no cross products please!!!
            min = prior[i].value;
            pos_min = i;
        }    
    }
    if(pos_min != num_pred - index){
    	temp = prior[num_pred - index - 1];
        prior[num_pred - index - 1] = prior[pos_min];
        prior[pos_min] = temp;
    }

    return(prior[num_pred - index - 1].key);
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
   		while((void*)temp < current_node -> buffer){

            sum = sum + *(col_ptr + *(temp));	
			temp = temp + 1;	
		}
		if(current_node -> next != NULL){
			current_node = current_node->next;
			temp = current_node->buffer_start;
		}   
    }
    
    if(sum == 0){
        printf("NULL");
        return;
    }
    printf("%llu", sum);

}

void print_sum(struct result* result, struct query_info *query, struct file_info *info){
	struct result* res;
	int rel;
	uint64_t col;
	for (int i = 0; i < query -> cols_count; ++i){
		rel = query->rels[query->cols_to_print[i].rel];
		col = query->cols_to_print[i].col;
		res = &result[query->cols_to_print[i].rel];
		calculate_sum(res, query, info, rel, col);
		printf(" ");
	}
	printf("\n");
}