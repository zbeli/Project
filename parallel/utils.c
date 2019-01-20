#include "utils.h"

void relation_similarity(relation *relR, relation * relS, result *result_lists, finfo *info, qinfo *query, int current_pred){
    result temp_lists[4];
	for (int i = 0; i < 4; ++i){
		if( result_lists[i].start_list != NULL){
			result_init(&temp_lists[i]);
			copy_result(&temp_lists[i], &result_lists[i]);

			free_result(&result_lists[i]);
			result_lists[i].start_list=NULL;
			result_lists[i].list_size=0;
			result_lists[i].counter=0;

			result_init(&result_lists[i]);
		}
		else{
			temp_lists[i].start_list=NULL;
		}
	}


	int index = query -> preds[current_pred].tuple_1.rel;
	int index2 = query -> preds[current_pred].tuple_2.rel;
	int rel_id = query -> rels[index];
	uint64_t col1 = query -> preds[current_pred].tuple_1.col;
	uint64_t col2 = query -> preds[current_pred].tuple_2.col;

    if(temp_lists[index].start_list == NULL && temp_lists[index2].start_list != NULL){
        result_init(&result_lists[index]);

    }else if(temp_lists[index].start_list != NULL && temp_lists[index2].start_list == NULL){
        result_init(&result_lists[index2]);

    }else if(temp_lists[index].start_list == NULL && temp_lists[index2].start_list == NULL){
        result_init(&result_lists[index]);
        result_init(&result_lists[index2]);

    }
    relation relat_redev[3];

    int z = 0;

    for(int i = 0; i < relR -> num_tuples; i ++){
        for(int j = 0; j < relS -> num_tuples; j++){
        	if(relR -> tuples[i].payload == relS -> tuples[j].payload){
        		insert_inter(relR -> tuples[i].key, &result_lists[index]);
        		insert_inter(relS -> tuples[j].key, &result_lists[index2]);		//maybe reconsidering this
        	}
        }
    }

    for(int r = 0 ; r < 4 ; r++){    
    	if(r == index || r == index2 ||  temp_lists[r].start_list == NULL){continue;}
        list_to_rel_with_indexes(&relat_redev[z], &temp_lists[r]);

        for(int i = 0; i < relR -> num_tuples; i ++){
            for(int j = 0; j < relS -> num_tuples; j++){
            	if(relR -> tuples[i].payload == relS -> tuples[j].payload){
                    insert_inter(relat_redev[z].tuples[i].payload, &result_lists[r]);	//j
            	}
            }
        }
        free(relat_redev[z].tuples);
        z++;
    }
}

void update_existing_interlists(relation *relR, relation * relS, result *result_lists, finfo *info, qinfo *query, int current_pred){
    result temp_lists[4];
	for (int i = 0; i < 4; ++i){
		if( result_lists[i].start_list != NULL){
			result_init(&temp_lists[i]);
			copy_result(&temp_lists[i], &result_lists[i]);

			free_result(&result_lists[i]);
			result_lists[i].start_list=NULL;
			result_lists[i].list_size=0;
			result_lists[i].counter=0;

			result_init(&result_lists[i]);
		}
		else{
			temp_lists[i].start_list=NULL;
		}
	}

	int index_1 = query -> preds[current_pred].tuple_1.rel;
	int index_2 = query -> preds[current_pred].tuple_2.rel;

    relation relat_redev[2];

	int counter = 0;
	int temp_array[2];

	/*Find lists in the interdata*/
	for(int i = 0; i < 4; i++){
	    if(temp_lists[i].start_list != NULL && i != index_1 && i != index_2){
	    	temp_array[counter] = i;
		    counter++;
	    }
	}

	if(counter == 1){
		list_to_rel_with_indexes(&relat_redev[0], &temp_lists[temp_array[counter-1]]);
		for(int i = 0; i < relR -> num_tuples; i ++){
	        for(int j = 0; j < relS -> num_tuples; j++){
	        	if(relR -> tuples[i].payload == relS -> tuples[j].payload){
	        		insert_inter(relR -> tuples[i].key, &result_lists[index_1]);
	        		insert_inter(relS -> tuples[j].key, &result_lists[index_2]);
	        		insert_inter(relat_redev[0].tuples[i].payload, &result_lists[temp_array[counter-1]]);
	        		
	        	}
	        }
	    }
        free(relat_redev[0].tuples);

	}else if(counter == 2){
		list_to_rel_with_indexes(&relat_redev[0], &temp_lists[temp_array[counter-2]]);
        list_to_rel_with_indexes(&relat_redev[1], &temp_lists[temp_array[counter-1]]);
		for(int i = 0; i < relR -> num_tuples; i ++){
	        for(int j = 0; j < relS -> num_tuples; j++){
	        	if(relR -> tuples[i].payload == relS -> tuples[j].payload){
	        		insert_inter(relR -> tuples[i].key, &result_lists[index_1]);
	        		insert_inter(relS -> tuples[j].key, &result_lists[index_2]);
	        		insert_inter(relat_redev[0].tuples[i].payload, &result_lists[temp_array[counter-2]]);
    	            insert_inter(relat_redev[1].tuples[i].payload, &result_lists[temp_array[counter-1]]);
	        	}
	        }
	    }
        free(relat_redev[0].tuples);
        free(relat_redev[1].tuples);
	}else if(counter == 0){
		for(int i = 0; i < relR -> num_tuples; i ++){
	        for(int j = 0; j < relS -> num_tuples; j++){
	        	if(relR -> tuples[i].payload == relS -> tuples[j].payload){
	        		insert_inter(relR -> tuples[i].key, &result_lists[index_1]);
	        		insert_inter(relS -> tuples[j].key, &result_lists[index_2]);	        		
	        	}
	        }
	    }
    }

	for (int i = 0; i < 4; ++i)
	    if(temp_lists[i].start_list != NULL)
			free_result(&temp_lists[i]);   
}


void update_results_filter(result *result_lists, result *tmp_list1, int index, finfo * info, qinfo *query, int pred_index, struct thrd_pool *thread_pool){
	result *combined_result;
	relation relR, relS;
	if (result_lists[index].start_list != NULL){
		list_to_rel_with_indexes(&relR, &result_lists[index]);
		list_to_rel_with_indexes(&relS, tmp_list1);

		combined_result = RadixHashJoin(&relR, &relS, thread_pool);

		update_interlists_filter(result_lists, combined_result, tmp_list1, index, info, query, pred_index);
        free(relR.tuples);
        free(relS.tuples);        
	}
	else{
		result_init(&result_lists[index]);
		copy_result(&result_lists[index], tmp_list1);
		return;

	}
}

void update_interlists_filter(result *result_lists, result *combined_result, result *tmp_list1, int index, finfo *info, qinfo *temp_q, int pred_index){
	result temp_lists[4];
	for (int i = 0; i < 4; ++i){
		if( result_lists[i].start_list!=NULL){
			result_init(&temp_lists[i]);
			copy_result(&temp_lists[i], &result_lists[i]);

			free_result(&result_lists[i]);
			result_lists[i].start_list=NULL;
			result_lists[i].list_size=0;
			result_lists[i].counter=0;

			result_init(&result_lists[i]);
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

	for (int i = 0; i < combined_result->list_size; ++i){   	
		while((void*)temp < current_node->buffer){
			for (int j = 0; j < 4; ++j){
				if (temp_lists[j].start_list == NULL){
					continue;	
				}
				int index = *temp;
				buffer = index / BUFFER;	
				offset = index % BUFFER;

				current_node_inner = temp_lists[j].start_list;

				for(int k = 0; k < buffer ; k++){
					if(current_node_inner->next!=NULL){
						current_node_inner =current_node_inner->next;
					}
				}
				temp_inner = (current_node_inner->buffer_start) + offset*sizeof(int);	// ama alla3oume ta int kai auta 


				row = *temp_inner;
				insert_inter(row, &result_lists[j]);
			}
    		temp = temp + 2;
		}
		if(current_node->next != NULL){
			current_node = current_node->next;
			temp = current_node->buffer_start;
		}
	}	
}

void update_results(result *result_lists, result *tmp_list1, int index_1, result *tmp_list2, int index_2, result* res, struct file_info* info, struct query_info *temp_q, int pred_index){

	result *combined_result;

	if( result_lists[index_1].start_list == NULL && result_lists[index_2].start_list == NULL){
		result_init(&result_lists[index_1]);
		result_init(&result_lists[index_2]);
		copy_result(&result_lists[index_2], tmp_list2);
		copy_result(&result_lists[index_1], tmp_list1);

	}
	else if(result_lists[index_1].start_list != NULL  && result_lists[index_2].start_list == NULL){

	    result_init(&result_lists[index_2]); 
		update_interlists(result_lists, res, tmp_list1, index_1, tmp_list2, index_2, info, temp_q, pred_index);	
	}
	else if(result_lists[index_1].start_list == NULL  && result_lists[index_2].start_list != NULL){

        result_init(&result_lists[index_1]); 
		update_interlists(result_lists, res, tmp_list2, index_2, tmp_list1, index_1, info, temp_q, pred_index);	//testing

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

void updateDifferCol(relation *relR, relation *relS, result * result_lists, finfo *info, qinfo *query, int current_pred){
    result temp_lists[4];
	for (int i = 0; i < 4; ++i){
		if( result_lists[i].start_list != NULL){
			result_init(&temp_lists[i]);
			copy_result(&temp_lists[i], &result_lists[i]);

			free_result(&result_lists[i]);
			result_lists[i].start_list=NULL;
			result_lists[i].list_size=0;
			result_lists[i].counter=0;

			result_init(&result_lists[i]);
		}
		else{
			temp_lists[i].start_list=NULL;
		}
	}
	int index_1 = query -> preds[current_pred].tuple_1.rel;
	int index_2 = query -> preds[current_pred].tuple_2.rel;
    relation relat_redev[2];    
    int z = 0;

    for(int i = 0; i < relR -> num_tuples; i ++){
    	if(relR -> tuples[i].payload == relS -> tuples[i].payload){

    		insert_inter(relR -> tuples[i].key, &result_lists[index_1]);
    		insert_inter(relS -> tuples[i].key, &result_lists[index_2]);
    	}  
    }

    for(int r = 0 ; r < 4 ; r++){    
    	if(r == index_1 || r == index_2 || temp_lists[r].start_list == NULL){continue;}

        list_to_rel_with_indexes(&relat_redev[z], &temp_lists[r]);

        for(int j = 0; j < relS -> num_tuples; j++){
        	if(relR -> tuples[j].payload == relS -> tuples[j].payload){
                insert_inter(relat_redev[z].tuples[j].payload, &result_lists[r]);
        	}
        }
        free(relat_redev[z].tuples);
        z++;
    }
    for (int i = 0; i < 4; ++i)
	    if(temp_lists[i].start_list != NULL)
			free_result(&temp_lists[i]);
}
void create_relation(struct relation* rel, struct file_info *info, int rel_id, uint64_t column){
	/*Create relation from file*/
	int i,j;
	uint64_t *col_ptr; /*pointer to the column of the relation*/

    rel->num_tuples = info[rel_id].num_tup;
    col_ptr = info[rel_id].col_array[column];
    
    rel->tuples = (struct tuple*)malloc(rel->num_tuples*sizeof(struct tuple));

    for (i = 0; i < rel -> num_tuples; i++){
    	rel -> tuples[i].key = i;
    	rel -> tuples[i].payload = *(col_ptr+i);
    }
}

/*Creation of relation from list(key to mapped relations)*/
void create_rel_from_list(struct relation* rel, struct result* result, struct file_info *info, int rel_id, uint64_t column){

	int i, j;
	uint64_t *col_ptr;
	struct node *current_node;
	current_node = result -> start_list;
	int * temp = current_node->buffer_start;

	rel -> num_tuples = result -> counter;
    col_ptr = info[rel_id].col_array[column];

	rel -> tuples = (struct tuple*)malloc(rel->num_tuples*sizeof(struct tuple));
    j = 0;
	for(i = 0; i < result -> list_size; i++){
    
    	while((void*)temp < current_node->buffer){
            //key to mapped relations
		    rel -> tuples[j].key = *temp;
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

	rel -> tuples = (struct tuple*)malloc((rel -> num_tuples) * sizeof(struct tuple));
    j = 0;
	for(i = 0; i < result -> list_size; i++){
    
    	while((void*)temp < current_node->buffer){

		    rel -> tuples[j].key = j;
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

/*Creation of the two lists from the result of RHJ.*/
void create_interlist(struct result *result, struct result* list1, struct result* list2, struct file_info* info){

	int i;
	struct node *current_node;
	current_node = result -> start_list;
	int row_1, row_2;
	int* temp = current_node -> buffer_start;

	for(i = 0; i < result -> list_size; i++){

		while((void*)temp < current_node -> buffer ){
			row_1 = *(int*)temp;
			row_2 = *(int*)(temp+1);
			temp = temp + 2;

			insert_inter(row_1, list1);
			insert_inter(row_2, list2);
		}

		if(current_node -> next != NULL){
			current_node = current_node -> next;
			temp = current_node -> buffer_start;
		}
	}	
}

void update_interlists(result *result_lists, result *combined_result, result *tmp_list1, int index_1, result *tmp_list2, int index_2, struct file_info* info, struct query_info *temp_q, int pred_index){
	result temp_lists[4];
	for (int i = 0; i < 4; ++i){
		if( result_lists[i].start_list != NULL){
			result_init(&temp_lists[i]);
			copy_result(&temp_lists[i], &result_lists[i]);

			free_result(&result_lists[i]);
			result_lists[i].start_list=NULL;
			result_lists[i].list_size=0;
			result_lists[i].counter=0;

			result_init(&result_lists[i]);
		}
		else{
			temp_lists[i].start_list=NULL;
		}
	}
    relation new_relat, old_relat, insert_relat;
    relation relat_redev[2];

    if(&temp_lists[index_1].start_list != NULL){
	    list_to_rel_with_indexes(&old_relat, &temp_lists[index_1]);	
    }
    else {
	    list_to_rel_with_indexes(&old_relat, &temp_lists[index_2]);	
    }

	list_to_rel_with_indexes(&new_relat, tmp_list1);
	list_to_rel_with_indexes(&insert_relat, tmp_list2);


	int counter = 0;
	int temp_array[2];
	/*Find lists in the interdata*/
	for(int i = 0; i < 4; i++){
	    if(temp_lists[i].start_list != NULL && i != index_1 && i != index_2){
	    	temp_array[counter] = i;
		    counter++;
	    }
	}

	uint64_t numOfTuples = 0;
	if(new_relat.num_tuples%2 == 0){
		numOfTuples = new_relat.num_tuples;
	}else{
		numOfTuples = new_relat.num_tuples-1;	
	}

    if(counter == 1){
                list_to_rel_with_indexes(&relat_redev[0], &temp_lists[temp_array[counter-1]]);
    	        for(int i = 0; i < numOfTuples; i=i+2){
    	        	insert_result(relat_redev[0].tuples[new_relat.tuples[i].payload].payload, relat_redev[0].tuples[new_relat.tuples[i+1].payload].payload ,&result_lists[temp_array[counter-1]]);
    	        	insert_result(old_relat.tuples[new_relat.tuples[i].payload].payload, old_relat.tuples[new_relat.tuples[i+1].payload].payload, &result_lists[index_1]);
    	        	insert_result(insert_relat.tuples[i].payload, insert_relat.tuples[i+1].payload, &result_lists[index_2]);
    	        }
    	        if(new_relat.num_tuples%2 != 0){
    	        	insert_inter(relat_redev[0].tuples[new_relat.tuples[numOfTuples].payload].payload, &result_lists[temp_array[counter-1]]);
    	        	insert_inter(old_relat.tuples[new_relat.tuples[numOfTuples].payload].payload, &result_lists[index_1]);
    	        	insert_inter(insert_relat.tuples[numOfTuples].payload, &result_lists[index_2]);
    	        }

    	        free(relat_redev[0].tuples);
    }else if(counter == 2){
                list_to_rel_with_indexes(&relat_redev[0], &temp_lists[temp_array[counter-2]]);
                list_to_rel_with_indexes(&relat_redev[1], &temp_lists[temp_array[counter-1]]);

    	        for(int i = 0; i < numOfTuples; i=i+2){
                    insert_result(relat_redev[0].tuples[new_relat.tuples[i].payload].payload, relat_redev[0].tuples[new_relat.tuples[i+1].payload].payload, &result_lists[temp_array[counter-2]]);
                    insert_result(relat_redev[1].tuples[new_relat.tuples[i].payload].payload, relat_redev[1].tuples[new_relat.tuples[i+1].payload].payload, &result_lists[temp_array[counter-1]]);
                    insert_result(old_relat.tuples[new_relat.tuples[i].payload].payload, old_relat.tuples[new_relat.tuples[i+1].payload].payload, &result_lists[index_1]);
                    insert_result(insert_relat.tuples[i].payload, insert_relat.tuples[i+1].payload, &result_lists[index_2]);
    	        }
      	        if(new_relat.num_tuples%2 != 0){
    	        	insert_inter(relat_redev[0].tuples[new_relat.tuples[numOfTuples].payload].payload, &result_lists[temp_array[counter-2]]);
    	        	insert_inter(relat_redev[1].tuples[new_relat.tuples[numOfTuples].payload].payload, &result_lists[temp_array[counter-1]]);    	        	
    	        	insert_inter(old_relat.tuples[new_relat.tuples[numOfTuples].payload].payload, &result_lists[index_1]);
    	        	insert_inter(insert_relat.tuples[numOfTuples].payload, &result_lists[index_2]);
    	        }
    	        free(relat_redev[0].tuples);
    	        free(relat_redev[1].tuples);
    }else if(counter == 0){
    	        for(int i = 0; i < numOfTuples; i=i+2){
    	        	insert_result(old_relat.tuples[new_relat.tuples[i].payload].payload, old_relat.tuples[new_relat.tuples[i+1].payload].payload, &result_lists[index_1]);
    	        	insert_result(insert_relat.tuples[i].payload, insert_relat.tuples[i+1].payload, &result_lists[index_2]);
    	        }
     	        if(new_relat.num_tuples%2 != 0){
    	        	insert_inter(old_relat.tuples[new_relat.tuples[numOfTuples].payload].payload, &result_lists[index_1]);
    	        	insert_inter(insert_relat.tuples[numOfTuples].payload, &result_lists[index_2]);
    	        }
    }
    free(new_relat.tuples);
	free(old_relat.tuples);
	free(insert_relat.tuples);

	for (int i = 0; i < 4; ++i)
	    if(temp_lists[i].start_list != NULL)
			free_result(&temp_lists[i]);

}/*End of update_interlists*/

/*Creation of relation from list(key to interdata)*/
void list_to_relation(struct relation* rel, struct result* result, struct file_info *info, int rel_id, uint64_t column){

	int i, j;
	uint64_t *col_ptr;
	struct node *current_node;
	current_node = result -> start_list;
	int * temp = current_node->buffer_start;

	rel -> num_tuples = result -> counter;
    col_ptr = info[rel_id].col_array[column];

	rel -> tuples = (struct tuple*)malloc(rel->num_tuples*sizeof(struct tuple));
    j = 0;
	for(i = 0; i < result -> list_size; i++){
    
    	while((void*)temp < current_node->buffer){
            //key to interdata
		    rel -> tuples[j].key = j;
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

int getRelId(qinfo * query, int rel){
   return(query -> rels[rel]);
}

