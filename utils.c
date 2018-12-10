#include "utils.h"

void update_results(result *result_lists, result *tmp_list1, uint64_t index_1, result *tmp_list2, uint64_t index_2, result* res, struct file_info* info){
	if(tmp_list2 == NULL){
		printf("Update_Results FILTER\n");
		result *combined_result;
		relation relR, relS;
		if (result_lists[index_1].start_list!=NULL){
			list_to_rel_with_indexes(&relR, &result_lists[index_1]);
			list_to_rel_with_indexes(&relS, tmp_list1);

			combined_result = RadixHashJoin(&relR, &relS);

			update_interlists(result_lists, combined_result, tmp_list1, index_1, NULL, -1, info);
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
			update_interlists(result_lists, res, tmp_list1, index_1,tmp_list2, index_2, info);
		}
	}

	printf("END update_results\n");
}

void update_interlists(result *result_lists, result *combined_result, result *tmp_list1, uint64_t index_1, result *tmp_list2, uint64_t index_2, struct file_info* info){
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

				}
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

        relation relat1, old_relat1, relat6;
        for(int r=0 ; r<4 ; r++){
        	if(r==index_2 || r==index_1 || temp_lists[r].start_list==NULL){continue;}
printf("============================== %d\n",r);

			create_rel_from_list_1(&old_relat1, &temp_lists[index_1], info, 1, 0);
// create_rel_from_list(&old_relat1, &temp_lists[index_1], info, 1, 0);
			create_rel_from_list(&relat1, tmp_list1, info, 1, 0);


        	  //       create_rel_from_list_1(&old_relat1, &temp_lists[1], info, 1, 0);
        	  //       create_rel_from_list(&relat1, tmp_list1, info, 1, 0);
        	 
        	        //R6
        	        create_rel_from_list(&relat6, &temp_lists[r], info, 6, 0);
        	 		printf("R6: %d - R1:%d - NEW_R1: %d\n", temp_lists[r].counter, temp_lists[index_1].counter, tmp_list1->counter);
        	        printf("R6: %d - R1:%d - NEW_R1: %d\n", relat6.num_tuples, old_relat1.num_tuples, relat1.num_tuples);
        	    int count=0;
        	        for(int i = 0; i < relat1.num_tuples; i ++){
        	            for(int j = 0; j < old_relat1.num_tuples; j++){
        	                if(relat1.tuples[i].key == old_relat1.tuples[j].key){
        	                	count++;
        	                    insert_inter(relat6.tuples[old_relat1.tuples[j].payload].key, &result_lists[r]);
        	                    // insert_inter(relat6.tuples[j].key, &result_lists[r]);
        	                }
        	            }
        	        }
        	        printf("%d \n",count);

        
        }
        copy_result(&result_lists[index_1], tmp_list1);
        copy_result(&result_lists[index_2], tmp_list2);
		printf("\ncombined_result counter %d\n", (combined_result->counter)/2);
		for (int i = 0; i < 4; ++i){
			if( result_lists[i].start_list!=NULL){
				printf("Result %d counter %d\n",i,result_lists[i].counter);
			}
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
	int i,j;
	uint64_t *col_ptr; /*pointer to the column of the relation*/

    rel->num_tuples = info[rel_id].num_tup;
    col_ptr = info[rel_id].col_array[column];

    printf("REL: %d COL: %lu rows: %d \n", rel_id, column, rel->num_tuples);
    
    rel->tuples = (struct tuple*)malloc(rel->num_tuples*sizeof(struct tuple));

    for (i = 0; i < rel -> num_tuples; i++){
    	rel -> tuples[i].key = i;
    	rel -> tuples[i].payload = *(col_ptr+i);
    }
}

void create_rel_from_list_1(struct relation* rel, struct result* result, struct file_info *info, int rel_id, uint64_t column){
	int i, j;
	uint64_t *col_ptr; /*pointer to the column of the relation*/
	struct node *current_node;
	current_node = result -> start_list;
	int * temp = current_node->buffer_start;

relation *t_rel;
t_rel = (relation*)malloc(sizeof(relation));
	t_rel -> num_tuples = 0;
    col_ptr = info[rel_id].col_array[column];

	t_rel -> tuples = (struct tuple*)malloc((result->counter)*sizeof(struct tuple));
    j = 0;
    int flag=0;
    int counter=0;
	for(i = 0; i < result -> list_size; i++){
    
    	while((void*)temp < current_node->buffer){

    		for(int k=0; k<t_rel->num_tuples; k++){
    			if(t_rel -> tuples[k].key == *temp){
    				flag = 1;
    				break;
    			}
    		}
    		if(flag ==0 ){
    			t_rel -> tuples[j].key = *temp;   					//!!!!!!!!!!!!!!!!!!!!!??
    			t_rel -> tuples[j].payload = counter;
    			t_rel -> num_tuples++;

    			j++;
    		}

    		flag=0;
            temp = temp + 1;
	    	counter++;
		}

		if(current_node->next != NULL){
			current_node = current_node->next;
			temp = current_node->buffer_start;
		}
    }

    rel->num_tuples = 0;
    rel->tuples = (struct tuple*)malloc(t_rel->num_tuples*sizeof(struct tuple));
    for(int k=0; k<t_rel->num_tuples; k++){
    	rel->tuples[k].key = t_rel -> tuples[k].key;   					//!!!!!!!!!!!!!!!!!!!!!??
    	rel->tuples[k].payload = t_rel -> tuples[k].payload;
    	rel-> num_tuples++;
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

void create_interlist(struct result *result, struct result* list1, struct result* list2, struct file_info* info){

	// printf("\t\t ----------------CREATE INTERLIST------------------\n");
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