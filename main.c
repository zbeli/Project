#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <math.h>
#include <time.h>
#include <string.h>

#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "str.h"
#include "result.h"

#include "utils.h"




// #define PATH "/home/zisis/Desktop/submission/submission/workloads/small/"
#define PATH "/home/panos/Desktop/small/"

void filter(struct relation *rel, struct result * result, struct file_info *info, int rel_id, uint64_t column, uint64_t value, char op);


int main(void){

    int i,j;
    result* res;


	//test_case_1
	//Table R
	int r_size = 10;
	char R[10] = {'a','a','a','a','d','d','c','b','a','a'};
	int s_size = 72;
	char S[72] = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z',
		'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','e','t','u','v','w','x','y','z'};
	// for(int i=0 ; i<72 ; i++){
	// 	printf("%c ",S[i]);
	// }

    struct relation relR;
    relR.num_tuples = r_size;
    relR.tuples = (struct tuple*)malloc(relR.num_tuples*sizeof(struct tuple));
    for (i = 0; i < relR.num_tuples; i++){
    	relR.tuples[i].key = i+1;
    	relR.tuples[i].payload = R[i];
    }

    struct relation relS;
    relS.num_tuples = s_size;
    relS.tuples = (struct tuple*)malloc(relS.num_tuples*sizeof(struct tuple));
    for (i = 0; i < relS.num_tuples; i++){
    	relS.tuples[i].key = i+1;
    	relS.tuples[i].payload = S[i];
    }


    // res = RadixHashJoin(&relR, &relS);
    // print_result(res);

    // free(relR.tuples);
    // free(relS.tuples);

    // free_result(res);

    // return 0;
    /*###########################################################################*/
    /*###########################################################################*/
    /*#################                                     #####################*/
    /*#############                                             #################*/
    /*##########             WELCOME   TO   PART 2                 ##############*/
    /*#############                                             #################*/
    /*#################                                     #####################*/
    /*###########################################################################*/
    /*###########################################################################*/

    FILE *file;

	char  ch;
	// int i;
	int num_of_files = 0;
	char **files;
	char nl = '\n';
	size_t size = 5;

	char file_init[100];
	
	strcpy(file_init, PATH);
	strcat(file_init, "small.init");


	if((file = fopen(file_init, "r")) == NULL){
		printf("Error opening the file!\n");
		exit(-1);
	}

	while((ch = fgetc(file)) != EOF){
		if(ch == '\n')
			num_of_files++;
	}
	printf("Num of files: %d\n", num_of_files);	

	///////////////////////malloc checking?
	files = (char **)malloc(num_of_files * sizeof(char *));
	for(i = 0; i < num_of_files; i++){
		files[i] = (char*)malloc(size * sizeof(char));
	}
	
	fseek(file, 0, SEEK_SET); //Go to the beginning of the file
	i=0;

	char ptr[size];

	while((fgets(ptr, size, file)) != NULL){
		strcpy(files[i], ptr);
		files[i] = strtok(files[i], &nl);
		i++;
	}

	printf("FILES:\t");	
	for(i = 0; i < num_of_files; i++){
		 printf("%s ", files[i]);
	}
	printf("\n\n");

	fclose(file);

	//info about the relations
	struct file_info info[num_of_files]; 

	char fl[100];
	strcpy(fl, PATH);

	uint64_t **data; //pointers to the data addresses
	data = (uint64_t**)malloc(num_of_files * sizeof(uint64_t*));

	// uint64_t *data = (uint64_t*)mmap(NULL, buf.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, file_no, 0);

	struct stat buf; //struct thats keep information of a file
	int file_no = -1;


	/*Storing relations in memory*/
	for(i = 0; i < num_of_files; i++){
		strcat(fl, files[i]);
		// printf("%s\n", files[i]);
		//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		if((file = fopen(fl, "rb")) == NULL){
			printf("Error opening the file 2!\n");
			exit(-1);
		}
		strcpy(fl,PATH);

		file_no = fileno(file); //return file descriptor of file
		fstat(file_no, &buf);	//return file information inside buf

		//store the relation into the memory
		data[i] = (uint64_t*)mmap(NULL, buf.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, file_no, 0);

		//////////////////////////////////////////////
		//keep metadata for the relations
		info[i].num_tup = *data[i];
		info[i].num_col = *(data[i]+1);
		create_col_array(&info[i], data[i]);

		// printf("#####>%llu, %llu\n", info[i].num_tup, info[i].num_col);
		printf("\n%s\tTuples: %llu\t -Columns: %llu\n", files[i], info[i].num_tup, info[i].num_col);

		fclose(file);
	}
	//////////////////////////////
	printf("\t\t\t+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
	uint64_t *col = info[1].col_array[1];
	for(i = 0; i < 10; i++){
		printf("%llu\n", *(col + i));
	}
	printf("\t\t\t+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");

	// return - 1;

	
	// fclose(file);
/*	printf("------------------------------------------------\n");
	for(i = 0; i < num_of_files; i++){

		printf("1.%llu\t", *info[i].col_array[0]);
		printf("2.%llu\t", *info[i].col_array[1]);
		printf("3.%llu\n", *info[i].col_array[2]);

	}
	printf("------------------------------------------------\n");*/

	/////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////
    FILE *work_fp;
	char file_path[100];
	
	strcpy(file_path, PATH);
	strcat(file_path, "small.work");


	if((work_fp = fopen(file_path, "r")) == NULL){
		printf("Error opening the file 3!\n");
		exit(-1);
	}


	char *query;
	size_t len=0;
	ssize_t read;

	struct query_info temp_q;
	char *temp_rel;
	char *temp_col;
	char *c_token;
	char temp_pred[20];	// xrhsimopoieitai gia thn klhsh thw insert pred

	int query_len=0;
	int field=0;	// 0 gia ti sxesei, 1 predicates, 2 provoles
	int relations_count=1;	// poses relations xrhsimopoiountai
	int columns_to_print_count=1;	// plithos provolwn
	int pred_count=1; // plithos predicates

	result *res_comp = (result*)malloc(sizeof(result));

	while ((read = getline(&query, &len, work_fp)) !=-1 ){
		if(strcmp(query,"F\n")==0){		// stamataei otan diavasei F
			break;
		}
		query[strlen(query)-1]='\0';
		query_len = strlen(query);

		for(i=0 ; i<query_len ; i++){
			if(query[i]=='|'){
				field++;
			}
			else if(query[i]==' '){
				if(field==0){
					relations_count++;
				}
				else if(field==2){
					columns_to_print_count++;
				}
			}
			else if(query[i]=='&'){
				pred_count++;
			}
		}

		printf("%s\n",query);
		// printf("relations %d\n",relations_count);
		// printf("columns_to_print_count %d\n",columns_to_print_count);
		// printf("pred_count %d\n",pred_count);

		temp_q.rel_count = relations_count;
		temp_q.pred_count = pred_count;
		temp_q.cols_count = columns_to_print_count;
		temp_q.rels = (uint64_t*)malloc(relations_count*sizeof(uint64_t));
		temp_q.preds = (struct predicate*)malloc(pred_count*sizeof(struct predicate));
		temp_q.cols_to_print = (struct rel_col_tuple*)malloc(columns_to_print_count*sizeof(struct rel_col_tuple));

// relations
		i=0;
		temp_rel = strtok(query," ");
		// printf("(%d)%s-",i,temp_rel);
		temp_q.rels[i] = atoi(temp_rel);
		for(i=1 ; i<relations_count ; i++){
			if(i==relations_count-1){
				temp_rel = strtok(NULL,"|");
				// printf("(%d)%s\n",i,temp_rel);
			}
			else{
				temp_rel = strtok(NULL," ");
				// printf("(%d)%s-",i,temp_rel);
			}
			temp_q.rels[i] = atoi(temp_rel);
		}

// predicates
		for(i=0 ; i<pred_count ; i++){
			if(i==relations_count-1){
				temp_rel = strtok(NULL,"|");
				// printf("(%d)%s\n",i,temp_rel);
			}
			else{
				temp_rel = strtok(NULL,"&");
				// printf("(%d)%s\n",i,temp_rel);
			}
			strcpy(temp_pred, temp_rel);	// stelnw auto sthn insert logw problhmatos me strtok()
			insert_pred(&temp_q, temp_pred, i);
			
		}

// columns
		for(i=0 ; i<columns_to_print_count ; i++){
			temp_col = strtok(NULL,".");
			// printf("(%d)%s-",i,temp_col);
			temp_q.cols_to_print[i].rel = atoi(temp_col);
			temp_col = strtok(NULL," ");
			temp_q.cols_to_print[i].col  = atoi(temp_col);
			// printf("(%d)%s ",i,temp_col);
		}


		// print_query_info(&temp_q);
		printf("\n");

		// comparison_query(info,0,2,8600,'>',res_comp);
		// print_result(res_comp);
		// free_result(res_comp);

		field=0;
		relations_count=1;
		columns_to_print_count=1;
		pred_count=1;


	    //break;	//gia debbug mono gia to prwto predicate
	}
// return 0;
	fclose(work_fp);

	////////////////////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////</////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////////////////

	printf("==============================================================================\n");
	int num_rel = temp_q.rel_count; //number of predicates in the query
	int num_pred = temp_q.pred_count; //number of predicates in the query

	// int current_relation, current_column;
	int rel_1=-1, rel_2=-1;
	uint64_t col_1, col_2; // !!!!! unint64_t or int
	int predicate_type;

	struct priority prior[num_pred]; //priority ekteleshs predicate
	struct result tmp_list1; // create_interlist
	struct result tmp_list2; // create_interlist

	/*arxikopoihsh twn endiameswn domwn*/
	///!!!!!
	for(i = 0; i < num_rel; i++){
		result_lists[i].start_list = NULL;
		// result_init(&(result_lists[i]));
	}
    //////////////////////////////////////////////////////
    sum_result.start_list = NULL;	///////?????????????????????

    result_init(&sum_result);
    //////////////////////////////////////////////////////

	// for(i = 0; i < num_pred; i++){
	// 	printf("%d - %llu\n", prior[i].key, prior[i].value);
	// }
	struct predicate pred;
	struct relation rel_R;
	struct relation rel_S;


	/*For every predicate*/
	// for(i = 1; i < 2; i++){			//testing

	for(i = 0; i < num_pred; i++){
		printf("\t\t\t...Predicate: %d/%d...\n",i+1, num_pred);

		//update relations!!!!!!!!!

		// calculate_priority(prior, &temp_q, info);
		
		pred = temp_q.preds[i];
		rel_1 = temp_q.rels[pred.tuple_1.rel];
		col_1 = pred.tuple_1.col;

		/*check if list of relation 1 exists*/
		if(result_lists[pred.tuple_1.rel].start_list == NULL){
			// result_init(&(result_lists[pred.tuple_1.rel]));
			create_relation(&rel_R, info, rel_1, col_1);	

		}else{
			/*Create relation from list*/
			create_rel_from_list(&rel_R, &(result_lists[pred.tuple_1.rel]), info, rel_1, col_1);
		}

		/*Two relations in the current predicate*/
		if(pred.flag == -1){
			rel_2 = temp_q.rels[pred.tuple_2.rel];
			col_2 = pred.tuple_2.col;

		    /*check if list of relation 2 exists*/
			if(result_lists[pred.tuple_2.rel].start_list == NULL){
				// result_init(&(result_lists[pred.tuple_2.rel]));
    	    	create_relation(&rel_S, info, rel_2, col_2);
			}else{
				/*Create relation from list*/
                create_rel_from_list(&rel_S, &(result_lists[pred.tuple_2.rel]), info, rel_2, col_2);

			}

			printf("RELATIONS: %d %d\n", rel_1, rel_2);

			res = RadixHashJoin(&rel_R, &rel_S);

			result_init(&tmp_list1);
			result_init(&tmp_list2);
			create_interlist(res, &tmp_list1, &tmp_list2, &temp_q, info, i);


			update_results(result_lists, &tmp_list1, pred.tuple_1.rel, &tmp_list2, pred.tuple_2.rel);

            free_result(&tmp_list1);
            free_result(&tmp_list2);
            tmp_list1.start_list=NULL;
            tmp_list1.list_size=0;
            tmp_list1.counter=0;
            tmp_list2.start_list=NULL;
            tmp_list2.list_size=0;
            tmp_list2.counter=0;

            // free(res);

		    // calculate_sum(&result_lists[1], &temp_q, info);
            // print_result(&tmp_list2);
            //print_result(&result_lists[2]);
		    if(i == 0 ){
		    	break;}	//debug
            // break;   //debug
		} /*Two relations in the current predicate*/

		else{/*Filter type of predicate*/
            
            printf("SOUROTIRIIIII => rel: %d value: %llu\n", rel_1, temp_q.preds[i].value);	

            result_init(&tmp_list1);
			// filter(&rel_R, &tmp_list1, info ,rel_1, col_1, temp_q.preds[i].value, temp_q.preds[i].op);
            comparison_query(info, rel_1, col_1, temp_q.preds[i].value, temp_q.preds[i].op ,&tmp_list1);

            // print_result(&tmp_list1);

			update_results(result_lists, &tmp_list1, temp_q.preds[i].tuple_1.rel, NULL, -1);
			//print_result(&result_lists[temp_q.preds[i].tuple_1.rel]);

			// free_result(&tmp_list1);
   //          tmp_list1.start_list=NULL;
   //          tmp_list1.list_size=0;
   //          tmp_list1.counter=0;			
		    
		    printf("List_size: %d\n", result_lists[pred.tuple_1.rel].counter);
		    if(i == 0 ){
		    	break;}	//debug
		}
	}	/*For every predicate*/

printf("lalalalalalla\n");
	calculate_sum(&result_lists[0], &temp_q, info, 6, 0);
printf("lelelelelelelelele\n");
	// printf("ALL OK UNTIL HERE!!!\n");

	/*Free*/
	free(data);

	for(i = 0; i < num_of_files; i++){
		free(info[i].col_array);
	}
	for(i=0; i < num_of_files; i++){
		free(files[i]);
	}
	free(files);

    printf("End of Program.\n");
	return 0;
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
void filter(struct relation *rel,struct result * result, struct file_info *info, int rel_id, uint64_t column, uint64_t value, char op){
    printf("\t\t\t--------  WELCOME TO SOUROTIRIIIII  ------ \n");
    int i = 0;
   	uint64_t *col_ptr; /*pointer to the column of the relation*/

    rel -> num_tuples = info[rel_id].num_tup;
    col_ptr = info[rel_id].col_array[column];

    if(op == '<'){
	    for(i = 0; i < rel -> num_tuples; i++){
	        if(rel -> tuples[i].payload < value){
                insert_inter(rel->tuples[i].key, result);
	        }
	    }
    }
}
////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
int count = 0; 	//debug

void item_exists(struct result * result, int row, struct file_info* info, int rel, uint64_t column){
	// printf("\t\t ITEM_EXISTS\n");
	int i;
	uint64_t *col_ptr;
	int flag_exists = 0;
	struct node *current_node;
	current_node = result -> start_list;

	int* temp = current_node->buffer_start;
    col_ptr = info[rel].col_array[column];

    uint64_t value = *(col_ptr + row);

	for(i = 0; i < result -> list_size; i++){
		while((void*)temp < current_node->buffer){

			if(*temp != row){ 			
			// if(*(col_ptr + *(temp))  != value){ 
				temp = temp + 1;    //keep searching
			}
			else{

			    flag_exists = 1;    //we found it, don't insert it in the list
				return;
			}
		}
		/*go to next node of the list*/
		if(current_node -> next != NULL){
			current_node = current_node->next;
			temp = current_node -> buffer_start;
		}
	}

	/*If the element doesn't exist insert it in the list */
	if(flag_exists == 0){
		// count++;			//debug
		insert_inter(row, result);
	}
}

