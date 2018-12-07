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




#define PATH "/home/zisis/Desktop/submission/submission/workloads/small/"
// #define PATH "/home/panos/Desktop/an_logismikou/submission/workloads/small/"


void calculate_priority(struct priority *priority, struct query_info *query, struct file_info *info);
void create_relation(struct relation* rel, struct file_info *info, int rel_id, uint64_t column);
// void create_interlist(struct result *result, struct result* list1, struct result* list2, int rel1, int rel2, struct file_info* info);
void create_rel_from_list(struct relation* rel, struct result* result, struct file_info *info, int rel_id, uint64_t column);
void filter(struct relation *rel,struct result * result, struct file_info *info, int rel_id, uint64_t column, uint64_t value, char op);
void calculate_sum(struct result* result, struct query_info *query, struct file_info *info);

void create_interlist(struct result *result, struct result* list1, struct result* list2, struct query_info * query, struct file_info* info, int pred_id);


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

	result *res_comp;

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
	struct result list[num_rel]; //endiamesh domh gia relations tou predicate


	/*arxikopoihsh twn endiameswn domwn*/
	///!!!!!
	for(i = 0; i < num_rel; i++){
		result_lists[i].start_list = NULL;
		// result_init(&(result_lists[i]));
	}


	// for(i = 0; i < num_pred; i++){
	// 	printf("%d - %llu\n", prior[i].key, prior[i].value);
	// }
	struct predicate pred;
	struct relation rel_R;
	struct relation rel_S;

	/*For every predicate*/
	for(i = 0; i < num_pred; i++){
		printf("\t\t\t...Predicate: %d/%d...\n",i+1, num_pred);

		//update relations!!!!!!!!!

		// calculate_priority(prior, &temp_q, info);
		
		pred = temp_q.preds[i];
		rel_1 = temp_q.rels[pred.tuple_1.rel];
		col_1 = pred.tuple_1.col;

		/*check if list of relation 1 exists*/
		if(result_lists[pred.tuple_1.rel].start_list == NULL){
			result_init(&(result_lists[pred.tuple_1.rel]));
			create_relation(&rel_R, info, rel_1, col_1);	

		}else{
			/*Create relation from list*/
			create_rel_from_list(&rel_R, &(result_lists[pred.tuple_1.rel]), info, rel_1, col_1);

			// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			/*free the list - (reconstruction after RHJ)*/
			free_result(&(result_lists[pred.tuple_1.rel]));
			result_lists[pred.tuple_1.rel].start_list = NULL;
			
			result_init(&(result_lists[pred.tuple_1.rel]));


			// printf("%d", result_lists[pred.tuple_1.rel].list_size);			
		}

		/*Two relations in the current predicate*/
		if(pred.flag == -1){
			rel_2 = temp_q.rels[pred.tuple_2.rel];
			col_2 = pred.tuple_2.col;

		    /*check if list of relation 2 exists*/
			if(result_lists[pred.tuple_2.rel].start_list == NULL){
				result_init(&(result_lists[pred.tuple_2.rel]));
    	    	create_relation(&rel_S, info, rel_2, col_2);
			}else{
				/*Create relation from list*/
                create_rel_from_list(&rel_S, &(result_lists[pred.tuple_2.rel]), info, rel_2, col_2);
				// create_relation(&(result_lists[pred.tuple_2.rel]), info, rel_2, pred.tuple_2.rel, col_2);
							// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
				/*free the list - (reconstruction after RHJ)*/
				free_result(&(result_lists[pred.tuple_2.rel]));
				result_lists[pred.tuple_2.rel].start_list = NULL;
				// ????????????????????????????????????????????
				// result_lists[pred.tuple_2.rel].list_size = 0;
				// result_lists[pred.tuple_2.rel].counter = 0;
				result_init(&(result_lists[pred.tuple_2.rel]));
			}
            // printf("================================\n");
			// printf("REL: %d COL: %llu rows: %d \n", rel_1, col_1, relR.num_tuples);
            // printf("    -->%d\n", relR.tuples[0].payload);
            // printf("================================\n");


			printf("RELATIONS: %d %d\n", rel_1, rel_2);

			// res = RadixHashJoin(&relR, &relS);	//testing		
			res = RadixHashJoin(&rel_R, &rel_S);
			// print_result(res);
			// return -1;

			printf("\t\t %d %d\n", result_lists[pred.tuple_1.rel].counter, result_lists[pred.tuple_2.rel].counter);

			create_interlist(res, &result_lists[pred.tuple_1.rel], &result_lists[pred.tuple_2.rel], &temp_q, info, i);
// create_interlist(struct result *result, struct result* list1, struct result* list2, struct query_info * query, struct file_info* info, int pred_id);



			// create_interlist(res, &result_lists[pred.tuple_1.rel], &result_lists[pred.tuple_2.rel], rel_1, rel_2, info);


			// printf("\t\t\t\tSEGMENTATION\n");
			//TSEKARE AN YPARXOUN OI LISTES!!!!!!


	/*		print_result(&result_lists[pred.tuple_1.rel]);
			printf("=========================================================================\n");
			printf("=========================================================================\n");
			printf("=========================================================================\n");

			print_result(&result_lists[pred.tuple_2.rel]);*/


			printf("========> Elements: %d\n", res->counter);
			printf("========> Elements Rel 1: %d\n", result_lists[pred.tuple_1.rel].counter);
			printf("========> Elements Rel 2: %d\n", result_lists[pred.tuple_2.rel].counter);

			// break; //debug

			printf("________________________________________\n");
			// for(j=0; j<rel_R.num_tuples; j++){
			//  	printf("%d ", rel_R.tuples[j].payload);
			// }


			// print_result(&result_lists[pred.tuple_1.rel]);
			// printf("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
			// print_result(&result_lists[pred.tuple_2.rel]);
            
            // free(res);
		    calculate_sum(&result_lists[1], &temp_q, info);

		    if(i == 1 ){
		    	break;}	//debug
            // break;   //debug
		}
		else{/*Filter type of predicate*/
            
            printf("SOUROTIRIIIII => rel: %d value: %llu\n", rel_1, temp_q.preds[i].value);	

			filter(&rel_R,&result_lists[pred.tuple_1.rel], info ,rel_1, col_1, temp_q.preds[i].value, temp_q.preds[i].op);
		    
		    printf("List_size: %d\n", result_lists[pred.tuple_1.rel].counter);	
		}


		// printf("-->%llu , %llu\n", temp_q.preds[i].tuple_1.rel, temp_q.preds[i].tuple_1.col);

		// printf("#########: %llu \n", temp_q.rels[i]);
		// predicate with key prior[i]

	}

    // calculate_sum(&result_lists[2], &temp_q, info);


	printf("ALL OK UNTIL HERE!!!\n");

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
// struct file_info *info, int rel_id, uint64_t column)

void calculate_sum(struct result* result, struct query_info *query, struct file_info *info){
    int i;
    uint64_t sum = 0;
    uint64_t *col_ptr; /*pointer to the column of the relation*/


    struct node *current_node;
	current_node = result -> start_list;
	int * temp = current_node -> buffer_start;

    col_ptr = info[1].col_array[0];

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
    printf("\t\t\t REL: 1, COLUMN: 0 SUM: %llu\n", sum);
    printf("\t\t\t----------------------------------------------\n");

}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
void filter(struct relation *rel,struct result * result, struct file_info *info, int rel_id, uint64_t column, uint64_t value, char op){
    printf("\t\t\t--------  WELCOME TO SOUROTIRIIIII------ \n");
    int i = 0;
   	uint64_t *col_ptr; /*pointer to the column of the relation*/

    rel -> num_tuples = info[rel_id].num_tup;
    col_ptr = info[rel_id].col_array[column];

    if(op == '>'){
    	printf("Correct operator!\n");
	    for(i = 0; i < rel -> num_tuples; i++){
	        if(rel -> tuples[i].payload > value){
                insert_inter(rel->tuples[i].key, result);
	        }
	    }
    }
}
////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
 int count = 0;

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

	// printf("\t\t\t ==> %d\n", result -> list_size);
	for(i = 0; i < result -> list_size; i++){
		// printf("\t\t %d\n", i);


		while((void*)temp < current_node->buffer){
			
			// printf("%d ", *(int*)temp);
			// printf("11111111111111111111111111111111111\n");
			if(*(col_ptr + *(temp))  != value){ 
				temp = temp + 1;    //keep sarching
				// printf("\t\t###################################\n");
			}
			else{
				flag_exists = 1;    //we found it, don't insert it in the list
				// printf("\t\t$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$\n");
				// break;
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

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

// void create_interlist(struct result *result, struct result* list1, struct result* list2, int rel1, int rel2, struct file_info* info){
void create_interlist(struct result *result, struct result* list1, struct result* list2, struct query_info * query, struct file_info* info, int pred_id){

	// printf("\t\t ----------------CREATE INTERLIST------------------\n");
	int i;
	// int *ptr;
	struct node *current_node;
	current_node = result -> start_list;
	int row_1, row_2;
	int* temp = current_node -> buffer_start;

	int rel1 = query->rels[query -> preds[pred_id].tuple_1.rel];
	uint64_t col1 = query -> preds[pred_id].tuple_1.col;
	int rel2 = query->rels[query -> preds[pred_id].tuple_2.rel];
	uint64_t col2 = query -> preds[pred_id].tuple_2.col;

			// pred = temp_q.preds[i];


	int j = 0; //debug

	for(i = 0; i < result -> list_size; i++){

		while((void*)temp < current_node -> buffer ){
			// printf("+++++++++++++++++++++++++++++++++++++");
			
			row_1 = *(int*)temp;
			row_2 = *(int*)(temp+1);
			temp = temp + 2;
			// printf("\t\t00000000000000: %d %d %d\n", row_1, row_2, *temp);
			/*Check if the values exist in the respective lists*/
			item_exists(list1, row_1, info, rel1, col1);
			item_exists(list2, row_2, info, rel2, col2);

			//debug
			j++;
			// if(j == 20000){

				// break;
			// }
		}

		if(current_node -> next != NULL){
			current_node = current_node -> next;
			temp = current_node -> buffer_start;
		}
	}	
	// print_result(list1);
	// printf("\t\t\t-----------------------------------\n");
	// print_result(list2);
	printf("\t\t\t@@@@@COUNT: %d\n", count);

}

/////////////////////////////////////////////////
/////////////////////////////////////////////////
void create_rel_from_list(struct relation* rel, struct result* result, struct file_info *info, int rel_id, uint64_t column){
	int i, j;
	uint64_t *col_ptr; /*pointer to the column of the relation*/

	struct node *current_node;
	current_node = result -> start_list;
	int * temp = current_node->buffer_start;


	rel -> num_tuples = result -> counter;
    col_ptr = info[rel_id].col_array[column];

    // printf("REL: %d COL: %llu rows: %d \n", rel_id, column, rel->num_tuples);
    // printf("    -->%llu\n", *col_ptr);

	rel -> tuples = (struct tuple*)malloc(rel->num_tuples*sizeof(struct tuple));
    j = 0;
	for(i = 0; i < result -> list_size; i++){
    
    	while((void*)temp < current_node->buffer){
			// temp = temp + 1;

		    rel -> tuples[j].key = *temp;   //!!!!!!!!!!!!!!!!!!!!!
	    	printf("\t\t\t\t\t************************%llu ", *(col_ptr+j));
	    	break;
	    	//////////??????????????????????????????
	    	rel -> tuples[j].payload = *(col_ptr + *(temp));
          	// rel -> tuples[j].payload = *(col_ptr + *(temp));


            temp = temp + 1;
	    	j++;
		}

		if(current_node->next != NULL){
			current_node = current_node->next;
			temp = current_node->buffer_start;
		}
    }

}

/////////////////////////////////////////////////
///////////////////////////////////////////////////
void create_relation(struct relation* rel, struct file_info *info, int rel_id, uint64_t column){
	/*Create relation from file*/
	printf("____________________________CREATE (file)\n");
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

	printf("__________________________________\n");
	

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