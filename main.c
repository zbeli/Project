#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <math.h>
#include <time.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "str.h"
#include "result.h"
#include "utils.h"
#include "parse.h"
#include "query.h"

// #define PATH "/home/zisis/Desktop/submission/submission/workloads/small/"
#define PATH "/home/panos/Desktop/small/"

int main(void){
   	int i;
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


	struct stat buf; //struct thats keep information of a file
	int file_no = -1;


	/*Storing relations in memory*/
	for(i = 0; i < num_of_files; i++){
		strcat(fl, files[i]);
		if((file = fopen(fl, "rb")) == NULL){
			printf("Error opening the file 2!\n");
			exit(-1);
		}
		strcpy(fl,PATH);

		file_no = fileno(file); //return file descriptor of file
		fstat(file_no, &buf);	//return file information inside buf

		//store the relation into the memory
		data[i] = (uint64_t*)mmap(NULL, buf.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, file_no, 0);
		// data[i] = (uint64_t*)mmap(NULL, 80, PROT_READ, MAP_PRIVATE | MAP_POPULATE, file_no, 0);

		//keep metadata for the relations
		info[i].num_tup = *data[i];
		// info[i].num_tup = 10;
		info[i].num_col = *(data[i]+1);
		create_col_array(&info[i], data[i]);

		printf("\n%s\tTuples: %lu\t -Columns: %lu\n", files[i], info[i].num_tup, info[i].num_col);

		fclose(file);
	}

	printf("\t\t\t+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
	uint64_t *col = info[6].col_array[1];
	for(i = 0; i < 10; i++){
		printf("%lu\n", *(col + i));
	}
	printf("\t\t\t+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");

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
int query_count=0;
	while ((read = getline(&query, &len, work_fp)) !=-1 ){
		if(strcmp(query,"F\n")==0){		// stamataei otan diavasei F
			break;
		}
		query_count++;
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
		temp_q.rels = (int*)malloc(relations_count*sizeof(int));
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
			if(i==pred_count-1){
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
		printf("\n");

		if(query_count==9){
			calculate_query(&temp_q,info);			//
		}

		field=0;
		relations_count=1;
		columns_to_print_count=1;
		pred_count=1;

	}
	fclose(work_fp);

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