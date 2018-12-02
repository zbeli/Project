#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

#include "str.h"
#include "result.h"
#include "kati.h"

int main(int argc, char** argv){
	if(argc!=2){
		printf("Correct syntax : ./join input_file\n");
	}

	FILE *init_fp;
	init_fp = fopen(argv[1],"r");
	if(init_fp==NULL){
		printf("Cannot open init file\n");
		exit(0);
	}


	FILE *rel_fp;
	char cur_rel_file[25];
	char* line = NULL;
	size_t len=0;
	ssize_t read;
	rel_list_head list;
	uint64_t rows;
	uint64_t columns;

	list.count=0;
	list.first = NULL;
	while( (read=getline(&line, &len, init_fp)) != -1){

		sprintf(cur_rel_file, "./small/%s", line);
		cur_rel_file[strlen(cur_rel_file)-1] = '\0';

		rel_fp = fopen(cur_rel_file,"r");
		if(rel_fp==NULL){
			printf("Cannot open file %s\n",cur_rel_file);
			exit(0);
		}
		fread(&rows, sizeof(rows), 1, rel_fp);
		fread(&columns, sizeof(columns), 1, rel_fp);
		fclose(rel_fp);

		printf("%s - ",cur_rel_file);
		printf("%" PRIu64 " - %" PRIu64 "\n",rows,columns);
		insert_rel(&list, columns, rows);
	}	

	fclose(init_fp);

	print_list(&list);
	free_list(&list);
    printf("End of Program.\n");
	return 0;
}

