#include "parse.h"

void information_storing(struct file_info *info, uint64_t * data, struct file_stats * fstats){
	int i, j;
	uint64_t num_col = info -> num_col;
	uint64_t num_tup = info -> num_tup;

	info -> col_array = (uint64_t**)malloc((num_col)*sizeof(uint64_t*));
	for(i = 0; i < num_col; i++){
		info -> col_array[i] = (data + i*(info->num_tup) + 2);
	}

    fstats -> min_elem = (uint64_t*)malloc((num_col)*sizeof(uint64_t));
    fstats -> max_elem = (uint64_t*)malloc((num_col)*sizeof(uint64_t));
    uint64_t min, max, *tmp;
    for(i = 0; i < num_col; i++){
    	min = *(info -> col_array[i]);
    	max = *(info -> col_array[i]);
        tmp = info -> col_array[i];
    	for(j = 0; j < num_tup; j++){
            if(*(tmp + j) > max){
            	max = *(tmp + j);
            }
            if(*(tmp + j) < min){
            	min = *(tmp + j);
            }
    	}
    	fstats -> min_elem[i] = min;
    	fstats -> max_elem[i] = max;
    }
}

void print_query_info(struct query_info* query){
	printf("Sxeseis %d\n", query->rel_count);
	for(int i=0 ; i<query->rel_count ; i++){
		printf("rel:%d\n",query->rels[i]);
	}
	printf("--------------------\n");
	printf("Predicates %d\n", query->pred_count);
	for(int i=0 ; i<query->pred_count ; i++){
		if(query->preds[i].value==-1){
			// printf("rel1:%d col1:%lu - operator:%c - rel2:%d col2:%lu\n",query->preds[i].tuple_1.rel, query->preds[i].tuple_1.col, query->preds[i].op , query->preds[i].tuple_2.rel, query->preds[i].tuple_2.col);
			printf("rel1:%d col1:%llu - operator:%c - rel2:%d col2:%llu\n",query->preds[i].tuple_1.rel, query->preds[i].tuple_1.col, query->preds[i].op , query->preds[i].tuple_2.rel, query->preds[i].tuple_2.col);
		}
		else{
			// printf("rel1:%d col1:%lu - operator:%c - value:%lu\n",query->preds[i].tuple_1.rel, query->preds[i].tuple_1.col, query->preds[i].op , query->preds[i].value);
			printf("rel1:%d col1:%llu - operator:%c - value:%llu\n",query->preds[i].tuple_1.rel, query->preds[i].tuple_1.col, query->preds[i].op , query->preds[i].value);
		}
	}
	printf("--------------------\n");
	printf("Provoles %d\n", query->cols_count);
	for(int i=0 ; i<query->cols_count ; i++){
		// printf("rel:%d col:%lu\n", query->cols_to_print[i].rel, query->cols_to_print[i].col);
		printf("rel:%d col:%llu\n", query->cols_to_print[i].rel, query->cols_to_print[i].col);
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