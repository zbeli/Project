#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#include"query_selection.h"


void QueryOptimization(qinfo * query, finfo *info, fstats *stats, int *qselect){
    int numRelations = query -> rel_count;
    
    struct Connections relCon[numRelations]; //Connections between the relations in the query
    create_connections(relCon, query);       //Create Connections for the relations in the query
    // print_connections(relCon, query);        //Print the Connections of the query

    char relations_set[numRelations];
    for (int i = 0; i < numRelations; ++i){
        relations_set[i] = i+'0';
    }

    //In case of 2 - No need of Best Tree if a filter exists
    if(query->pred_count == 2){
        int selection[2] = {0,1};
            if(query->preds[1].flag != -1){ //filter exists
                selection[0]=1;
                selection[1]=0;
            }
        for (int i = 0; i < query->pred_count; ++i){
            qselect[i] = selection[i];
        }
        return;
    }
    

    //Best Tree creation
    int treesize = (int)pow(2, numRelations);
    BestTree tree;
    tree.tree_size = treesize;
    tree.query = query;
    tree.con = relCon;
    tree.nodes = (tree_node**)calloc(treesize, sizeof(tree_node*));

    //Init BestTree
    for (int i = 0; i < numRelations; ++i){
    	char str[2]="\0";
    	str[0] = relations_set[i];

        int n = hash(str, tree.tree_size);

        nodedata* data = InitStats(&stats[getRelId(query, i)], &info[getRelId(query, i)], 1);

        //bitmap init
        set_bit(data->relsmap, i);
        for (int j = 0; j < 4; ++j){
            if(i!=j)
            	clear_bit(data->relsmap, j);
        }
        
        data->cost = 0;
        data->pred = NULL;
        data->numpred = 0;
        for (int i = 0; i < 4; ++i)
            data->selection[i]=-1;
        
        TreeInsert(&tree, str, data);
    }

    tree_node* besttree[4]={NULL};

    //Filters first of all!
    int i;
    for (i = 0; i < query->pred_count; ++i){
        if(query->preds[i].flag == -1)  continue;
        int rel = query->preds[i].tuple_1.rel;
        char key[2]="\0";
        key[0] = rel+'0';
        tree_node* cnode = TreeSearch(&tree, key);
        cnode->data->pred = &(query->preds[i]);
        if(cnode->data->pred->op == '>'){
            GreaterFilterEstimation(cnode);

        }else if(cnode->data->pred->op == '<'){
            LessFilterEstimation(cnode);

        }else if(cnode->data->pred->op == '='){
            EqualFilterEstimation(cnode);

        }
        for (int j = 0; j < query->rel_count; ++j){
            tree.nodes[j]->data->selection[tree.nodes[j]->data->numpred] = i;
            tree.nodes[j]->data->numpred++;                                    
        }
        break;
    }

    int selection[query->pred_count];
    
    /*Dynamic Algorithm*/
    for (int i = 1; i < numRelations; ++i){

        if(i == numRelations - 1) break;

        int subSetNum = getSubsetNum(numRelations, i);
        Subset* subs = (Subset*)malloc(subSetNum*sizeof(Subset));

        getSubsets(subs, relations_set, numRelations, i); 


        //FOR EVERY SUBSET
        for(int j = 0; j < subSetNum; j++){                  
            int n = hash(subs[j].key, tree.tree_size);
            tree_node* current_tree = TreeSearch(&tree, subs[j].key);
            if(current_tree == NULL){
                continue;
            }

            //for every relation not existing in S
            for (int rj = 0; rj < numRelations; ++rj){
                //Check if relation isnt in the S (Rj not in S)
            	if(check_bit(current_tree->data->relsmap, rj) == 1)
                    continue;
                if(connected(&relCon[rj], current_tree, numRelations) != 1)
                    continue;
                
                //Create New Tree
                char str[2]="\0";
                str[0] = relations_set[rj];
                int n2 = hash(str, tree.tree_size);

                CreateJoinTree(&tree, current_tree, tree.nodes[n2]);

                char newkey[5];
                strcpy(newkey, current_tree->key);
                strcat(newkey, tree.nodes[n2]->key);
                tree_node* newtree = TreeSearch(&tree , newkey);
                if(newtree == NULL) {
                    continue;
                }

                if(besttree[i]==NULL || besttree[i]->data->cost > newtree->data->cost){
                    besttree[i] = newtree;
                }
            }
            current_tree = NULL;
        }
        free(subs);
    }/*Dynamic Algorithm*/

    //add the last join if not in the tree
    int flag = 0;
    for (int i = 0; i < query->pred_count; ++i){
        for (int j = 0; j < besttree[numRelations-2]->data->numpred; ++j){
            if(besttree[numRelations-2]->data->selection[j] == i){
                flag = 1;
                break;
            }
        }
        if(flag == 0){
            besttree[numRelations-2]->data->selection[besttree[numRelations-2]->data->numpred] = i;
            besttree[numRelations-2]->data->numpred++; 
        }
        flag=0;
    }

    for (int i = 0; i < besttree[numRelations-2]->data->numpred; ++i){
        selection[i] = besttree[numRelations-2]->data->selection[i];
    }
    for (int i = 0; i < query->pred_count; ++i){
        qselect[i] = selection[i];
    }

    // Free
    for (int i = 0; i < tree.tree_size; ++i){
        if(tree.nodes[i] == NULL) continue; 
        if(strlen(tree.nodes[i]->key) == 1){
                free(tree.nodes[i]->data->stats->min_elem);
                free(tree.nodes[i]->data->stats->max_elem);
                free(tree.nodes[i]->data->stats->d);
        }else{               
        // }else if(strlen(tree.nodes[i]->key) == 2){
            for (int j = 0; j < 2; ++j){
                free(tree.nodes[i]->data->stats[j].min_elem);
                free(tree.nodes[i]->data->stats[j].max_elem);
                free(tree.nodes[i]->data->stats[j].d);                
            }
        }
        // free(tree.nodes[i]->data);
    }
    for (int i = 0; i < tree.tree_size; ++i){
        if(tree.nodes[i] == NULL) continue; 
        free(tree.nodes[i]->data);
        free(tree.nodes[i]);
    }
    free(tree.nodes);

    return;
}



void CreateJoinTree(BestTree *tree, tree_node *oldtree, tree_node* newtree){

    char newkey[5];
    strcpy(newkey, oldtree->key);
    strcat(newkey, newtree->key);
 
    int newtree_index = hash(newkey, tree->tree_size);
    
    nodedata * newdata; 
    newdata = (nodedata*)malloc(sizeof(nodedata));

    //Init maps of the new tree
    for (int i = 0; i < 4; ++i){
        if(check_bit(oldtree->data->relsmap, i) == 1 || check_bit(newtree->data->relsmap, i) == 1){
            set_bit(newdata->relsmap, i);
        }else{
            clear_bit(newdata->relsmap, i);
        }

    }

    newdata->pred = (struct predicate*)RelatedPredicate(tree, oldtree, newtree, tree->query);
    newdata->numpred = 0;
    if(newdata->pred != NULL){
        for (int i = 0; i < oldtree->data->numpred; ++i){
            newdata->selection[i] = oldtree->data->selection[i];
        }

        newdata->numpred = oldtree->data->numpred;
        int newpred = FindPred(newdata->pred, tree->query);

        newdata->selection[newdata->numpred] = newpred;
        newdata->numpred++;

        JoinEstimation(tree, oldtree, newtree, newdata);
        TreeInsert(tree, newkey, newdata);
    }else{
        free(newdata);
    }
}


void JoinEstimation(BestTree* tree, tree_node *ltree, tree_node *rtree, nodedata* newdata){
    int numrels = strlen(ltree->key) + strlen(rtree->key);

    //in case of 3 relations find the rel of ltree that we need
    int treeindex = 0;
    int count_rel = -1;
    if(numrels == 3){
        for (int i = 0; i < 4; ++i){
            if(check_bit(ltree->data->relsmap, i) == 1){
                count_rel++;
                //find the connected relation
                if(connectedRels(tree->con, i, atoi(rtree->key))){  
                    treeindex = i;
                    break;
                }
            }
        }
        treeindex = count_rel;
    }

    struct file_stats *newstats;
    newstats = (struct file_stats*)malloc(2*sizeof(struct file_stats)); 

    uint64_t num_col = ltree->data->stats[treeindex].num_col;


    newstats[0].num_col = num_col;
    newstats[0].min_elem = (uint64_t*)malloc((num_col)*sizeof(uint64_t));
    newstats[0].max_elem = (uint64_t*)malloc((num_col)*sizeof(uint64_t));
    newstats[0].d = (uint64_t*)malloc((num_col)*sizeof(uint64_t));

    num_col = rtree->data->stats->num_col;
    newstats[1].num_col = num_col;
    newstats[1].min_elem = (uint64_t*)malloc((num_col)*sizeof(uint64_t));
    newstats[1].max_elem = (uint64_t*)malloc((num_col)*sizeof(uint64_t));
    newstats[1].d = (uint64_t*)malloc((num_col)*sizeof(uint64_t));

    uint64_t left_colId = newdata->pred->tuple_1.col;
    uint64_t right_colId = newdata->pred->tuple_2.col;

    uint64_t lA = ltree->data->stats[treeindex].min_elem[left_colId];
    uint64_t uA = ltree->data->stats[treeindex].max_elem[left_colId];
    uint64_t fA = ltree->data->stats[treeindex].f;
    uint64_t dA = ltree->data->stats[treeindex].d[left_colId];
    
    if(dA == 0) dA=1;

    uint64_t lB = rtree->data->stats->min_elem[right_colId];
    uint64_t uB = rtree->data->stats->max_elem[right_colId];
    uint64_t fB = rtree->data->stats->f;
    uint64_t dB = rtree->data->stats->d[right_colId];

    if(dB == 0) dB=1;

    uint64_t newfA, newdA, newfB, newdB;
    uint64_t n = uA-lA+1;

    if(lA<lB){
            newstats[0].min_elem[left_colId] = lB;
            newstats[1].min_elem[right_colId] = lB;
    }else{
            newstats[0].min_elem[left_colId] = lA;                
            newstats[1].min_elem[right_colId] = lA;                
    }
    if(uA<uB){
            newstats[0].max_elem[left_colId] = uA;
            newstats[1].max_elem[right_colId] = uA;
    }else{
            newstats[0].max_elem[left_colId] = uB;
            newstats[1].max_elem[right_colId] = uB;                            
    }

    newfA = newfB = fA*fB/n;
    newdA = newdB = dA*dB/n;
    if(newfA == 0) newfA = 1;
    if(newdA == 0) newdA = 1;

    newstats[0].f = newfA;
    newstats[1].f = newfB;

    newstats[0].d[left_colId] = newdA;
    newstats[1].d[right_colId] = newdB;

    num_col = newstats[0].num_col;
    for (int i = 0; i < num_col; ++i){
        if(i == left_colId) continue;
        newstats[0].min_elem[i] = ltree->data->stats[treeindex].min_elem[i];
        newstats[0].max_elem[i] = ltree->data->stats[treeindex].max_elem[i];

        newstats[0].f = newfA;
        uint64_t dC = ltree->data->stats[treeindex].d[i];
        uint64_t base = 1-newdA/dA;
        uint64_t exp = ltree->data->stats[treeindex].f/dC;


        newstats[0].d[i] = dC*(1-pow(base, exp));
        if(newstats[0].d[i] == 0)
            newstats[0].d[i] = 1;
    }

    num_col = newstats[1].num_col;
    for (int i = 0; i < num_col; ++i){
        if(i == right_colId) continue;
        newstats[1].min_elem[i] = rtree->data->stats->min_elem[i];
        newstats[1].max_elem[i] = rtree->data->stats->max_elem[i];
        newstats[1].f = newfA;
        uint64_t dC = rtree->data->stats->d[i];
        uint64_t base = 1-newdB/dB;
        uint64_t exp = rtree->data->stats->f/dC;
        newstats[1].d[i] = dC*(1-pow(base, exp));
        if(newstats[1].d[i] == 0)
            newstats[1].d[i] = 1;
    }
    
    newdata->stats = newstats;
    newdata->cost = newfA;
}

void GreaterFilterEstimation(tree_node *node){

    uint64_t  k = node->data->pred->value;        //value of filter
    uint64_t  colId = node->data->pred->tuple_1.col;

    //check if k < lA
    if(k < node->data->stats->min_elem[colId]){
        k = node->data->stats->min_elem[colId];
    }
    
    uint64_t newmin, newmax, newf, newd;  //new values
    uint64_t uA , lA, dA, fA;             //old values
    uA = node->data->stats->max_elem[colId];
    lA = node->data->stats->min_elem[colId];
    dA = node->data->stats->d[colId];
    fA = node->data->stats->f;

    newmin = k;
    newmax = node->data->stats->max_elem[colId];
    newd = ((newmax-newmin)/(uA-lA))*dA;
    if(newd == 0)
        newd = 1;
    newf = ((newmax-newmin)/(uA-lA)*fA);
    if(newf == 0)
        newf = 1;
    //update rest of the columnns(l and U remain the same in this case)
    uint64_t  num_col = node->data->stats->num_col;
    for (int i = 0; i < num_col; ++i){
        if(i == colId) continue;
        uint64_t dc = node->data->stats->d[i];
        if(dc == 0)
            dc=1;
        node->data->stats->d[i] = dc*(1-pow((1-newf/fA), fA/dc));
        if(node->data->stats->d[i] == 0)
            node->data->stats->d[i]=1;
    }
    //Update column A
    node->data->stats->min_elem[colId] = newmin;
    node->data->stats->max_elem[colId] = newmax;
    node->data->stats->d[colId] = newd;
    node->data->stats->f = newf;

}

void LessFilterEstimation(tree_node *node){

    uint64_t  k = node->data->pred->value;  //value of filter
    uint64_t  colId = node->data->pred->tuple_1.col;

    //check if k > uA
    if(k > node->data->stats->max_elem[colId]){
        k = node->data->stats->max_elem[colId];
    }

    uint64_t newmin, newmax, newf, newd;  //new values
    uint64_t uA , lA, dA, fA;             //old values
    uA = node->data->stats->max_elem[colId];
    lA = node->data->stats->min_elem[colId];
    dA = node->data->stats->d[colId];
    fA = node->data->stats->f;

    newmin = node->data->stats->min_elem[colId];;
    newmax = k;
    newd = ((newmax-newmin)/(uA-lA))*dA;
    if(newd == 0) 
        newd = 1;
    newf = ((newmax-newmin)/(uA-lA)*fA);
    if(newf == 0) 
        newf = 1;
    
    //update rest of the columnns(l and U remain the same in this case)
    uint64_t  num_col = node->data->stats->num_col;
    for (int i = 0; i < num_col; ++i){
        if(i == colId) continue;
        uint64_t dc = node->data->stats->d[i];
        if(dc == 0) {
            dc = 1;
            node->data->stats->d[i]=1;
        }
        node->data->stats->d[i] = dc*(1-pow((1-newf/fA), fA/dc));

        if(node->data->stats->d[i] == 0) 
            node->data->stats->d[i]=1;
    }
   //Update column A
    node->data->stats->min_elem[colId] = newmin;
    node->data->stats->max_elem[colId] = newmax;
    node->data->stats->d[colId] = newd;
    node->data->stats->f = newf;
}

void EqualFilterEstimation(tree_node *node){

    uint64_t  k = node->data->pred->value;        //value of filter
    uint64_t  colId = node->data->pred->tuple_1.col;

    //Update stats of the column that involves in the predicate
    if(k > node->data->stats->max_elem[colId]){
        node->data->stats->d[colId] = 1;
        node->data->stats->f = 1;
        return;
    }

    //new values 
    uint64_t newmin, newmax, newf, newd;
    newmin = newmax = k;
    newd = 1;
    newf = node->data->stats->f/(node->data->stats->d[colId]);

    uint64_t  num_col = node->data->stats->num_col;
    uint64_t  f = node->data->stats->f;

    //update rest of the columnns (l and u are remain the same in this case)
    for (int i = 0; i < num_col; ++i){
        if(i == colId) continue;
        uint64_t dc = node->data->stats->d[i];
        if(dc == 0){
            node->data->stats->d[i]=1;
            dc=1;
        }
        node->data->stats->d[i] = dc*(1-pow((1-newf/f), f/dc));
        if(node->data->stats->d[i] == 0){
            node->data->stats->d[i] = 1;
        }
    }
    //Update column A
    node->data->stats->min_elem[colId] = newmin;
    node->data->stats->max_elem[colId] = newmax;
    node->data->stats->d[colId] = newd;
    node->data->stats->f = newf;

}

void* RelatedPredicate(BestTree* tree, tree_node* ltree, tree_node* rtree, qinfo* query){
    int rel1 = atoi(ltree->key);
    int rel2 = atoi(rtree->key);
    if(strlen(ltree->key)>1){
    
        for (int i = 0; i < strlen(ltree->key); ++i){
            if(connectedRels(tree->con, ltree->key[i]-'0', rel2) == 1){
                rel1 = ltree->key[i]-'0';
            }
        }
    }
    for (int i = 0; i < query->pred_count; ++i){
        if(query->preds[i].tuple_1.rel == rel1 && query->preds[i].tuple_2.rel == rel2)
            return &(query->preds[i]);
    }
    return NULL;
}

void getSubsets(Subset * subs, char *relations_set, int set_size, int current_size){ 
    unsigned int pow_set_size = pow(2, set_size); 
    int i, j;
    int counter=0;
  
    for(i = 0; i < pow_set_size; i++){
      char s[5]="";
      for(j = 0; j < set_size; j++){ 
          
          if(i & (1<<j)) {
            int len = strlen(s);
            s[len] = relations_set[j];
            s[len+1] = '\0';
          }
       }
       if(strlen(s) == current_size){
           strcpy(subs[counter].key, s);
           counter++;
       }
    }
}

nodedata* InitStats(fstats *stats, finfo * info, int numS){
    int num_col;
    nodedata * data; 
    data = (nodedata*)malloc(sizeof(nodedata));

    data->stats = (struct file_stats*)malloc(numS*sizeof(struct file_stats));

    for (int j = 0; j < numS; ++j){
        
        num_col = info -> num_col;

        data->stats[j].f = stats->f;
        data->stats[j].num_col = stats->num_col;

        data->stats[j].min_elem = (uint64_t*)malloc((num_col)*sizeof(uint64_t));
        data->stats[j].max_elem = (uint64_t*)malloc((num_col)*sizeof(uint64_t));
        data->stats[j].d = (uint64_t*)malloc((num_col)*sizeof(uint64_t));

        for (int i = 0; i < num_col; ++i){
            data->stats[j].min_elem[i] = stats->min_elem[i];
            data->stats[j].max_elem[i] = stats->max_elem[i];
            data->stats[j].d[i] = stats->d[i];
        }
    }
    return data;
}

void TreeInsert(BestTree* tree, char* key, nodedata *data) {
	tree_node* newnode = (tree_node*)malloc(sizeof(tree_node));
	strcpy(newnode->key, key);
	newnode->data = data;
    int index = hash(key, tree->tree_size);

    tree_node* current_node = tree->nodes[index];
    int i = 1;
    while (current_node != NULL) {
        index++;
        current_node = tree->nodes[index];
    } 
    tree->nodes[index] = newnode;
}

tree_node* TreeSearch(BestTree* tree, char* key){
    int index = hash(key, tree->tree_size);

    int i=1;
    while(tree->nodes[index] != NULL){
        if(strcmp(tree->nodes[index]->key, key) == 0){
            return tree->nodes[index];
        }
        index++;

    }
    return NULL;
}

int FindPred(struct predicate* ptr, qinfo* query){
    for (int i = 0; i < query->pred_count; ++i){
        if(&(query->preds[i]) == ptr){
            return i;
        }
    }
    return -1;
}


void printTree(BestTree* tree){
    printf("%d #####################   TREE   ##########################\n", __LINE__);
    for (int i = 0; i < tree->tree_size; ++i){
        if(tree->nodes[i] != NULL)
            printf("%d: %s Cost: %llu\n",i, tree->nodes[i]->key, tree->nodes[i]->data->cost);
            // printf("%d: %s Cost: %lu\n",i, tree->nodes[i]->key, tree->nodes[i]->data->cost);
    }
    printf("############################################################\n");
}
int getSubsetNum(int numRelations, int subset_size){
    int num_of_subsets = 0;
    for (int i = 0; i < (int)pow(2, numRelations); ++i){
        int count = 0;
        int n = i;
        while(n){
            count += n & 1;
            n >>= 1;
        }
        if(count == subset_size)
            num_of_subsets++;
    }
    return num_of_subsets;
}

int hash(char* s,  int size) {
    long hash = 0;
    int base = 163;
    const int len_s = strlen(s);
    for (int i = 0; i < len_s; i++) {
        hash += (long)pow(base, len_s - (i+1)) * s[i];
        hash = hash % size;
    }
    return (int)hash;
}


/*Fucntions for the connections between the relations in the query*/
int connected(struct Connections *relCon, struct tree_node* S, int numRelations){
    for(int i = 0; i < numRelations; i++){
        if(check_bit(S->data->relsmap, i) == 1){
            if(check_bit(relCon->connectormap, i) == 1)
                return 1;           
        }
    }
    return 0;
}

int connectedRels(struct Connections *relCon, int rel1, int rel2){
    if(check_bit(relCon[rel1].connectormap, rel2) == 1)
        return 1;
    else return 0;
}


void create_connections(struct Connections *relCon, qinfo * query){
    for (int i = 0; i < query -> rel_count; ++i){
        for (int j = 0; j < query -> rel_count; ++j)
            clear_bit(relCon[i].connectormap, j);
    }
    for (int i = 0; i < query -> pred_count; ++i){
        if(query->preds[i].flag != -1)
            continue;
        int rel1 = query->preds[i].tuple_1.rel;
        int rel2 = query->preds[i].tuple_2.rel;

        // create connections between the relations
        set_bit(relCon[rel1].connectormap, rel2);
        set_bit(relCon[rel2].connectormap, rel1);

    }
}

void print_connections(struct Connections *relCon, qinfo * query){
    printf("------- Connections -----------\n");
    for (int i = 0; i < query -> rel_count; ++i){
        printf("rel: %d > ", i);
        for (int j = 0; j < query -> rel_count; ++j){
            if(check_bit(relCon[i].connectormap, j) == 1)
                printf("%d ", j);
        } 
        printf("\n");
    }
    printf("------- Connections -----------\n");
}


/*bit manipulation functions*/
void set_bit(unsigned char bitv[], int n) {
    bitv[n / CHAR_BIT] |= 1 << (n % CHAR_BIT);
}

void clear_bit(unsigned char bitv[], int n) {
    bitv[n / CHAR_BIT] &= ~(1 << (n % CHAR_BIT));
}
int check_bit(unsigned char bitv[], int n) {
    if ((bitv[n / CHAR_BIT] & (1 << (n % CHAR_BIT))) != 0) {
        return 1;
    }
    else
        return 0;
}

void print_bit_array(unsigned char *bitv, int bit) {
    if (check_bit(bitv, bit) == 1) {
        printf("1");
    }
    else {
        printf("0");
    }
}
