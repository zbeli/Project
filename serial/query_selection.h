#ifndef QUERY_SELECTION_H
#define QUERY__SELECTION_H

#include "utils.h"
#include "parse.h"

/*Tree*/
typedef struct BestTree{
    int tree_size;
    qinfo* query;               //pointer to the query 
    struct Connections* con;    //pointer to the struct of connections
    struct tree_node ** nodes;  //nodes of the tree

}BestTree;

/*Data of tree nodes*/
typedef struct nodeData{
    unsigned char relsmap[1];    //bitvector thats keeps the relations in set
    struct file_stats *stats;    //stats of the relations in the node
    struct predicate* pred;      //pointer to the repsective predicate
    int selection[4];            //order of predicates selection
    int numpred;                 //number of preds currenlty 
    uint64_t cost;               //cost of current tree

}nodedata;

/*Node of the tree*/
typedef struct tree_node{
    char key[10];
    struct nodeData *data;
    
}tree_node;

/*Bitmap for storing the connection between the relations in the query*/
struct Connections{
	unsigned char connectormap[1];
};


typedef struct Subset{
  char key[10];
}Subset;



void QueryOptimization(qinfo * query, finfo *info, fstats *stats, int* qselect);

/*Functions for statistics estimation of the new tree*/
void JoinEstimation(BestTree* tree, tree_node *lefttree, tree_node *righttree, nodedata *data);
void GreaterFilterEstimation(tree_node *node);
void LessFilterEstimation(tree_node *node);
void EqualFilterEstimation(tree_node *node);

/*Tree Creation*/
void CreateJoinTree(BestTree* tree, tree_node *oldtree, tree_node* newrel);
/*Insertion in tree*/
void TreeInsert(BestTree* tree, char* key, nodedata *data);
/*Find tree with the corresponding key*/
tree_node* TreeSearch(BestTree* tree, char* key);
/*Function for printing the BestTree*/
void printTree(BestTree* tree);


/*Function for finding the corresponing predicate*/
void* RelatedPredicate(BestTree* tree, tree_node* ltree, tree_node* rtree, qinfo* query);
/*Find index of corresponding predicate*/
int FindPred(struct predicate* ptr, qinfo* query);

/*Initialization of statistics */
nodedata* InitStats(fstats *stats, finfo * info, int numS);

/*Function for returning the number of subsets*/
int getSubsetNum(int numRelations, int subset_size);
/*Return the subsets of size current_size*/
void getSubsets(Subset * subs, char *relations_set, int set_size, int current_size);


/*Create connections for the relations in the query*/
void create_connections(struct Connections *relCon, qinfo * query);
/*Function for checking if a connection between specific relations exists*/
int connected(struct Connections *relCon, struct tree_node* S, int numRelations);
/*Print Connections between relations in the query*/
void print_connections(struct Connections *relCon, qinfo * query);
/*Connection checking between rel1 and rel2*/
int connectedRels(struct Connections *relCon, int rel1, int rel2);


/*Hash function for the tree*/
int hash(char* s, int size);


/*bit manipulation functions*/
void set_bit(unsigned char bitv[], int n);
void clear_bit(unsigned char bitv[], int n);
int check_bit(unsigned char bitv[], int n);
void print_bit_array(unsigned char *bitv, int bit);

#endif /*query_selection.h*/