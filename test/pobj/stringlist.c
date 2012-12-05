#include <string.h>
#include <stdio.h>
#include "pobj.h"

struct node {
  char *str;
  struct node *next;
};

int node_ref_fields[] = { 
  member_offset(struct node, str), 
  member_offset(struct node, next), 
  -1
};

typedef struct node Node;

Node *list = NULL;

int getline(char *string, int num) {
  if (fgets(string, num, stdin) != string) {
    return -1;
  }
  num = strlen(string);
  if (string[num-1] == '\n') {
    string[num-1] = '\0';
    return num - 1;
  } else {
    return num;
  }
}

void process(char *line) {
  int len = strlen(line);
  Node *tmp = (Node *) pobj_malloc(sizeof(Node));
  char *cpy = (char *) pobj_malloc(sizeof(char) * (len + 1));
  pobj_ref_typify(tmp, node_ref_fields);
  strcpy(cpy, line);
  pobj_update(cpy);
  pobj_set_ref(tmp, (void **) &(tmp->str), cpy);
  pobj_set_ref(tmp, (void **) &(tmp->next), list);
  pobj_static_set_ref(&list, tmp);
}

void print(void) {
  int i = 1;
  Node *tmp = list;
  printf("Entries: \n");
  while (tmp != NULL) {
    printf("%d. %s\n", i++, tmp->str);
    tmp = tmp->next;
  }
  printf("Done.\n");
}

int main(int argc, char **argv) {
/*   static Node *list = NULL; */
  char line[256];
  pobj_init(NULL);
/*   if (pobj_init() == 0) { */
    while ((getline(line, 255)) != -1) {
      process(line);
    }
/*   } */
  print();
  pobj_shutdown();
  return 0;  
}
