#include <pthread.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "pobj.h"
#include "common.h"

#define MAX_MSG_SIZE 128

struct node {
  char *data;
  struct node *next;
};

typedef struct node Node;

int node_ref_fields[] = {
  member_offset(struct node, data),
  member_offset(struct node, next),
  -1
};

pthread_mutex_t *lock;
Node *head = NULL;
Node *tail = NULL;

void print() {
  Node *tmp = head;
  while (tmp != NULL) {
    printf("%s\n", tmp->data);
    tmp = tmp->next;
  }
}

Node *createNode(int id, int num) {
  Node *tmp;
  //pobj_start();
  tmp = (Node *) pobj_malloc(sizeof(Node));
  pobj_ref_typify(tmp, node_ref_fields);
  tmp->data = (char *) pobj_malloc(sizeof(char) * MAX_MSG_SIZE);
  sprintf(tmp->data, "%d: This is message #%d", id, num);
  tmp->next = NULL;
  pobj_update(tmp);
  pobj_update(tmp->data);
  //pobj_end();
  return tmp;
}

void *work(void *pid) {
  int id = *((int *) pid);
  int i;
  for (i = 0; i <= id; i++) {
/*     pthread_mutex_lock(lock); */
    Node *tmp = createNode(id, i);
    pthread_mutex_lock(lock);
    //pobj_start();
    if (head == NULL) {
/*       head = tail = tmp; */
      pobj_static_set_ref(&head, tmp);
      pobj_static_set_ref(&tail, tmp);
    } else {
      tail->next = tmp;
/*       tail = tmp; */
      pobj_update(tail);
      pobj_static_set_ref(&tail, tmp);
    }
    //pobj_end();
    pthread_mutex_unlock(lock);
  }
  return NULL;
}

void message(char *msg) {
  printf("MASTER: %s\n", msg);
}

int main(int argc, char **argv) {
  int num = 5;
  if (argc > 1) {
    num = atoi(argv[1]);
  }
  if (pobj_init(NULL) == 0) {
    pthread_t threads[num];
    int ids[num];
    int i;

    message("First run");
    lock = (pthread_mutex_t *) malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(lock, NULL);
    pthread_mutex_lock(lock);
    for (i = 0; i < num; i++) {
      ids[i] = i;
      pthread_create(&threads[i], NULL, work, (void *) &ids[i]);
    }
    pthread_mutex_unlock(lock);
    for (i = 0; i < num; i++) {
      pthread_join(threads[i], NULL);
    }
  } else {
    message("Subsequent run");
  }
  print();
  pobj_shutdown();
}
