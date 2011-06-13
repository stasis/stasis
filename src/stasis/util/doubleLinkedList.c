#include <stasis/common.h>
#include <stasis/util/doubleLinkedList.h>
#include <assert.h>

typedef LL_ENTRY(value_t) value_t;
typedef struct LL_ENTRY(node_t) node_t;
typedef struct LL_ENTRY(list) list;

static inline void LL_ENTRY(pushNode) (struct LL_ENTRY(list)* l,
           struct LL_ENTRY(node_t) * v);
static inline struct LL_ENTRY(node_t)* LL_ENTRY(popNode) (struct LL_ENTRY(list)* l);
static inline void LL_ENTRY(unshiftNode)(struct LL_ENTRY(list)* l,
             struct LL_ENTRY(node_t) * v);
static inline struct LL_ENTRY(node_t)* LL_ENTRY(shiftNode) (struct LL_ENTRY(list)* l);
static inline void LL_ENTRY(removeNode)(list * l, node_t * n);

list * LL_ENTRY(create)(node_t*(*getNode)(value_t*v,void*conf), void(*setNode)(value_t*v,node_t*n,void*conf),void*conf) {
  list* ret = malloc(sizeof(list));

  // bypass const annotation on head, tail...
  list tmp = {
    getNode,
    setNode,
    conf,
    malloc(sizeof(node_t)),
    malloc(sizeof(node_t))
  };
  memcpy(ret, &tmp, sizeof(list));

  ret->head->prev = 0;
  ret->head->next = ret->tail;
  ret->tail->prev = ret->head;
  ret->tail->next = 0;
  return ret;
}

void LL_ENTRY(destroy)(list* l) {
  value_t * n;
  while((n = LL_ENTRY(pop)(l))) {
    // nop
  }
  free(l->head);
  free(l->tail);
  free(l);
}
int LL_ENTRY(push)(list* l, value_t * v) {
  node_t * n = malloc(sizeof(node_t));
  if(!n) { return ENOMEM; }
  n->v = v;
  assert(l->getNode(v, l->conf) == 0);
  l->setNode(v, n, l->conf);
  LL_ENTRY(pushNode)(l, n);
  return 0;
}
value_t* LL_ENTRY(pop) (list* l) {
  node_t * n = LL_ENTRY(popNode)(l);
  if(n) {
    value_t * v = n->v;
    assert(l->getNode(v, l->conf) == n);
    l->setNode(v, 0, l->conf);
    free(n);
    return v;
  } else {
    return 0;
  }
}
int LL_ENTRY(unshift)(list* l, value_t * v) {
  node_t * n = malloc(sizeof(node_t));
  if(!n) { return ENOMEM; }
  n->v = v;
  assert(l->getNode(v, l->conf) == 0);
  l->setNode(v, n, l->conf);
  LL_ENTRY(unshiftNode)(l, n);
  return 0;
}
value_t * LL_ENTRY(shift) (list* l) {
  node_t * n = LL_ENTRY(shiftNode)(l);
  if(n) {
    value_t * v = n->v;
    assert(l->getNode(v, l->conf) == n);
    l->setNode(v, 0, l->conf);
    free(n);
    return v;
  } else {
    return 0;
  }
}

LL_ENTRY(value_t*) LL_ENTRY(head)
     (LL_ENTRY(list)* l) {
  if(l->head->next != l->tail) {
    assert(l->getNode(l->head->next->v, l->conf) == l->head->next);
    return l->head->next->v;
  } else {
    return 0;
  }
}
LL_ENTRY(value_t*) LL_ENTRY(tail)
     (struct LL_ENTRY(list)* l) {
  if(l->tail->prev != l->head) {
    assert(l->getNode(l->tail->prev->v, l->conf) == l->tail->prev);
    return l->tail->prev->v;
  } else {
    return 0;
  }
}

int LL_ENTRY(remove)(list * l, value_t * v) {
  node_t *n = l->getNode(v, l->conf);
  if(n == 0) {
    return ENOENT;
  }
  assert(v == n->v);
  l->setNode(n->v, 0, l->conf);
  LL_ENTRY(removeNode)(l,n);
  free(n);
  return 0;
}

static inline void LL_ENTRY(pushNode)(list* l, node_t * n) {

  // Need to update 3 nodes: n , tail, tail->prev

  // n
  n->prev = l->tail->prev;
  n->next = l->tail;

  // tail
  l->tail->prev = n;

  // tail->prev is now n->prev
  n->prev->next = n;
}
node_t* LL_ENTRY(popNode) (list* l) {
  node_t * n = l->tail->prev;
  assert(n != l->tail);
  if(n != l->head) {
    assert(n->prev != 0);
    assert(n->next == l->tail);

    // n->prev
    n->prev->next = n->next;

    // tail
    l->tail->prev = n->prev;

    return n;
  } else {
    assert(n->prev == 0);
    return 0;
  }
}
static inline void LL_ENTRY(unshiftNode)(list* l, node_t * n) {

  // n
  n->prev = l->head;
  n->next = l->head->next;

  // head
  l->head->next = n;

  // head->next is now n->next
  n->next->prev = n;
}
static inline node_t * LL_ENTRY(shiftNode) (list* l) {
  node_t * n = l->head->next;
  assert(n != l->head);
  if(n != l->tail) {
    assert(n->next != 0);
    assert(n->prev == l->head);

    // n->next
    n->next->prev = n->prev;

    // head
    l->head->next = n->next;

    return n;
  } else {
    assert(n->next == 0);
    return 0;
  }
}
static inline void LL_ENTRY(removeNode)(list * l, node_t * n) {
  assert(n != l->head);
  assert(n != l->tail);
  assert(n->next != n);
  assert(n->prev != n);
  assert(n->next != n->prev);

  n->prev->next = n->next;
  n->next->prev = n->prev;
}
