#include <config.h>
#include <stdlib.h>
#include <string.h>
#include <lladd/doubleLinkedList.h>
#include <assert.h>

typedef LL_ENTRY(value_t) value_t;
typedef struct LL_ENTRY(node_t) node_t;
typedef struct LL_ENTRY(list) list;

list * LL_ENTRY(create)() {
  list* ret = malloc(sizeof(list));

  // bypass const annotation on head, tail...
  list tmp = { 
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
node_t * LL_ENTRY(push)(list* l, value_t * v) { 
  node_t * n = malloc(sizeof(node_t));
  n->v = v;
  LL_ENTRY(pushNode)(l, n);
  return n;
}
value_t* LL_ENTRY(pop) (list* l) {
  node_t * n = LL_ENTRY(popNode)(l);
  if(n) { 
    value_t * v = n->v;
    free(n);
    return v;
  } else { 
    return 0;
  }
}
node_t * LL_ENTRY(unshift)(list* l, value_t * v) { 
  node_t * n = malloc(sizeof(node_t));
  n->v = v;
  LL_ENTRY(unshiftNode)(l, n);
  return n;
}
value_t * LL_ENTRY(shift) (list* l) {
  node_t * n = LL_ENTRY(shiftNode)(l);
  if(n) { 
    value_t * v = n->v;
    free(n);
    return v;
  } else { 
    return 0;
  }
}


void LL_ENTRY(pushNode)(list* l, node_t * n) { 

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
void LL_ENTRY(unshiftNode)(list* l, node_t * n) { 

  // n
  n->prev = l->head;
  n->next = l->head->next;

  // head
  l->head->next = n;

  // head->next is now n->next
  n->next->prev = n;
}
node_t * LL_ENTRY(shiftNode) (list* l) {
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
void LL_ENTRY(remove)(list * l, node_t * n) { 
  LL_ENTRY(removeNoFree)(l,n);
  free(n);
}
void LL_ENTRY(removeNoFree)(list * l, node_t * n) { 
  assert(n != l->head);
  assert(n != l->tail);
  assert(n->next != n);
  assert(n->prev != n);
  assert(n->next != n->prev);

  n->prev->next = n->next;
  n->next->prev = n->prev;
}
