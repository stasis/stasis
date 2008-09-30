#ifndef DOUBLE_LINKED_LIST_H
#define DOUBLE_LINKED_LIST_H
#define LL_ENTRY(foo) ll##foo

typedef void LL_ENTRY(value_t);

struct LL_ENTRY(node_t) {
  LL_ENTRY(value_t) * v;
  struct LL_ENTRY(node_t) * prev;
  struct LL_ENTRY(node_t) * next;
};

struct LL_ENTRY(list) { 
  struct LL_ENTRY(node_t)* const head;
  struct LL_ENTRY(node_t)* const tail;
};

struct LL_ENTRY(list)* LL_ENTRY(create)();
void LL_ENTRY(destroy)(struct LL_ENTRY(list) * l);

struct LL_ENTRY(node_t)* LL_ENTRY(push) (struct LL_ENTRY(list)* l,
					 LL_ENTRY(value_t) * v);
LL_ENTRY(value_t)* LL_ENTRY(pop) (struct LL_ENTRY(list)* l);

struct LL_ENTRY(node_t)* LL_ENTRY(unshift)(struct LL_ENTRY(list)* l, 
					    LL_ENTRY(value_t) * v);
LL_ENTRY(value_t)* LL_ENTRY(shift) (struct LL_ENTRY(list)* l);

void LL_ENTRY(remove)(struct LL_ENTRY(list)* l,
		      struct LL_ENTRY(node_t)* n);

void LL_ENTRY(pushNode) (struct LL_ENTRY(list)* l,
			 struct LL_ENTRY(node_t) * v);
struct LL_ENTRY(node_t)* LL_ENTRY(popNode) (struct LL_ENTRY(list)* l);

void LL_ENTRY(unshiftNode)(struct LL_ENTRY(list)* l, 
			   struct LL_ENTRY(node_t) * v);
struct LL_ENTRY(node_t)* LL_ENTRY(shiftNode) (struct LL_ENTRY(list)* l);
void LL_ENTRY(removeNoFree)(struct LL_ENTRY(list)* l,
		      struct LL_ENTRY(node_t)* n);

static inline LL_ENTRY(value_t*)LL_ENTRY(head)
     (struct LL_ENTRY(list)* l) { 
  if(l->head->next != l->tail) { 
    return l->head->next->v;
  } else { 
    return 0;
  } 
}
static inline LL_ENTRY(value_t*) LL_ENTRY(tail)
     (struct LL_ENTRY(list)* l) { 
  if(l->tail->prev != l->head) { 
    return l->tail->prev->v;
  } else { 
    return 0;
  } 
}
#endif
