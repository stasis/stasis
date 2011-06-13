#ifndef DOUBLE_LINKED_LIST_H
#define DOUBLE_LINKED_LIST_H
#define LL_ENTRY(foo) ll##foo

typedef void LL_ENTRY(value_t);

typedef struct LL_ENTRY(node_t) {
  LL_ENTRY(value_t) * v;
  struct LL_ENTRY(node_t) * prev;
  struct LL_ENTRY(node_t) * next;
} LL_ENTRY(node_t);

typedef struct LL_ENTRY(list) {
  LL_ENTRY(node_t) * (*getNode)(LL_ENTRY(value_t) * v, void * conf);
  void (*setNode)(LL_ENTRY(value_t) * v, LL_ENTRY(node_t) * n,  void * conf);
  void * conf;
  struct LL_ENTRY(node_t)* const head;
  struct LL_ENTRY(node_t)* const tail;
} LL_ENTRY(list) ;

LL_ENTRY(list)* LL_ENTRY(create)();
void LL_ENTRY(destroy)(LL_ENTRY(list) * l);

/** @return 0 on success, error code on error (such as ENOMEM) */
int LL_ENTRY(push) (LL_ENTRY(list)* l,
                   LL_ENTRY(value_t) * v);
LL_ENTRY(value_t)* LL_ENTRY(pop) (LL_ENTRY(list)* l);
/** @return 0 on success, error code on error (such as ENOMEM) */
int LL_ENTRY(unshift)(LL_ENTRY(list)* l,
                      LL_ENTRY(value_t) * v);

LL_ENTRY(value_t)* LL_ENTRY(shift) (LL_ENTRY(list)* l);

/** @return 0 on success, ENOENT if the entry does not exist */
int LL_ENTRY(remove)(LL_ENTRY(list)* l,
                      LL_ENTRY(value_t)* n);

LL_ENTRY(value_t*) LL_ENTRY(head)
     (LL_ENTRY(list)* l);
LL_ENTRY(value_t*) LL_ENTRY(tail)
     (struct LL_ENTRY(list)* l);
#endif
