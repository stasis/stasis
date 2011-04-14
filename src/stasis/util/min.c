#include <stasis/common.h>
#include <stasis/util/min.h>
#include <stasis/redblack.h>

struct stasis_aggregate_min_t {
  struct rbtree * tree;
};

stasis_aggregate_min_t * stasis_aggregate_min_init(int(*cmp)(const void* a, const void *b, const void *c)) {
  stasis_aggregate_min_t * ret = malloc(sizeof(*ret));
  ret->tree = rbinit(cmp,0);
  return ret;
}
void stasis_aggregate_min_deinit(stasis_aggregate_min_t * min) {
  rbdestroy(min->tree);
  free(min);
}
void stasis_aggregate_min_add(stasis_aggregate_min_t * min, void * a) {
  rbsearch(a, min->tree);
}
const void * stasis_aggregate_min_remove(stasis_aggregate_min_t * min, void * a) {
  return rbdelete(a, min->tree);
}
const void * stasis_aggregate_min_compute(stasis_aggregate_min_t * min) {
  return rbmin(min->tree);
}

