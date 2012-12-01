#include <config.h>
#include <stasis/util/multiset.h>
#include <assert.h>

struct stasis_util_multiset_t {
  lsn_t * items;
  intptr_t item_count;
};

stasis_util_multiset_t * stasis_util_multiset_create() {
  stasis_util_multiset_t * set = stasis_alloc(stasis_util_multiset_t);
  set->items = stasis_alloc(lsn_t);
  set->item_count = 0;
  return set;
}
void stasis_util_multiset_destroy(stasis_util_multiset_t * set) {
  free(set->items);
  free(set);
}

void stasis_util_multiset_insert(stasis_util_multiset_t * set, lsn_t item) {
  set->items = stasis_realloc(set->items, set->item_count+1, lsn_t);
  assert(set->items);
  set->items[set->item_count] = item;
  (set->item_count)++;
}
int stasis_util_multiset_remove(stasis_util_multiset_t * set, lsn_t item) {
  intptr_t i;
  for(i = 0; i < set->item_count; i++) {
    if(set->items[i] == item) { break; }
  }
  if(i == set->item_count) { return 0; }
  for(/*nop*/; i < set->item_count-1; i++) {
    set->items[i] = set->items[i]+1;
  }
  (set->item_count)--;
  return 1;
}
lsn_t stasis_util_multiset_min(const stasis_util_multiset_t * set) {
  lsn_t min = LSN_T_MAX;
  for(intptr_t i = 0; i < set->item_count; i++) {
    min = min < set->items[i] ? min : set->items[i];
  }
  return min;
}
