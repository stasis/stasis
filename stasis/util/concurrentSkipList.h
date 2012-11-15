/*
 * concurrentSkipList.h
 *
 *  Created on: Feb 8, 2012
 *      Author: sears
 */
#ifndef CONCURRENTSKIPLIST_H_
#define CONCURRENTSKIPLIST_H_

#include <stdio.h>
#include <stasis/common.h>
#include <stasis/util/random.h>

BEGIN_C_DECLS

//#define stasis_util_skiplist_assert(x) assert(x)
#define stasis_util_skiplist_assert(x)

#define STASIS_SKIPLIST_HP_COUNT 3

#include <stasis/util/hazard.h>

typedef struct stasis_skiplist_node_t {
  hazard_ptr key;
  pthread_mutex_t level_mut;
  char level;
  int16_t refcount;
} stasis_skiplist_node_t;
typedef struct {
  hazard_ptr header;
  int levelCap;
  int levelHint;
  pthread_mutex_t levelHint_mut;
  pthread_key_t k;
  hazard_t * h;
  hazard_t * ret_hazard;
  int (*cmp)(const void *a, const void *b);
  int (*finalize)(void *node, void *nul);
} stasis_skiplist_t;
static inline stasis_skiplist_node_t** stasis_util_skiplist_get_forward_raw(
    stasis_skiplist_node_t * x, int n) {
  return (stasis_skiplist_node_t**)(((intptr_t)(x + 1))
      +(n-1)*(sizeof(stasis_skiplist_node_t*)+sizeof(pthread_mutex_t)));
}
static inline hazard_ptr* stasis_util_skiplist_get_forward(
    stasis_skiplist_node_t * x, int n){
  return (hazard_ptr*)stasis_util_skiplist_get_forward_raw(x,n);
}
static inline pthread_mutex_t * stasis_util_skiplist_get_forward_mutex(
    stasis_skiplist_node_t * x, int n) {
  return (pthread_mutex_t*)(stasis_util_skiplist_get_forward(x,n)+1);
}
int stasis_util_skiplist_node_finalize(void * pp, void * conf) {
  stasis_skiplist_node_t * p = pp;
  stasis_skiplist_t * list = conf;
  if(p->refcount == 0) {
    void * oldKey = (void*)p->key;  // do this early to find races.
    for(int i = 1; i <= p->level; i++) {
      stasis_skiplist_node_t * n = *stasis_util_skiplist_get_forward_raw(p, i);
      int oldval = __sync_sub_and_fetch(&n->refcount, 1);
      (void)oldval;
      stasis_util_skiplist_assert(oldval >= 0);
    }
    hazard_free(list->ret_hazard, oldKey);
    pthread_mutex_destroy(&p->level_mut);
    stasis_util_skiplist_assert(oldKey == (void*)p->key);
    stasis_util_skiplist_assert(p->refcount == 0);
    free(p);
    return 1;
  } else {
    return 0;
  }
}
int stasis_util_skiplist_default_key_finalize(void * p, void * ignored) {
  free(p);
  return 1;
}


static inline int stasis_util_skiplist_random_level(pthread_key_t k) {
  kiss_table_t * kiss = pthread_getspecific(k);
  if(kiss == 0) {
    kiss = stasis_alloc(kiss_table_t);
    stasis_util_random_kiss_settable(kiss,
        random(), random(), random(), random(), random(), random());
    pthread_setspecific(k, kiss);
  }
  // MWC is weaker but faster than KISS.  The main drawback is that it has a
  // period of 2^60.  I can't imagine that mattering for our purposes.

  // __builtin_ctz counts trailing zeros, so, this function hardcodes p = 0.5.
  // MWC returns a 32-bit int; above 2^32 elements we start to violate the
  // O(log n) bounds.
  return 1+__builtin_ctz(stasis_util_random_kiss_MWC(kiss));
}


static inline hazard_ptr stasis_util_skiplist_make_node(int level, void * key) {
  stasis_skiplist_node_t * x
    = (stasis_skiplist_node_t*)malloc(sizeof(*x)
        + (level) * (sizeof(hazard_ptr) + sizeof(pthread_mutex_t)));
  x->key = (hazard_ptr)key;
  x->level = level;
  x->refcount = 0;
  pthread_mutex_init(&x->level_mut,0);
  for(int i = 1; i <= level; i++) {
    pthread_mutex_init(stasis_util_skiplist_get_forward_mutex(x, i), 0);
    *stasis_util_skiplist_get_forward(x, i) = 0;
  }
  return (hazard_ptr)x;
}
static inline void stasis_util_skiplist_cleanup_tls(void * p) {
  free(p);
}
static inline int stasis_util_skiplist_cmp_helper(
    stasis_skiplist_t* list, stasis_skiplist_node_t *a, void * bkey) {
  if(a == NULL) { return 1; }
  void *akey = hazard_ref(list->ret_hazard, 1, &(a->key));
  int ret;
  if(akey == NULL) { ret = (bkey == NULL ? 0 : -1); }
  else if(bkey == NULL) { ret = 1; }
  else {ret = list->cmp(akey, bkey); }
  hazard_release(list->ret_hazard, 1);
  return ret;
}
static inline int stasis_util_skiplist_cmp_helper2(
    stasis_skiplist_t* list, stasis_skiplist_node_t *a, stasis_skiplist_node_t * b) {
  if(b == NULL) { return a == NULL ? 0 : -1; }
  void *bkey = hazard_ref(list->ret_hazard, 2, &(b->key));
  int ret = stasis_util_skiplist_cmp_helper(list, a, bkey);
  hazard_release(list->ret_hazard, 2);
  return ret;
}
static inline stasis_skiplist_t * stasis_util_skiplist_init(
    int (*cmp)(const void*, const void*),
    int (*finalize)(void *, void * nul)) {
  stasis_skiplist_t * list = stasis_alloc(stasis_skiplist_t);
  list->levelCap = 32;
  list->h = hazard_init(STASIS_SKIPLIST_HP_COUNT+list->levelCap,
      STASIS_SKIPLIST_HP_COUNT, 250, stasis_util_skiplist_node_finalize, list);
  list->finalize
    = finalize ? finalize : stasis_util_skiplist_default_key_finalize;
  list->ret_hazard = hazard_init(3, 3, 250, list->finalize, NULL);
  list->levelHint = 1;
  pthread_mutex_init(&list->levelHint_mut, 0);
  list->header = stasis_util_skiplist_make_node(list->levelCap, NULL);
  pthread_key_create(&(list->k), stasis_util_skiplist_cleanup_tls);
  list->cmp = cmp;
  return list;
}
static inline void stasis_util_skiplist_deinit(stasis_skiplist_t * list) {
  hazard_deinit(list->h);
  hazard_deinit(list->ret_hazard);
  pthread_mutex_destroy(&list->levelHint_mut);
  free((void*)list->header);
  kiss_table_t * kiss = pthread_getspecific(list->k);
  if(kiss) {
    stasis_util_skiplist_cleanup_tls(kiss);
    pthread_setspecific(list->k, 0);
  }
  pthread_key_delete(list->k);
  free(list);
}

static inline void * stasis_util_skiplist_search(stasis_skiplist_t * list, void * searchKey) {
  // the = 0 here are to silence GCC -O3 warnings.
  stasis_skiplist_node_t *x, *y = 0;
  int cmp = 0;
  x = hazard_set(list->h,0,(void*)list->header);
  for(int i = list->levelHint; i > 0; i--) {
    y = hazard_ref(list->h,1,stasis_util_skiplist_get_forward(x, i));
    while((cmp = stasis_util_skiplist_cmp_helper(list, y, searchKey)) < 0) {
      x = hazard_set(list->h,0,(void*)y);
      y = hazard_ref(list->h,1,stasis_util_skiplist_get_forward(x, i));
    }
  }
  void * ret;
  if(cmp == 0) {
    ret = hazard_ref(list->ret_hazard, 0, &(y->key));
  } else {
    ret = 0;
    hazard_release(list->ret_hazard, 0);
  }
  hazard_release(list->h,0);
  hazard_release(list->h,1);
  return ret;
}
static inline stasis_skiplist_node_t * stasis_util_skiplist_get_lock(
    stasis_skiplist_t * list, stasis_skiplist_node_t * x, void * searchKey, int i) {
  stasis_skiplist_node_t * z
    = hazard_ref(list->h, 2, stasis_util_skiplist_get_forward(x, i));
  while(stasis_util_skiplist_cmp_helper(list, z, searchKey) < 0) {
    x = hazard_set(list->h, 0, (void*)z);
    z = hazard_ref(list->h, 2, stasis_util_skiplist_get_forward(x, i));
  }
  pthread_mutex_lock(stasis_util_skiplist_get_forward_mutex(x, i));
  z = hazard_ref(list->h, 2, stasis_util_skiplist_get_forward(x, i));
  while(stasis_util_skiplist_cmp_helper(list, z, searchKey) < 0) {
    // Should lock of z be here?
    pthread_mutex_unlock(stasis_util_skiplist_get_forward_mutex(x, i));
    x = hazard_set(list->h, 0, (void*)z);
    // Note: lock of z was here (and it was called x)
    pthread_mutex_lock(stasis_util_skiplist_get_forward_mutex(x, i));
    z = hazard_ref(list->h, 2, stasis_util_skiplist_get_forward(x, i));
  }
  stasis_util_skiplist_assert(stasis_util_skiplist_cmp_helper2(list, x, (stasis_skiplist_node_t*)*stasis_util_skiplist_get_forward(x, i)) < 0);
  hazard_release(list->h, 2);
  return x;
}
/**
 * Insert a value into the skiplist.  Any existing value will be replaced.
 * @return the old value or null if there was no such value.
 */
static inline void * stasis_util_skiplist_insert(stasis_skiplist_t * list, void * searchKey) {
  stasis_skiplist_node_t * update[list->levelCap+1];
  stasis_skiplist_node_t *x, *y;
IN:
  x = hazard_set(list->h, 0, (void*)list->header);
  int L = list->levelHint;
  // for i = L downto 1
  int i;
  for(i = L+1; i > 1;) {
    i--;
    y = hazard_ref(list->h, 1, stasis_util_skiplist_get_forward(x, i));
    while(stasis_util_skiplist_cmp_helper(list, y, searchKey) < 0) {
      x = hazard_set(list->h, 0, (void*)y);
      y = hazard_ref(list->h, 1, stasis_util_skiplist_get_forward(x, i));
    }
    update[i] = hazard_set(list->h, STASIS_SKIPLIST_HP_COUNT+(L-i), x);
  }
  // update[L..1] is set.
  // h [HP_COUNT+[0..L-1] is set.
  // Note get_lock grabs the hazard pointer for x.
  x = stasis_util_skiplist_get_lock(list, x, searchKey, 1);
  y = hazard_ref(list->h, 1, stasis_util_skiplist_get_forward(x, 1));
  if(stasis_util_skiplist_cmp_helper(list, y, searchKey) == 0) {
    pthread_mutex_unlock(stasis_util_skiplist_get_forward_mutex(x, 1));
    pthread_mutex_lock(&y->level_mut);

    x = hazard_ref(list->h, 0, stasis_util_skiplist_get_forward(y, 1));
    int isGarbage = stasis_util_skiplist_cmp_helper(list, x, searchKey) < 0;
    if(!isGarbage) {
      void * oldKey;
      do {
        oldKey = hazard_ref(list->ret_hazard, 0, &(y->key));
      } while(!__sync_bool_compare_and_swap(&(y->key), oldKey, searchKey));
      pthread_mutex_unlock(&y->level_mut);


      hazard_release(list->h, 0);
      hazard_release(list->h, 1);
      for(int i = L; i > 0; i--) {
        hazard_release(list->h, (i-1)+STASIS_SKIPLIST_HP_COUNT);
        // h [HP_COUNT+[L-1..0] is cleared
      }
      hazard_free(list->ret_hazard, oldKey);
      return oldKey;
    } else {
      pthread_mutex_unlock(&y->level_mut);
//      printf("insert landed on garbage node.  retrying.\n");
      goto IN;
    }
  }
  hazard_ptr newnode = stasis_util_skiplist_make_node(stasis_util_skiplist_random_level(list->k), searchKey);
  y = hazard_set(list->h, 1, (void*)newnode);
  pthread_mutex_lock(&y->level_mut);
  for(int i = L+1; i <= y->level; i++) {
    update[i] = (void*)list->header;
  }
  // update[L+1..y->level] is set
  for(int i = 1; i <= y->level; i++) {
    if(i != 1) {
      x = stasis_util_skiplist_get_lock(list, update[i], searchKey, i);
    }
    *stasis_util_skiplist_get_forward(y, i) = *stasis_util_skiplist_get_forward(x, i);
    *stasis_util_skiplist_get_forward(x, i) = (hazard_ptr)y;
    pthread_mutex_unlock(stasis_util_skiplist_get_forward_mutex(x, i));
  }
  pthread_mutex_unlock(&y->level_mut);

  int L2 = list->levelHint;
  if(L2 < list->levelCap && *stasis_util_skiplist_get_forward((void*)list->header, L2+1) != 0) {
    if(pthread_mutex_trylock(&list->levelHint_mut) == 0) {
      while(list->levelHint < list->levelCap &&
          *stasis_util_skiplist_get_forward((void*)list->header, list->levelHint+1) != 0) {
        list->levelHint = list->levelHint+1; // XXX atomics?
      }
      pthread_mutex_unlock(&list->levelHint_mut);
    }
  }
  hazard_release(list->h, 0);
  hazard_release(list->h, 1);
  for(int i = L; i > 0; i--) {
    // h [HP_COUNT+[L-1..0] is cleared
    hazard_release(list->h, (i-1)+STASIS_SKIPLIST_HP_COUNT);
  }
  return NULL;
}
/**
 * Delete a value from the list, returning it if it existed.
 * @return The old value, or null.
 */
static inline void * stasis_util_skiplist_delete(stasis_skiplist_t * list, void * searchKey) {
  stasis_skiplist_node_t * update[list->levelCap+1];
  stasis_skiplist_node_t *x, *y;
  x = hazard_set(list->h, 0, (void*)list->header);
  int L = list->levelHint;
  // for i = L downto 1
  int i;
  for(i = L+1; i > 1;) {
    i--; // decrement after check, so that i is 1 at the end of the loop.
    y = hazard_ref(list->h, 1, stasis_util_skiplist_get_forward(x, i));
    while(stasis_util_skiplist_cmp_helper(list, y, searchKey) < 0) {
      x = hazard_set(list->h, 0, (void*)y);
      y = hazard_ref(list->h, 1, stasis_util_skiplist_get_forward(x, i));
    }
    update[i] = hazard_set(list->h, STASIS_SKIPLIST_HP_COUNT+(L-i), x);
  }
  // h[HP_COUNT+[0..L-1] is set
  y = hazard_set(list->h, 1, (void*)x);
  int isGarbage = 0;
  int first = 1;
  // do ... until equal and not garbage
  do {
    // Note: it is unsafe to copy y->i directly into y, since doing so releases
    // the hazard pointer in race.  Fortunately, we don't need x for anything
    // until we overwrite it immediately below.
    x = hazard_ref(list->h, 0, stasis_util_skiplist_get_forward(y, i));
    if(first) {
      first = 0;
    } else {
      // This unlock was not in the pseudocode, but seems to be necessary...
      pthread_mutex_unlock(&y->level_mut);
    }
    y = hazard_set(list->h, 1, x);
    if(stasis_util_skiplist_cmp_helper(list, y, searchKey) > 0) {
      hazard_release(list->ret_hazard, 0);
      hazard_release(list->h, 0);
      hazard_release(list->h, 1);
      for(i = L+1; i > 1;) {
        i--;
        hazard_release(list->h, (i-1)+STASIS_SKIPLIST_HP_COUNT);
        // h[HP_COUNT+[L-1..0] is cleared
      }
      return NULL;
    }
    pthread_mutex_lock(&y->level_mut);
    x = hazard_ref(list->h, 0, stasis_util_skiplist_get_forward(y, i));
    // Note: this is a > in pseudocode, which lets equal nodes link back into themselves.
    isGarbage = stasis_util_skiplist_cmp_helper2(list, y, x) > 0;
    // pseudocode would unlock if garbage here.  Moved unlock to top of loop.
  } while(!(!isGarbage && stasis_util_skiplist_cmp_helper(list, y, searchKey) == 0));
  for(int i = L+1; i <= y->level; i++) { update[i] = (void*)list->header; }
  for(int i = y->level; i > 0; i--) {
    x = stasis_util_skiplist_get_lock(list, update[i], searchKey, i);
    pthread_mutex_lock(stasis_util_skiplist_get_forward_mutex(y, i));
    stasis_util_skiplist_assert(*stasis_util_skiplist_get_forward(x, i) == (intptr_t)y);
    __sync_fetch_and_add(&x->refcount, 1);
    *stasis_util_skiplist_get_forward(x, i) = *stasis_util_skiplist_get_forward(y, i);
    *stasis_util_skiplist_get_forward(y, i) = (hazard_ptr)x;
    stasis_util_skiplist_assert(stasis_util_skiplist_cmp_helper2(list, y, x) > 0); // assert is garbage
    pthread_mutex_unlock(stasis_util_skiplist_get_forward_mutex(x, i));
    pthread_mutex_unlock(stasis_util_skiplist_get_forward_mutex(y, i));
  }

  void * oldKey = hazard_ref(list->ret_hazard, 0, &(y->key));
  pthread_mutex_unlock(&y->level_mut);
  int L2 = list->levelHint;
  if(L2 > 1 && *stasis_util_skiplist_get_forward((void*)list->header, L2) == 0) {
    if(pthread_mutex_trylock(&list->levelHint_mut) == 0) {
      while(list->levelHint > 1 && (stasis_skiplist_node_t*)*stasis_util_skiplist_get_forward((void*)list->header, list->levelHint) == 0) {
        list->levelHint = list->levelHint - 1;
      }
      pthread_mutex_unlock(&list->levelHint_mut);
    }
  }
  hazard_release(list->h, 0);
  hazard_release(list->h, 1);
  for(i = L+1; i > 1;) {
    i--;
    hazard_release(list->h, (i-1)+STASIS_SKIPLIST_HP_COUNT);
    // h[HP_COUNT+[L-1..0] is cleared
  }
  // oldKey will be freed by y's finalizer
  hazard_free(list->h, y);
  return oldKey;
}
void stasis_skiplist_release(stasis_skiplist_t * list) {
  hazard_release(list->ret_hazard, 0);
}

END_C_DECLS
#endif /* CONCURRENTSKIPLIST_H_ */
