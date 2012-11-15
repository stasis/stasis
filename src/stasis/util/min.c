#include <stasis/common.h>
#include <stasis/util/min.h>
#include <stasis/util/redblack.h>
#include <assert.h>

static int cmp_lsn_t(const void *ap, const void *bp, const void *ign) {
  lsn_t a = *(lsn_t*)ap;
  lsn_t b = *(lsn_t*)bp;

  return (a < b) ? -1 :
         (a > b) ?  1 : 0;
}

struct stasis_aggregate_min_t {
  struct rbtree * tree;
  lsn_t ** vals;
  int num_entries;
  pthread_key_t key;
  lsn_t * memo;
};

static void free_key(void * key) {
  free(key);
}

stasis_aggregate_min_t * stasis_aggregate_min_init(int large) {
  stasis_aggregate_min_t * ret = stasis_alloc(stasis_aggregate_min_t);
  if(large) {
    ret->tree = rbinit(cmp_lsn_t,0);
  } else {
    ret->tree = 0;
    ret->vals = 0;
    ret->num_entries = 0;
    pthread_key_create(&ret->key, free_key);
    ret->memo = 0;
  }
  return ret;
}
void stasis_aggregate_min_deinit(stasis_aggregate_min_t * min) {
  if(min->tree) {
    rbdestroy(min->tree);
  } else {

  }
  free(min->vals);
  pthread_key_delete(min->key);
  free(min);
}
void stasis_aggregate_min_add(stasis_aggregate_min_t * min, lsn_t * a) {
  if(min->tree) {
    const void * ret = rbsearch(a, min->tree);
    assert(ret == a);
  } else {
    if(min->memo) {
      if(*min->memo > *a) {
        min->memo = a;
      }
    }
    lsn_t * p = pthread_getspecific(min->key);
    if(!p) {
      p = stasis_alloc(lsn_t);
      *p = -1;
      pthread_setspecific(min->key, p);
    }

    if(*p != -1 && min->vals[*p] == 0) { min->vals[*p] = a; return; }

    for(int i = 0; i < min->num_entries; i++) {
      if(!min->vals[i]) {
        min->vals[i] = a;
        *p = i;
        return;
      }
    }
    min->num_entries++;
    min->vals = realloc(min->vals, min->num_entries * sizeof(lsn_t**));
    *p = min->num_entries-1;
    min->vals[*p] = a;
    return;
  }
}
const lsn_t * stasis_aggregate_min_remove(stasis_aggregate_min_t * min, lsn_t * a) {
  if(min->tree) {
    const lsn_t * ret = rbdelete(a, min->tree);
    assert(ret == a);
    return ret;
  } else {
    if(min->memo && *min->memo == *a) { min->memo = NULL; }
    lsn_t * p = pthread_getspecific(min->key);
    if(p /*key defined*/) {
      if(*p != -1 /*key points to slot in array*/) {
        if(min->vals[*p]/*slot in array points to something*/ ) {
          if(*min->vals[*p] == *a) {
            min->vals[*p] = 0;  // clear array entry
            return a;
          }
        }
      }
    }
    for(int i = 0; i < min->num_entries; i++) {
      if(min->vals[i] && (*min->vals[i] == *a)) {
	assert(min->vals[i] == a);
        lsn_t * ret = min->vals[i];
        min->vals[i] = 0;
        return ret;
      }
    }
    abort();
  }
}
const lsn_t * stasis_aggregate_min_compute(stasis_aggregate_min_t * min) {
  if(min->tree) {
    return (lsn_t*)rbmin(min->tree);
  } else {
    if(min->memo) {
      return min->memo;
    } else {
      for(int i = 0; i < min->num_entries; i++) {
        if(min->vals[i]) {
          if(!min->memo) {
            min->memo = min->vals[i];
          } else if(*min->vals[i] < *min->memo) {
            min->memo = min->vals[i];
          }
        }
      }
      return min->memo;
    }
  }
}

