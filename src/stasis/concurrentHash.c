/*
 * concurrenthash.c
 *
 *  Created on: Oct 15, 2009
 *      Author: sears
 */
#include <stasis/common.h>
#include <assert.h>

typedef struct bucket {
  pageid_t key;
  pthread_mutex_t mut;
  void * val;
} bucket_t;

typedef struct hashtable {
  bucket_t* buckets;
  pageid_t maxbucketid;
  pageid_t numentries;
  char tracknum;
} hashtable_t;

hashtable_t * hashtable_init(pageid_t size, int tracknum) {
  pageid_t newsize = 1;
  for(int i = 0; size; i++) {
    size /= 2;
    newsize *= 2;
  }
  hashtable_t *ht = malloc(sizeof(*ht));

  ht->maxbucketid = (newsize) - 1;
  ht->buckets = calloc(ht->maxbucketid+1, sizeof(bucket_t));
  for(pageid_t i = 0; i <= ht->maxbucketid; i++) {
    pthread_mutex_init(&ht->buckets[i].mut, 0);
  }

  ht->numentries = 0;
  ht->tracknum = (tracknum == 0 ? 0 : 1);

  return ht;
}
void hashtable_deinit(hashtable_t * ht) {
  for(pageid_t i = 0; i < ht->maxbucketid; i++) {
    pthread_mutex_destroy(&ht->buckets[i].mut);
  }
  free(ht->buckets);
  free(ht);
}
static inline pageid_t hashtable_func(hashtable_t *ht, pageid_t p) {
  return p & ht->maxbucketid;
}
typedef enum {
  LOOKUP,
  INSERT,
  TRYINSERT,
  REMOVE
} hashtable_mode;
static inline void * hashtable_op(hashtable_mode mode, hashtable_t *ht, pageid_t p, void *val) {
  pageid_t idx = hashtable_func(ht, p);
  void * ret;
  bucket_t *b1 = &ht->buckets[idx], *b2 = NULL;
  pthread_mutex_lock(&b1->mut); // start crabbing

  int num_incrs = 0;

  while(1) {
    // Loop invariants:
    //         b1 is latched, b2 is unlatched
    assert(num_incrs < (ht->maxbucketid/4));
    num_incrs++;
    if(b1->key == p) { ret = b1->val; break; }
    if(b1->val == NULL) { ret = NULL; break; }
    idx = hashtable_func(ht, idx+1);
    b2 = b1;
    b1 = &ht->buckets[idx];
    pthread_mutex_lock(&b1->mut);
    pthread_mutex_unlock(&b2->mut);
  }
  if(mode == INSERT || (mode == TRYINSERT && ret == NULL)) {
    b1->key = p;
    b1->val = val;
  } else if(mode == REMOVE && ret != NULL)  {
    pageid_t idx2 = idx;
    idx = hashtable_func(ht, idx+1);
    b2 = b1;
    b1 = &ht->buckets[idx];
    pthread_mutex_lock(&b1->mut);
    while(1) {
      // Loop invariants: b2 needs to be overwritten.
      //                  b1 and b2 are latched
      //                  b1 is the next bucket to consider for copying into b2.

      // What to do with b1?
      // Case 1: It is null, we win.
      pageid_t newidx = hashtable_func(ht, b1->key);
      if(b1->val == NULL) {
//        printf("d\n"); fflush(0);
        b2->key = 0;
        b2->val = NULL;
        break;
      // Case 2: b1 belongs "after" b2
        // Subcase 1: newidx is higher than idx2, so newidx should stay where it is.
        // Subcase 2: newidx wrapped, so it is less than idx2, but more than half way around the ring.
      } else if(idx2 < newidx || (idx2 > newidx + (ht->maxbucketid/2))) {
        // skip this b1.
//        printf("s\n"); fflush(0);
        idx = hashtable_func(ht, idx+1);
        pthread_mutex_unlock(&b1->mut);
        b1 = &ht->buckets[idx];
        pthread_mutex_lock(&b1->mut);
      // Case 3: we can compact b1 into b2's slot.
      } else {
//        printf("c %lld %lld %lld  %lld\n", startidx, idx2, newidx, ht->maxbucketid); fflush(0);
        b2->key = b1->key;
        b2->val = b1->val;
        pthread_mutex_unlock(&b2->mut);
        // now we need to overwrite b1, so it is the new b2.
        idx2 = idx;
        idx = hashtable_func(ht, idx+1);
        b2 = b1;
        b1 = &ht->buckets[idx];
        pthread_mutex_lock(&b1->mut);
      }
    }
    pthread_mutex_unlock(&b2->mut);
  }
  pthread_mutex_unlock(&b1->mut);  // stop crabbing
  return ret;
}
void * hashtable_insert(hashtable_t *ht, pageid_t p, void * val) {
  return hashtable_op(INSERT, ht, p, val);
}
void * hashtable_test_and_set(hashtable_t *ht, pageid_t p, void * val) {
  return hashtable_op(TRYINSERT, ht, p, val);
}
void * hashtable_lookup(hashtable_t *ht, pageid_t p) {
  return hashtable_op(LOOKUP, ht, p, NULL);
}
void * hashtable_remove(hashtable_t *ht, pageid_t p) {
  return hashtable_op(REMOVE, ht, p, NULL);
}
