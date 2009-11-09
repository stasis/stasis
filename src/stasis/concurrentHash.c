/*
 * concurrenthash.c
 *
 *  Created on: Oct 15, 2009
 *      Author: sears
 */
#define _XOPEN_SOURCE
#include <config.h>
#include <stasis/concurrentHash.h>
#include <assert.h>
#include <stdio.h>

struct bucket_t {
  pageid_t key;
  pthread_mutex_t mut;
  void * val;
};

struct hashtable_t {
  bucket_t* buckets;
  pageid_t maxbucketid;
};

static inline pageid_t hashtable_wrap(hashtable_t *ht, pageid_t p) {
  return p & ht->maxbucketid;
}
static inline pageid_t hash6432shift(pageid_t key)
{
  //  return key * 13;

//  key = (~key) + (key << 21); // key = (key << 21) - key - 1;
//  key = key ^ (key >> 24);
//  key = (key + (key << 3)) + (key << 8); // key * 265
//  key = key ^ (key >> 14);
//  key = (key + (key << 2)) + (key << 4); // key * 21
//  key = key ^ (key >> 28);
//  key = key + (key << 31);
//  return key;

  //  key = (~key) + (key << 18); // key = (key << 18) - key - 1;
  //  key = key ^ (key >> 31);
  //  key = key * 21; // key = (key + (key << 2)) + (key << 4);
  //  key = key ^ (key >> 11);
  //  key = key + (key << 6);
  //  key = key ^ (key >> 22);


//  return (key | 64) ^ ((key >> 15) | (key << 17));

//    return stasis_crc32(&key, sizeof(key), 0);
  return key * 13;

}
static inline pageid_t hashtable_func(hashtable_t *ht, pageid_t key) {
  return hashtable_wrap(ht, hash6432shift(key));
}

hashtable_t * hashtable_init(pageid_t size) {
  pageid_t newsize = 1;
  for(int i = 0; size; i++) {
    size /= 2;
    newsize *= 2;
  }
  hashtable_t *ht = malloc(sizeof(*ht));

  ht->maxbucketid = (newsize) - 1;
  ht->buckets = calloc(ht->maxbucketid+1, sizeof(bucket_t));
  pthread_mutexattr_t attr;
  pthread_mutexattr_init(&attr);
  pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
  for(pageid_t i = 0; i <= ht->maxbucketid; i++) {
    pthread_mutex_init(&(ht->buckets[i].mut), &attr);
  }

  return ht;
}
void hashtable_deinit(hashtable_t * ht) {
  for(pageid_t i = 0; i < ht->maxbucketid; i++) {
    pthread_mutex_destroy(&ht->buckets[i].mut);
  }
  free(ht->buckets);
  free(ht);
}
typedef enum {
  LOOKUP,
  INSERT,
  TRYINSERT,
  REMOVE
} hashtable_mode;
static inline void * hashtable_begin_op(hashtable_mode mode, hashtable_t *ht, pageid_t p, void *val, hashtable_bucket_handle_t *h) {
  static int warned = 0;
  pageid_t idx = hashtable_func(ht, p);
  void * ret;
  bucket_t *b1 = &ht->buckets[idx], *b2 = NULL;
  pthread_mutex_lock(&b1->mut); // start crabbing

  int num_incrs = 0;

  while(1) {
    // Loop invariants:
    //         b1 is latched, b2 is unlatched
    if(num_incrs > 10 && !warned) {
      warned = 1;
      printf("The hashtable is seeing lots of collisions.  Increase its size?\n");
    }
    assert(num_incrs < (ht->maxbucketid/4));
    num_incrs++;
    if(b1->key == p) { ret = b1->val; break; }
    if(b1->val == NULL) { ret = NULL; break; }
    idx = hashtable_wrap(ht, idx+1);
    b2 = b1;
    b1 = &ht->buckets[idx];
    pthread_mutex_lock(&b1->mut);
    pthread_mutex_unlock(&b2->mut);
  }
  h->b1 = b1; // at this point, b1 is latched.
  h->key = p;
  h->idx = idx;
  h->ret = ret;
  return ret;
}

void hashtable_end_op(hashtable_mode mode, hashtable_t *ht, void *val, hashtable_bucket_handle_t *h) {
  pageid_t idx = h->idx;
  bucket_t * b1 = h->b1;
  bucket_t * b2 = NULL;
  if(mode == INSERT || (mode == TRYINSERT && h->ret == NULL)) {
    b1->key = h->key;
    b1->val = val;
  } else if(mode == REMOVE && h->ret != NULL)  {
    pageid_t idx2 = idx;
    idx = hashtable_wrap(ht, idx+1);
    b2 = b1;
    b1 = &ht->buckets[idx];
    pthread_mutex_lock(&b1->mut);
    while(1) {
      // Loop invariants: b2 needs to be overwritten.
      //                  b1 and b2 are latched
      //                  b1 is the next bucket to consider for copying into b2.

      // What to do with b1?
      // Case 1: It is null, we win.
      if(b1->val == NULL) {
//        printf("d\n"); fflush(0);
        b2->key = 0;
        b2->val = NULL;
        break;
      } else {
        pageid_t newidx = hashtable_func(ht, b1->key);
        // Case 2: b1 belongs "after" b2
        // Subcase 1: newidx is higher than idx2, so newidx should stay where it is.
        // Subcase 2: newidx wrapped, so it is less than idx2, but more than half way around the ring.
        if(idx2 < newidx || (idx2 > newidx + (ht->maxbucketid/2))) {
          // skip this b1.
  //        printf("s\n"); fflush(0);
          idx = hashtable_wrap(ht, idx+1);
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
          idx = hashtable_wrap(ht, idx+1);
          b2 = b1;
          b1 = &ht->buckets[idx];
          pthread_mutex_lock(&b1->mut);
        }
      }
    }
    pthread_mutex_unlock(&b2->mut);
  }
  pthread_mutex_unlock(&b1->mut);  // stop crabbing
}
static inline void * hashtable_op(hashtable_mode mode, hashtable_t *ht, pageid_t p, void *val) {
  hashtable_bucket_handle_t h;
  void * ret = hashtable_begin_op(mode, ht, p, val, &h);
  hashtable_end_op(mode, ht, val, &h);
  return ret;
}
static inline void * hashtable_op_lock(hashtable_mode mode, hashtable_t *ht, pageid_t p, void *val, hashtable_bucket_handle_t *h) {
  void * ret = hashtable_begin_op(mode, ht, p, val, h);
  pthread_mutex_lock(&h->b1->mut); // XXX evil
  hashtable_end_op(mode, ht, val, h);
  return ret;
}

void * hashtable_insert(hashtable_t *ht, pageid_t p, void * val) {
  void * ret = hashtable_op(INSERT, ht, p, val);
  return ret;
}
void * hashtable_test_and_set(hashtable_t *ht, pageid_t p, void * val) {
  void * ret = hashtable_op(TRYINSERT, ht, p, val);
  return ret;
}
void * hashtable_lookup(hashtable_t *ht, pageid_t p) {
  void * ret = hashtable_op(LOOKUP, ht, p, NULL);
  return ret;
}
void * hashtable_remove(hashtable_t *ht, pageid_t p) {
  void * ret = hashtable_op(REMOVE, ht, p, NULL);
  return ret;
}

void * hashtable_insert_lock(hashtable_t *ht, pageid_t p, void * val, hashtable_bucket_handle_t *h) {
  return hashtable_op_lock(INSERT, ht, p, val, h);
}
void * hashtable_test_and_set_lock(hashtable_t *ht, pageid_t p, void * val, hashtable_bucket_handle_t *h) {
  return hashtable_op_lock(TRYINSERT, ht, p, val, h);
}
void * hashtable_lookup_lock(hashtable_t *ht, pageid_t p, hashtable_bucket_handle_t *h) {
  return hashtable_op_lock(LOOKUP, ht, p, NULL, h);
}
void hashtable_unlock(hashtable_bucket_handle_t *h) {
  pthread_mutex_unlock(&h->b1->mut);
}

void * hashtable_remove_begin(hashtable_t *ht, pageid_t p, hashtable_bucket_handle_t *h) {
  return hashtable_begin_op(REMOVE, ht, p, NULL, h);
}
void hashtable_remove_finish(hashtable_t *ht, hashtable_bucket_handle_t *h) {
 // when begin_remove_lock returns, it leaves the remove half done.  we then call this to decide if the remove should happen.  Other than hashtable_unlock, this is the only method you can safely call while holding a latch.
  hashtable_end_op(REMOVE, ht, NULL, h);
}
void hashtable_remove_cancel(hashtable_t *ht, hashtable_bucket_handle_t *h) {
 // when begin_remove_lock returns, it leaves the remove half done.  we then call this to decide if the remove should happen.  Other than hashtable_unlock, this is the only method you can safely call while holding a latch.
  hashtable_end_op(LOOKUP, ht, NULL, h);  // hack
}
