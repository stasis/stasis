/*
 * threadsafeWrapper.c
 *
 *  Created on: Oct 20, 2009
 *      Author: sears
 */
#include <stasis/common.h>
#include <stasis/flags.h>
#include <stasis/page.h>
#include <stasis/replacementPolicy.h>


typedef struct {
  replacementPolicy** impl;
  pthread_mutex_t* mut;
  uint16_t num_buckets;
  pthread_key_t next_bucket;
} stasis_replacement_policy_concurrent_wrapper_t;


static inline uint16_t hash_mod_power_of_two(stasis_replacement_policy_concurrent_wrapper_t* rpw, uint16_t i) {
  return i & (rpw->num_buckets-1);
}
static inline uint16_t hash_mod_general_purpose(stasis_replacement_policy_concurrent_wrapper_t* rpw, uint16_t i) {
  return i % rpw->num_buckets;
}

static inline uint16_t hash_mod(stasis_replacement_policy_concurrent_wrapper_t* rpw, uint16_t i) {
  return stasis_replacement_policy_concurrent_wrapper_power_of_two_buckets ?
    hash_mod_power_of_two(rpw, i) : hash_mod_general_purpose(rpw, i);
}

static inline unsigned int bucket_hash(stasis_replacement_policy_concurrent_wrapper_t * rpw, void * page) {
  return hash_mod(rpw, ((unsigned long long)((Page*)page)->id));
}

static void  cwDeinit  (struct replacementPolicy* impl) {
  stasis_replacement_policy_concurrent_wrapper_t * rp = (stasis_replacement_policy_concurrent_wrapper_t *)impl->impl;
  for(int i = 0; i < rp->num_buckets; i++) {
    rp->impl[i]->deinit(rp->impl[i]);
    pthread_mutex_destroy(&rp->mut[i]);
  }
  pthread_key_delete(rp->next_bucket);
  free(rp->impl);
  free(rp->mut);
  free(rp);
  free(impl);
}
static void  cwHit     (struct replacementPolicy* impl, Page* page) {
  stasis_replacement_policy_concurrent_wrapper_t * rp = (stasis_replacement_policy_concurrent_wrapper_t *)impl->impl;
  unsigned int bucket = bucket_hash(rp, page);
  pthread_mutex_lock(&rp->mut[bucket]);
  rp->impl[bucket]->hit(rp->impl[bucket], page);
  pthread_mutex_unlock(&rp->mut[bucket]);
}
static Page* cwGetStaleHelper(struct replacementPolicy* impl, Page*(*func)(struct replacementPolicy*)) {
  stasis_replacement_policy_concurrent_wrapper_t * rp = (stasis_replacement_policy_concurrent_wrapper_t *)impl->impl;
  intptr_t bucket = (intptr_t)pthread_getspecific(rp->next_bucket);
  intptr_t oldbucket = bucket;
  Page*ret = 0;
  int spin_count = 0;
  while(ret == 0 && spin_count < rp->num_buckets) {
    int err;
    int lock_spin = 0;
    while((err = pthread_mutex_trylock(&rp->mut[bucket]))) {
      if(err != EBUSY) {
        fprintf(stderr, "error with trylock in replacement policy: %s", strerror(err)); abort();
      }
      bucket = hash_mod(rp, bucket + 1);
      lock_spin++;
      if(lock_spin > 4) {
        static int warned = 0;
        if(!warned) {
          fprintf(stderr, "Warning: lots of thread contention in concurrent wrapper\n");
          warned = 1;
        }
      }
    }
    ret = func(rp->impl[bucket]);
    pthread_mutex_unlock(&rp->mut[bucket]);
    bucket = hash_mod(rp, bucket + 1);
    spin_count++;
    if((!ret) &&
       stasis_replacement_policy_concurrent_wrapper_exponential_backoff &&
       spin_count > 1) {

      if(bucket != oldbucket) {
        pthread_setspecific(rp->next_bucket, (void*) bucket);
      }

      return 0;
    }
  }
  if(ret == 0) {  // should be extremely rare.
    static int warned = 0;
    if(!warned) {
      fprintf(stderr, "Warning: concurrentWrapper is having difficulty finding a page to replace\n");
      warned = 1;
    }
    for(int i = 0; i < rp->num_buckets; i++) {
      pthread_mutex_lock(&rp->mut[i]);
    }
    for(int i = 0; i < rp->num_buckets; i++) {
      if((ret = func(rp->impl[i]))) {
        bucket = i;
        break;
      }
    }
    for(int i = 0; i < rp->num_buckets; i++) {
      pthread_mutex_unlock(&rp->mut[i]);
    }
  }
  if(bucket != oldbucket) {
    // note that, even on success, we increment the bucket.  Otherwise, we could
    // (would) eventually get unlucky, and some caller would do a getStaleAndRemove,
    // fail to get a latch, insert it back, and the next getStaleAndRemove would
    // deterministically return the same page again, leading to an infinite loop.
    pthread_setspecific(rp->next_bucket, (void*) bucket);
  }
  return ret;
}
static Page* cwGetStale(struct replacementPolicy* impl) {
  stasis_replacement_policy_concurrent_wrapper_t * rp = (stasis_replacement_policy_concurrent_wrapper_t *)impl->impl;
  return cwGetStaleHelper(impl, rp->impl[0]->getStale);
}
static Page* cwGetStaleAndRemove(struct replacementPolicy* impl) {
  stasis_replacement_policy_concurrent_wrapper_t * rp = (stasis_replacement_policy_concurrent_wrapper_t *)impl->impl;
  return cwGetStaleHelper(impl, rp->impl[0]->getStaleAndRemove);
}
static Page* cwRemove  (struct replacementPolicy* impl, Page* page) {
  stasis_replacement_policy_concurrent_wrapper_t * rp = (stasis_replacement_policy_concurrent_wrapper_t *)impl->impl;
  unsigned int bucket = bucket_hash(rp, page);
  pthread_mutex_lock(&rp->mut[bucket]);
  void *ret = rp->impl[bucket]->remove(rp->impl[bucket], page);
  pthread_mutex_unlock(&rp->mut[bucket]);
  return (Page*)ret;
}
static void  cwInsert  (struct replacementPolicy* impl, Page* page) {
  stasis_replacement_policy_concurrent_wrapper_t * rp = (stasis_replacement_policy_concurrent_wrapper_t *)impl->impl;
  unsigned int bucket = bucket_hash(rp, page);
  pthread_mutex_lock(&rp->mut[bucket]);
  rp->impl[bucket]->insert(rp->impl[bucket], page);
  pthread_mutex_unlock(&rp->mut[bucket]);
}

replacementPolicy* replacementPolicyConcurrentWrapperInit(replacementPolicy** rp, int count) {
  replacementPolicy *ret = stasis_alloc(replacementPolicy);
  stasis_replacement_policy_concurrent_wrapper_t * rpw = stasis_alloc(stasis_replacement_policy_concurrent_wrapper_t);

  if(stasis_replacement_policy_concurrent_wrapper_power_of_two_buckets) {
    // ensure that count is a power of two.
    int bits = 1;
    while(count /= 2) { bits++; }
    count = 1; bits --;
    while(bits > 0) { count *= 2; bits--; }
  }
  rpw->mut = stasis_malloc(count, pthread_mutex_t);
  rpw->impl = stasis_malloc(count, replacementPolicy*);
  for(int i = 0; i < count; i++) {
    pthread_mutex_init(&rpw->mut[i],0);
    rpw->impl[i] = rp[i];
  }
  rpw->num_buckets = count;
  pthread_key_create(&rpw->next_bucket,0);
  pthread_setspecific(rpw->next_bucket, (void*)0);
  ret->init = NULL;
  ret->deinit = cwDeinit;
  ret->hit = cwHit;
  ret->getStale = cwGetStale;
  ret->getStaleAndRemove = cwGetStaleAndRemove;
  ret->remove = cwRemove;
  ret->insert = cwInsert;
  ret->impl = rpw;
  return ret;
}
