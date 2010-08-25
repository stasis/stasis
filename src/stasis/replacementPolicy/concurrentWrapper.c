/*
 * threadsafeWrapper.c
 *
 *  Created on: Oct 20, 2009
 *      Author: sears
 */
#include <stasis/common.h>
#include <stasis/page.h>
#include <stasis/replacementPolicy.h>

typedef struct {
  replacementPolicy** impl;
  pthread_mutex_t* mut;
  int num_buckets;
  pthread_key_t next_bucket;
} stasis_replacement_policy_concurrent_wrapper_t;

static inline unsigned int bucket_hash(stasis_replacement_policy_concurrent_wrapper_t * rpw, void * page) {
  return ((unsigned long long)((Page*)page)->id) % rpw->num_buckets;
}

static void  cwDeinit  (struct replacementPolicy* impl) {
  stasis_replacement_policy_concurrent_wrapper_t * rp = impl->impl;
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
static void  cwHit     (struct replacementPolicy* impl, void * page) {
  stasis_replacement_policy_concurrent_wrapper_t * rp = impl->impl;
  unsigned int bucket = bucket_hash(rp, page);
  pthread_mutex_lock(&rp->mut[bucket]);
  rp->impl[bucket]->hit(rp->impl[bucket], page);
  pthread_mutex_unlock(&rp->mut[bucket]);
}
static void* cwGetStaleHelper(struct replacementPolicy* impl, void*(*func)(struct replacementPolicy*)) {
  stasis_replacement_policy_concurrent_wrapper_t * rp = impl->impl;
  intptr_t bucket = (intptr_t)pthread_getspecific(rp->next_bucket);
  intptr_t oldbucket = bucket;
  void *ret = 0;
  int spin_count = 0;
  while(ret == 0 && spin_count < rp->num_buckets) {
    int err;
    while((err = pthread_mutex_trylock(&rp->mut[bucket]))) {
      if(err != EBUSY) {
        fprintf(stderr, "error with trylock in replacement policy: %s", strerror(err)); abort();
      }
      bucket = (bucket + 1) % rp->num_buckets;
    }
    ret = func(rp->impl[bucket]);
    pthread_mutex_unlock(&rp->mut[bucket]);
    bucket = (bucket + 1) % rp->num_buckets;
    spin_count++;
  }
  if(ret == 0) {  // should be extremely rare.
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
static void* cwGetStale(struct replacementPolicy* impl) {
  stasis_replacement_policy_concurrent_wrapper_t * rp = impl->impl;
  return cwGetStaleHelper(impl, rp->impl[0]->getStale);
}
static void* cwGetStaleAndRemove(struct replacementPolicy* impl) {
  stasis_replacement_policy_concurrent_wrapper_t * rp = impl->impl;
  return cwGetStaleHelper(impl, rp->impl[0]->getStaleAndRemove);
}
static void* cwRemove  (struct replacementPolicy* impl, void * page) {
  stasis_replacement_policy_concurrent_wrapper_t * rp = impl->impl;
  unsigned int bucket = bucket_hash(rp, page);
  pthread_mutex_lock(&rp->mut[bucket]);
  void *ret = rp->impl[bucket]->remove(rp->impl[bucket], page);
  pthread_mutex_unlock(&rp->mut[bucket]);
  return ret;
}
static void  cwInsert  (struct replacementPolicy* impl, void * page) {
  stasis_replacement_policy_concurrent_wrapper_t * rp = impl->impl;
  unsigned int bucket = bucket_hash(rp, page);
  pthread_mutex_lock(&rp->mut[bucket]);
  rp->impl[bucket]->insert(rp->impl[bucket], page);
  pthread_mutex_unlock(&rp->mut[bucket]);
}

replacementPolicy* replacementPolicyConcurrentWrapperInit(replacementPolicy** rp, int count) {
  replacementPolicy *ret = malloc(sizeof(*ret));
  stasis_replacement_policy_concurrent_wrapper_t * rpw = malloc(sizeof(*rpw));
  rpw->mut = malloc(sizeof(rpw->mut[0]) * count);
  rpw->impl = malloc(sizeof(rpw->impl[0]) * count);
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
