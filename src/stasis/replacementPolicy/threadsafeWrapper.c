/*
 * threadsafeWrapper.c
 *
 *  Created on: Oct 20, 2009
 *      Author: sears
 */
#include <stasis/common.h>
#include <stasis/replacementPolicy.h>

typedef struct {
  replacementPolicy* impl;
  pthread_mutex_t mut;
} stasis_replacement_policy_threadsafe_wrapper_t;

static void  tsDeinit  (struct replacementPolicy* impl) {
  stasis_replacement_policy_threadsafe_wrapper_t * rp = impl->impl;
  rp->impl->deinit(rp->impl);
  free(impl);
}
static void  tsHit     (struct replacementPolicy* impl, Page* page) {
  stasis_replacement_policy_threadsafe_wrapper_t * rp = impl->impl;
  pthread_mutex_lock(&rp->mut);
  rp->impl->hit(rp->impl, page);
  pthread_mutex_unlock(&rp->mut);
}
static Page* tsGetStale(struct replacementPolicy* impl) {
  stasis_replacement_policy_threadsafe_wrapper_t * rp = impl->impl;
  pthread_mutex_lock(&rp->mut);
  void *ret = rp->impl->getStale(rp->impl);
  pthread_mutex_unlock(&rp->mut);
  return ret;
}
static Page* tsRemove  (struct replacementPolicy* impl, Page* page) {
  stasis_replacement_policy_threadsafe_wrapper_t * rp = impl->impl;
  pthread_mutex_lock(&rp->mut);
  void *ret = rp->impl->remove(rp->impl, page);
  pthread_mutex_unlock(&rp->mut);
  return ret;
}
static Page* tsGetStaleAndRemove  (struct replacementPolicy* impl) {
  stasis_replacement_policy_threadsafe_wrapper_t * rp = impl->impl;
  pthread_mutex_lock(&rp->mut);
  void *ret = rp->impl->getStaleAndRemove(rp->impl);
  pthread_mutex_unlock(&rp->mut);
  return ret;
}
static void  tsInsert  (struct replacementPolicy* impl, Page* page) {
  stasis_replacement_policy_threadsafe_wrapper_t * rp = impl->impl;
  pthread_mutex_lock(&rp->mut);
  rp->impl->insert(rp->impl, page);
  pthread_mutex_unlock(&rp->mut);
}

replacementPolicy* replacementPolicyThreadsafeWrapperInit(replacementPolicy* rp) {
  replacementPolicy *ret = stasis_alloc(replacementPolicy);
  stasis_replacement_policy_threadsafe_wrapper_t * rpw = stasis_alloc(stasis_replacement_policy_threadsafe_wrapper_t);
  rpw->impl = rp;
  pthread_mutex_init(&rpw->mut,0);
  ret->init = NULL;
  ret->deinit = tsDeinit;
  ret->hit = tsHit;
  ret->getStale = tsGetStale;
  ret->getStaleAndRemove = tsGetStaleAndRemove;
  ret->remove = tsRemove;
  ret->insert = tsInsert;
  ret->impl = rpw;
  return ret;
}
