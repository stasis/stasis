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
static void  tsHit     (struct replacementPolicy* impl, void * page) {
  stasis_replacement_policy_threadsafe_wrapper_t * rp = impl->impl;
  pthread_mutex_lock(&rp->mut);
  rp->impl->hit(rp->impl, page);
  pthread_mutex_unlock(&rp->mut);
}
static void* tsGetStale(struct replacementPolicy* impl) {
  stasis_replacement_policy_threadsafe_wrapper_t * rp = impl->impl;
  pthread_mutex_lock(&rp->mut);
  void *ret = rp->impl->getStale(rp->impl);
  pthread_mutex_unlock(&rp->mut);
  return ret;
}
static void* tsRemove  (struct replacementPolicy* impl, void * page) {
  stasis_replacement_policy_threadsafe_wrapper_t * rp = impl->impl;
  pthread_mutex_lock(&rp->mut);
  void *ret = rp->impl->remove(rp->impl, page);
  pthread_mutex_unlock(&rp->mut);
  return ret;
}
static void* tsGetStaleAndRemove  (struct replacementPolicy* impl) {
  stasis_replacement_policy_threadsafe_wrapper_t * rp = impl->impl;
  pthread_mutex_lock(&rp->mut);
  void *ret = rp->impl->getStaleAndRemove(rp->impl);
  pthread_mutex_unlock(&rp->mut);
  return ret;
}
static void  tsInsert  (struct replacementPolicy* impl, void * page) {
  stasis_replacement_policy_threadsafe_wrapper_t * rp = impl->impl;
  pthread_mutex_lock(&rp->mut);
  rp->impl->insert(rp->impl, page);
  pthread_mutex_unlock(&rp->mut);
}

replacementPolicy* replacementPolicyThreadsafeWrapperInit(replacementPolicy* rp) {
  replacementPolicy *ret = malloc(sizeof(*ret));
  stasis_replacement_policy_threadsafe_wrapper_t * rpw = malloc(sizeof(*rpw));
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
