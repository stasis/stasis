/*
 * hazard.h
 *
 *  Created on: Feb 7, 2012
 *      Author: sears
 */
#include <stasis/common.h>
#include <stasis/util/time.h>
#include <assert.h>
#ifndef HAZARD_H_
#define HAZARD_H_

typedef intptr_t hazard_ptr;
typedef struct hazard_t hazard_t;

typedef struct hazard_ptr_rec_t {
  hazard_t * h;
  hazard_ptr * hp;
  void ** rlist;
  int rlist_len;
  struct hazard_ptr_rec_t * next;
} hazard_ptr_rec_t;

struct hazard_t {
  pthread_key_t hp;
  int num_slots;
  int stack_start;
  int num_r_slots;
  int (*finalizer)(void *, void* conf);
  void * conf;
  hazard_ptr_rec_t * tls_list;
  pthread_mutex_t tls_list_mut;
  pthread_cond_t thread_shutdown;
};
static int intptr_cmp(const void * ap, const void *bp) {
  intptr_t a = *(intptr_t*)ap;
  intptr_t b = *(intptr_t*)bp;
  return (a < b) ? -1 : ( (a > b) ? 1 : 0 );
}
static inline void hazard_scan(hazard_t * h, hazard_ptr_rec_t * rec) {
  if(rec == NULL) {
    rec = pthread_getspecific(h->hp);
  }
  if(rec == NULL) { return; }
  qsort(rec->rlist, rec->rlist_len, sizeof(void*), intptr_cmp);
  hazard_ptr * ptrs = 0;
  int ptrs_len = 0;
  pthread_mutex_lock(&h->tls_list_mut);
  hazard_ptr_rec_t * list = h->tls_list;
  while(list != NULL) {
    ptrs = realloc(ptrs, sizeof(hazard_ptr) * (ptrs_len+h->num_slots));
//    int would_stop = 0;
    for(int i = 0; i < h->num_slots; i++) {
      ptrs[ptrs_len] = list->hp[i];
      if(!ptrs[ptrs_len]) {
//        if(i >= h->stack_start) { would_stop = 1; }
        if(i >= h->stack_start) { break; }
      } else {
//        assert(! would_stop);
        ptrs_len++;
      }
    }
    list = list->next;
  }
  pthread_mutex_unlock(&h->tls_list_mut);
  qsort(ptrs, ptrs_len, sizeof(void*), intptr_cmp);
  int i = 0, j = 0;
  while(j < rec->rlist_len) {
    while(i < ptrs_len && (hazard_ptr)rec->rlist[j] > ptrs[i]) { i++; }
    if(i == ptrs_len || (hazard_ptr)rec->rlist[j] != ptrs[i]) {
      if(h->finalizer((void*)rec->rlist[j], h->conf)) {
        rec->rlist[j] = 0;
      }
    }
    j++;
  }
  j = 0;
  for(i = 0; i < rec->rlist_len; i++) {
    if(rec->rlist[i] != 0) {
      rec->rlist[j] = rec->rlist[i];
      j++;
    }
  }
  rec->rlist_len = j;
  free(ptrs);
}
static void hazard_deinit_thread(void * p) {
  hazard_ptr_rec_t * rec = p;
  if(rec != NULL) {
    while(rec->rlist_len != 0) {
      hazard_scan(rec->h, rec);
      if(rec->rlist_len != 0) {
        pthread_mutex_lock(&rec->h->tls_list_mut);
        struct timeval tv;
        gettimeofday(&tv, 0);
        struct timespec ts = stasis_double_to_timespec(stasis_timeval_to_double(tv) + 0.01);
        pthread_cond_timedwait(&rec->h->thread_shutdown,
          &rec->h->tls_list_mut, &ts);
        pthread_mutex_unlock(&rec->h->tls_list_mut);
      }
    }
    pthread_cond_broadcast(&rec->h->thread_shutdown);
    pthread_mutex_lock(&rec->h->tls_list_mut);
    hazard_ptr_rec_t ** last = &rec->h->tls_list;
    hazard_ptr_rec_t * list = *last;
    while(list != rec) {
      last = &list->next;
      list = *last;
    }
    *last = rec->next;
    pthread_mutex_unlock(&rec->h->tls_list_mut);
    free(rec->hp);
    free(rec->rlist);
    free(rec);
  }
}
/**
 * Init the state necessary for a set of hazard pointers.  This module
 * implements an optimization where the higher numbered pointers can be treated
 * as a fixed length stack.  Entries after the first NULL in that region will
 * be ignored.  This allows applications that need varying numbers of hazard
 * pointers to be collected efficiently.
 *
 * @param hp_slots the total number of slots.
 * @param stack_start the first hazard pointer in the "stack" region.
 * @param r_slots the max number of uncollected values per thread.
 */
static inline hazard_t* hazard_init(int hp_slots, int stack_start, int r_slots,
    int (*finalizer)(void*, void*), void * conf) {
  hazard_t * ret = stasis_alloc(hazard_t);
  pthread_key_create(&ret->hp, hazard_deinit_thread);
  ret->num_slots = hp_slots;
  ret->stack_start = stack_start;
  ret->num_r_slots = r_slots;
  ret->tls_list = NULL;
  ret->finalizer = finalizer;
  ret->conf = conf;
  pthread_mutex_init(&ret->tls_list_mut,0);
  pthread_cond_init(&ret->thread_shutdown, 0);
  return ret;
}
static inline hazard_ptr_rec_t * hazard_ensure_tls(hazard_t * h) {
  hazard_ptr_rec_t * rec = pthread_getspecific(h->hp);
  if(rec == NULL) {
    rec = stasis_alloc(hazard_ptr_rec_t);
    rec->hp = stasis_calloc(h->num_slots, hazard_ptr);
    rec->rlist = stasis_calloc(h->num_r_slots, void*);
    rec->rlist_len = 0;
    rec->h = h;
    pthread_setspecific(h->hp, rec);
    pthread_mutex_lock(&h->tls_list_mut);
    rec->next = h->tls_list;
    h->tls_list = rec;
    pthread_mutex_unlock(&h->tls_list_mut);
  }
  return rec;
}
static inline void hazard_deinit(hazard_t * h) {
  hazard_ptr_rec_t * rec = pthread_getspecific(h->hp);
  hazard_deinit_thread(rec);
  pthread_key_delete(h->hp);
  assert(h->tls_list == NULL);
  pthread_mutex_destroy(&h->tls_list_mut);
  pthread_cond_destroy(&h->thread_shutdown);
  free(h);
}
static inline void * hazard_ref(hazard_t* h, int slot, hazard_ptr* ptr) {
  hazard_ptr_rec_t * rec = hazard_ensure_tls(h);
  do {
    rec->hp[slot] = *ptr;         // Read ptr from ram
    __sync_synchronize();         // Push HP to ram
  } while(rec->hp[slot] != *ptr); // Re-read ptr from ram
  return (void*) rec->hp[slot];
}
/**
 *  Set a hazard pointer using a known-stable address.  This is mostly useful
 *  when the value pointed to by one hazard pointer should be pointed to by
 *  another hazard pointer.
 */
static inline void* hazard_set(hazard_t* h, int slot, void* val) {
  hazard_ptr_rec_t * rec = hazard_ensure_tls(h);
  rec->hp[slot] = (hazard_ptr)val;
  // val is stable (and on our stack!) so there's no reason to re-check it.
  __sync_synchronize();
  return val;
}
static inline void hazard_release(hazard_t* h, int slot) {
  hazard_ptr_rec_t * rec = hazard_ensure_tls(h);
  __sync_synchronize(); // prevent the = 0 from being moved before this line.
  rec->hp[slot] = 0;
}
// Must be called *after* all references to ptr are removed.
static inline void hazard_free(hazard_t* h,  void* ptr) {
  hazard_ptr_rec_t * rec = hazard_ensure_tls(h);
  rec->rlist[rec->rlist_len] = ptr;
  (rec->rlist_len)++;
  while(rec->rlist_len == h->num_r_slots) {
    hazard_scan(h, rec);
    if(rec->rlist_len == h->num_r_slots) {
      struct timespec slp = stasis_double_to_timespec(0.001);
      nanosleep(&slp,0);
    }
  }
}
#endif /* HAZARD_H_ */
