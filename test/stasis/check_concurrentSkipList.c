/*
 * check_concurrentSkipList.c
 *
 *  Created on: Feb 8, 2012
 *      Author: sears
 */
#include <string.h>
#include <assert.h>
#include <stasis/common.h>

int num_keys    = 1000000;
void * key_dup(intptr_t p) {
  intptr_t * ret = stasis_alloc(intptr_t);
  *ret = p;
  return ret;
}
#ifdef STRINGS
static inline int stasis_util_skiplist_cmp(const void *a, const void *b) {
  if(a == NULL) return -1;
  if(b == NULL) return 1;
  assert(!(a == NULL && b == NULL));
  return strcmp(a,b);
}
#else
static inline int stasis_util_skiplist_cmp(const void *a, const void *b) {
  // Note: Below, we ensure a and b are both >= 0 and small.
  //assert(*(intptr_t*)a < 2*num_keys);
  //assert(*(intptr_t*)b < 2*num_keys);
  return ((int)*(intptr_t*)a-(long)*(intptr_t*)b);
}
#endif
#include <stasis/util/concurrentSkipList.h>
#include "../check_includes.h"

#include <stasis/constants.h>
#include <stasis/util/random.h>

#include <sys/time.h>
#include <time.h>

#define LOG_NAME   "check_lhtable.log"

int num_threads = 4;
int concurrent = 0;
stasis_skiplist_t * list;
void * worker(void* p) {
  intptr_t * keys = p;
  intptr_t collisions = 0;
  for(int i = 0; i < num_keys; i++) {
    char * ret = stasis_util_skiplist_insert(list, key_dup(keys[i]));
    if(ret != NULL) {
      assert(!stasis_util_skiplist_cmp(ret, &keys[i]));
      collisions++;
    }
  }
  for(int i = 0; i < num_keys; i++) {
    char * ret = stasis_util_skiplist_search(list, &keys[i]);
    if(!concurrent) assert(!stasis_util_skiplist_cmp(ret, &keys[i]));
  }
  for(int i = 0; i < num_keys; i++) {
    char * ret = stasis_util_skiplist_delete(list, &keys[i]);
    if(ret == NULL) {
      collisions--;
    }
  }
  stasis_skiplist_release(list);
  return (void*) collisions;
}
/**
   @test
*/
START_TEST(concurrentSkipList_smokeTest) {
  list = stasis_util_skiplist_init(stasis_util_skiplist_cmp, 0);
  char ** const keys = stasis_malloc(num_keys, char*);
  for(int i = 0; i < num_keys; i++) {
#ifdef STRINGS
    int err = asprintf(&keys[i], "%d", (int)stasis_util_random64(2*num_keys));
    (void) err;
#else
    keys[i] = (void*)(1+stasis_util_random64(2*num_keys));
#endif
  }
  printf("Initted\n");
  fflush(stdout);
  struct timeval tv;
  gettimeofday(&tv,0);
  double start = stasis_timeval_to_double(tv);
  intptr_t collisions = (intptr_t)worker(keys);
  assert(collisions == 0);
  gettimeofday(&tv,0);
  double stop = stasis_timeval_to_double(tv);
  double elapsed = stop - start;
  double opspersec = 3.0*num_keys / elapsed;
  printf("Run took %f seconds.  %f ops/sec 1 thread %f ops/thread-second\n", elapsed, opspersec, opspersec);
  stasis_util_skiplist_deinit(list);
#ifdef STRINGS
  for(int i = 0; i < num_keys; i++) {
    free(keys[i]);
  }
#endif
  free(keys);
} END_TEST

START_TEST(concurrentSkipList_concurrentTest) {
  list = stasis_util_skiplist_init(stasis_util_skiplist_cmp, 0);
  concurrent = 1;
  char *** const keys = stasis_malloc(num_threads, char**);
  for(int j = 0; j < num_threads; j++) {
    keys[j] = stasis_malloc(num_keys, char*);
    for(int i = 0; i < num_keys; i++) {
  #ifdef STRINGS
      int err = asprintf(&keys[i], "%d", (int)stasis_util_random64(2*num_keys));
      (void) err;
  #else
      keys[j][i] = (void*)(1+stasis_util_random64(2*num_keys));
  #endif
    }
  }
  printf("Initted\n");
  fflush(stdout);
  pthread_t * threads = stasis_malloc(num_threads, pthread_t);
  struct timeval tv;
  gettimeofday(&tv,0);
  double start = stasis_timeval_to_double(tv);
  int collisions = 0;
  for(int j = 0; j < num_threads; j++) {
    pthread_create(&threads[j], 0, worker, keys[j]);
  }
  for(int j = 0; j < num_threads; j++) {
    intptr_t ret;
    pthread_join(threads[j], (void*)&ret);
    collisions += ret;
#ifdef STRINGS
    for(int i = 0; i < num_keys; i++) {
      free(keys[j][i]);
    }
#endif
    free(keys[j]);
  }
  assert(collisions == 0);
  free(threads);
  gettimeofday(&tv,0);
  double stop = stasis_timeval_to_double(tv);
  double elapsed = stop - start;
  double opspersec = 3.0*(double)num_keys*num_threads / elapsed;
  double opsperthsec = 3.0*(double)num_keys / elapsed;
  printf("Run took %f seconds.  %f ops/sec %d threads %f ops/thread-second\n", elapsed, opspersec, num_threads, opsperthsec);
  stasis_util_skiplist_deinit(list);
  free(keys);
} END_TEST
void * worker2(void * p) {
  for(int i = 0; i < num_keys; i++) {
    intptr_t key = (intptr_t)stasis_util_random64(1000);
    switch(stasis_util_random64(3)) {
    case 0: {
      stasis_util_skiplist_insert(list, key_dup(key));
    } break;
    case 1: {
      stasis_util_skiplist_search(list, &key);
    } break;
    case 2: {
      stasis_util_skiplist_delete(list, &key);
    } break;
    }
  }
  stasis_skiplist_release(list);
  return 0;
}
START_TEST(concurrentSkipList_concurrentRandom) {
  list = stasis_util_skiplist_init(stasis_util_skiplist_cmp, 0);
  pthread_t thread[num_threads];
  for(int i = 0; i < num_threads; i++) {
    pthread_create(&thread[i], 0, worker2, 0);
  }
  for(int i = 0; i < num_threads; i++) {
    pthread_join(thread[i], 0);
  }
  for(intptr_t i = 0; i < 1000; i++) {
    stasis_util_skiplist_delete(list, &i);
  }
  stasis_util_skiplist_deinit(list);
} END_TEST
Suite * check_suite(void) {
  Suite *s = suite_create("concurrentSkipList");
  /* Begin a new test */
  TCase *tc = tcase_create("concurrentSkipList");
  tcase_set_timeout(tc, 0); // disable timeouts

  /* Sub tests are added, one per line, here */
  tcase_add_test(tc, concurrentSkipList_smokeTest);
  tcase_add_test(tc, concurrentSkipList_concurrentTest);
  tcase_add_test(tc, concurrentSkipList_concurrentRandom);

  /* --------------------------------------------- */

  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"


