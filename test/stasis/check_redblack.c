#define _GNU_SOURCE
#include "../check_includes.h"

#undef STLSEARCH

#ifdef DBUG_TEST
extern int dbug_choice(int);
#endif

#include <stasis/redblack.h>
#include <stasis/stlredblack.h>

#include <stdio.h>
#include <time.h>
#include <limits.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>

/*
#include <sys/time.h>
#include <time.h>
*/
#define LOG_NAME   "check_lhtable.log"

/**
   @test
*/

typedef struct {
  int a;
  int b;
} tup;

static int cmp_1(const void *ap, const void *bp, const void *ign) {
  const tup * a = ap;
  const tup * b = bp;
  return a->a < b->a ? -1
     : ( a->a > b->a ?  1
     : ( a->b < b->b ? -1
     : ( a->b > b->b ?  1
     : 0 )));
}
static int cmp_2(const void *ap, const void *bp, const void *ign) {
  const tup * a = ap;
  const tup * b = bp;
  return a->b < b->b ? -1
     : ( a->b > b->b ?  1
     : ( a->a < b->a ? -1
     : ( a->a > b->a ?  1
     : 0 )));
}
static void assert_equal(const void * rb_ret_1, const void * rb_ret_2,
                         const void * stl_ret_1, const void * stl_ret_2) {
  if(rb_ret_1 != rb_ret_2 || rb_ret_1 != stl_ret_1 || rb_ret_1 != stl_ret_2) {
    printf("Inconsistency detected! libredblack 1: %llx, 2: %llx stl: 1: %llx 2: %llx\n",
        (unsigned long long)(intptr_t)rb_ret_1,
        (unsigned long long)(intptr_t)rb_ret_2,
        (unsigned long long)(intptr_t)stl_ret_1,
        (unsigned long long)(intptr_t)stl_ret_2);
    fflush(stdout);
    abort();
  }
}


/* Create two libredblack trees and two stl trees.  Use different orders with consistent == for each pair */
START_TEST(rbRandTest) {

  time_t seed = time(0);
  printf("\nSeed = %ld\n", seed);
  srandom(seed);

#ifdef DBUG_TEST
  uint64_t NUM_OPERATIONS = 6;
  uint64_t NUM_ENTRIES = 3;
  uint64_t NUM_A = 3;
  uint64_t NUM_B = 1;
# define myrandom(a) dbug_choice(a)
#else
  uint64_t NUM_OPERATIONS = 1000 * 1000;
  uint64_t NUM_ENTRIES = myrandom(100 * 1000);
  uint64_t NUM_A = myrandom(200);
  uint64_t NUM_B = myrandom(50000);
#endif
  printf("NUM_OPERATIONS = %lld NUM_ENTRIES = %lld NUM_A = %lld NUM_B = %lld\n",
         (long long int)NUM_OPERATIONS, (long long int)NUM_ENTRIES, (long long int)NUM_A, (long long int)NUM_B);

  rbtree *rb_1 = rbinit(cmp_1, NULL);
  rbtree *rb_2 = rbinit(cmp_2, NULL);
  rbtree *stl_1 = stl_rbinit(cmp_1, NULL);
  rbtree *stl_2 = stl_rbinit(cmp_2, NULL);

  tup * entries = malloc(sizeof(tup) * NUM_ENTRIES);
#ifdef DBUG_TEST
  for(uint64_t i = 0; i < NUM_ENTRIES; i++) {
    entries[i].a = i;
    entries[i].b = 0;
  }
#else
  for(uint64_t i = 0; i < NUM_ENTRIES; i++) {
    entries[i].a = myrandom(NUM_A);
    entries[i].b = myrandom(NUM_B);
  }
#endif
  uint64_t num_found = 0;
  uint64_t num_collide = 0;
  for(uint64_t i = 0; i < NUM_OPERATIONS; i++) {
    uint64_t off = myrandom(NUM_ENTRIES);
#ifdef DBUG_TEST
    switch(myrandom(3)+1) {
#else
    switch(myrandom(4)) {
    case 0:
#endif
    case 1: { // insert
      const void * rb_ret_1 = rbsearch(&entries[off], rb_1);
      const void * rb_ret_2 = rbsearch(&entries[off], rb_2);
      const void * stl_ret_1 = stl_rbsearch(&entries[off], stl_1);
      const void * stl_ret_2 = stl_rbsearch(&entries[off], stl_2);
      assert_equal(rb_ret_1, rb_ret_2, stl_ret_1, stl_ret_2);
    } break;
    case 2: { // lookup
      const void * rb_ret_1 = rbfind(&entries[off], rb_1);
      const void * rb_ret_2 = rbfind(&entries[off], rb_2);
      const void * stl_ret_1 = stl_rbfind(&entries[off], stl_1);
      const void * stl_ret_2 = stl_rbfind(&entries[off], stl_2);
      assert_equal(rb_ret_1, rb_ret_2, stl_ret_1, stl_ret_2);
      if(rb_ret_1) { num_found++; }
      if(rb_ret_1 && rb_ret_1 != &entries[off]) { num_collide++; }
    } break;
    case 3: { // delete
      const void * rb_ret_1 = rbdelete(&entries[off], rb_1);
      const void * rb_ret_2 = rbdelete(&entries[off], rb_2);
      const void * stl_ret_1 = stl_rbdelete(&entries[off], stl_1);
      const void * stl_ret_2 = stl_rbdelete(&entries[off], stl_2);
      assert_equal(rb_ret_1, rb_ret_2, stl_ret_1, stl_ret_2);
      if(rb_ret_1) { num_found++; }
      if(rb_ret_1 && rb_ret_1 != &entries[off]) { num_collide++; }
    } break;
    default: abort();
    }
  }

  rbdestroy(rb_1);
  rbdestroy(rb_2);
  stl_rbdestroy(stl_1);
  stl_rbdestroy(stl_2);

  free(entries);

  printf("Num found: %lld (of %lld)\n", (long long int)num_found, (long long int)NUM_OPERATIONS);
  printf("Num collide: %lld\n", (long long int)num_collide);

} END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("redblack");
  /* Begin a new test */
  TCase *tc = tcase_create("redblack");

  tcase_set_timeout(tc, 0); // disable timeouts


  /* Sub tests are added, one per line, here */

  tcase_add_test(tc, rbRandTest);

  /* --------------------------------------------- */

  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
