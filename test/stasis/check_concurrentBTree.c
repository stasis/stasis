/*
 * check_concurrentBTree.c
 *
 *  Created on: Dec 22, 2011
 *      Author: sears
 */
#define _GNU_SOURCE
#include "../check_includes.h"

#include <stasis/util/concurrentHash.h>
#include <stasis/util/random.h>

#include <stdio.h>
#include <time.h>
#include <limits.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include <stasis/util/concurrentBTree.h>

#define LOG_NAME   "check_concurrentBTree.log"

START_TEST(metadataBitTest) {
  int NUM_ITERS = 1000000;
  metadata_t m = 0;
  byte isLeaf = 0;
  byte balanced = 0;
  byte color = 0;
  byte level = 0;
  for(int i = 0; i < NUM_ITERS; i++) {
    switch(stasis_util_random64(3)) {
    case 0: {// leaf
      if(isLeaf) {
        assert(metadata_is_leaf(m));
      } else {
        assert(!metadata_is_leaf(m));
      }
      isLeaf = stasis_util_random64(2);
      if(isLeaf) {
        m = metadata_set_leaf(m);
        m = leaf_metadata_set_color(m, color);
      } else {
        m = metadata_clear_leaf(m);
        m = index_metadata_set_level(m, level);
      }
    } break;
    case 1: {// balanced
      if(balanced) {
        assert(metadata_is_balanced(m));
      } else {
        assert(!metadata_is_balanced(m));
      }
      balanced = stasis_util_random64(2);
      if(balanced) {
        m = metadata_set_balanced(m);
      } else {
        m = metadata_clear_balanced(m);
      }
    } break;
    case 2: {
      if(isLeaf) { // color
        assert(color == leaf_metadata_get_color(m));
        color = stasis_util_random64(3);
        m = leaf_metadata_set_color(m, color);
      } else { // level
        assert(level == index_metadata_get_level(m));
        level = stasis_util_random64(8);
        m = index_metadata_set_level(m, level);
        assert(level == index_metadata_get_level(m));
      }
    } break;
    default: abort();
    }
  }
} END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("lhtable");
  /* Begin a new test */
  TCase *tc = tcase_create("lhtable");

  tcase_set_timeout(tc, 0); // disable timeouts

  /* Sub tests are added, one per line, here */
  tcase_add_test(tc, metadataBitTest);

  /* --------------------------------------------- */

  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"

