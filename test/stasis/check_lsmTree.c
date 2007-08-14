#include <config.h>
#include <check.h>
#include "../check_includes.h"

#include <stasis/transactional.h>

#include <assert.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>

#include <sys/time.h>
#include <time.h>

#define LOG_NAME   "check_lsmTree.log"
#define NUM_ENTRIES 100000
#define OFFSET      (NUM_ENTRIES * 10)

#define DEBUG(...) 
/** @test
*/
START_TEST(lsmTreeTest)
{
  Tinit();
  int xid = Tbegin();
  recordid tree = TlsmCreate(xid, 0, sizeof(int)); // xxx comparator not set.
  for(int i = 0; i < NUM_ENTRIES; i++) {
    long pagenum = TlsmFindPage(xid, tree, (byte*)&i, sizeof(int));
    assert(pagenum == -1);
    DEBUG("TlsmAppendPage %d\n",i);
    TlsmAppendPage(xid, tree, (const byte*)&i, sizeof(int), i + OFFSET);
    //    fflush(NULL);
    pagenum = TlsmFindPage(xid, tree, (byte*)&i, sizeof(int));
    assert(pagenum == i + OFFSET);
  }

  for(int i = 0; i < NUM_ENTRIES; i++) {
    long pagenum = TlsmFindPage(xid, tree, (byte*)&i, sizeof(int));
    assert(pagenum == i + OFFSET);
  }

  Tcommit(xid);
  Tdeinit();
} END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("lsmTree");
  /* Begin a new test */
  TCase *tc = tcase_create("simple");

  tcase_set_timeout(tc, 1200); // 20 minute timeout
  /* Sub tests are added, one per line, here */
  tcase_add_test(tc, lsmTreeTest);

  /* --------------------------------------------- */

  tcase_add_checked_fixture(tc, setup, teardown);

  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
