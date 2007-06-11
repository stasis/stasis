#include <check.h>
#include <stasis/transactional.h>
#include <stasis/lockManager.h>
#include <pthread.h>
#include <config.h>

#include <assert.h>
#include "../check_includes.h"
#include <stdlib.h>

#define LOG_NAME "check_errorhandling.log"

START_TEST(simpleDeadlockTest) {
  printf("\n");
  Tinit();
  setupLockManagerCallbacksPage();
  
  int xid = Tbegin();
  
  recordid rid = Talloc(xid, sizeof(int));
  Talloc(xid, sizeof(int));
  
  Tcommit(xid);
  assert(!compensation_error());
  
  xid = Tbegin();
  int xid2 = Tbegin();

  int i;
  
  Tread(xid, rid, &i);
  
  Tread(xid2, rid, &i);

  assert(!compensation_error());

  Tset(xid, rid, &i);

  assert(compensation_error()==LLADD_DEADLOCK);
  compensation_set_error(0);
  Tabort(xid);
  Tabort(xid2);
  assert(!compensation_error());

} END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("error_handling");
  /* Begin a new test */
  TCase *tc = tcase_create("deadlocks");

  /* Sub tests are added, one per line, here */

  tcase_add_test(tc, simpleDeadlockTest); 
  
  /* --------------------------------------------- */
  
  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
