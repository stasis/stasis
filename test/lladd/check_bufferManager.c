#include <lladd/transactional.h>

#include <config.h>
#include <check.h>

#include <lladd/transactional.h>
/*#include <lladd/logger/logEntry.h> */
#include "../../src/lladd/logger/logHandle.h"
#include "../../src/lladd/logger/logWriter.h"

#include "../../src/lladd/latches.h"
#include <sched.h>
#include <assert.h>
#include "../check_includes.h"



#define LOG_NAME   "check_bufferMananger.log"


/** 
    @test 

    Spawns a bunch of threads, and has each one randomly load pages off of the disk.

    The pages are inialized with unique values that are checked by the
    threads as they load the pages.

    In order to be effective, this test must create enough pages on
    disk to make sure that loadPagePtr will eventually have to kick
    pages.

*/
START_TEST(pageLoadTest)
{
  fail_unless(0, "Write this test!");
} END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("logWriter");
  /* Begin a new test */
  TCase *tc = tcase_create("writeNew");

  /* Sub tests are added, one per line, here */

  tcase_add_test(tc, pageLoadTest);

  /* --------------------------------------------- */
  
  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
