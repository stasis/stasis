#include <lladd/transactional.h>

#include <config.h>
#include <check.h>

#include <lladd/transactional.h>
/*#include <lladd/logger/logEntry.h> */
#include "../../src/lladd/logger/logHandle.h"
#include "../../src/lladd/logger/logWriter.h"
#include "../../src/lladd/latches.h"
#include "../../src/lladd/page.h"
#include <lladd/bufferManager.h>
#include <sched.h>
#include <assert.h>
#include "../check_includes.h"

#define LOG_NAME   "check_bufferMananger.log"

#define NUM_PAGES 1000
#define THREAD_COUNT 5
#define READS_PER_THREAD 50000
void initializePages() {
  
  int i; 

  for(i = 0 ; i < NUM_PAGES; i++) {
    recordid rid;
    rid.page = i;
    rid.slot = 0;
    rid.size = sizeof(int);
    writeRecord(1, 1, rid, &i);
  }
  
}

void * workerThread(void * p) {
  int i;
  for(i = 0 ; i < READS_PER_THREAD; i++) {
    recordid rid;
    int j;

    int k = (int) (((double)NUM_PAGES)*rand()/(RAND_MAX+1.0));
    
    if(! (i % 5000) ) {
      printf("%d", i / 5000); fflush(NULL);
    }

    rid.page = k;
    rid.slot = 0;
    rid.size = sizeof(int);

    readRecord(1, rid, &j);
    assert(k == j);
  }

  return NULL;
}

START_TEST(pageSingleThreadTest) {
  Tinit();

  initializePages();

  /*  sleep(100000000); */

  workerThread(NULL);

  Tdeinit();
} END_TEST

/** 
    @test 

    Spawns a bunch of threads, and has each one randomly load pages off of the disk.

    The pages are inialized with unique values that are checked by the
    threads as they load the pages.

    In order to be effective, this test must create enough pages on
    disk to make sure that loadPagePtr will eventually have to kick
    pages.

*/
START_TEST(pageLoadTest) {
  pthread_t workers[THREAD_COUNT];
  int i;

  /*   fail_unless(0, "Broken for now.");
       assert(0); */
  Tinit();

  initializePages();

  for(i = 0; i < THREAD_COUNT; i++) {
    pthread_create(&workers[i], NULL, workerThread, NULL);
  }
  for(i = 0; i < THREAD_COUNT; i++) {
    pthread_join(workers[i], NULL);
  }

  Tdeinit();
} END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("logWriter");
  /* Begin a new test */
  TCase *tc = tcase_create("writeNew");

  /* Sub tests are added, one per line, here */

  /*tcase_add_test(tc, pageSingleThreadTest); */
  tcase_add_test(tc, pageLoadTest);

  /* --------------------------------------------- */
  
  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
