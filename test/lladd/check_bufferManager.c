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
#define THREAD_COUNT 25
#define READS_PER_THREAD 10000
#define RECORDS_PER_THREAD 10000
#define RECORD_THREAD_COUNT 25
void initializePages() {
  
  int i; 

  printf("Initialization starting\n"); fflush(NULL);

  for(i = 0 ; i < NUM_PAGES; i++) {
    Page * p;
    recordid rid;
    rid.page = i;
    rid.slot = 0;
    rid.size = sizeof(int);
    p = loadPage(rid.page);
    assert(p->id != -1); 
    pageSlotRalloc(p, 0, rid);
    unlock(p->loadlatch);
    /*    addPendingEvent(rid.page);  */
    writeRecord(1, 1, rid, &i);
    /*  removePendingEvent(rid.page);  */
    /*    assert(p->pending == 0); */

  }
  
  printf("Initialization complete.\n"); fflush(NULL);

}

void * workerThread(void * p) {
  int i;
 
  for(i = 0 ; i < READS_PER_THREAD; i++) {
    recordid rid;
    int j;

    int k = (int) (((double)NUM_PAGES)*rand()/(RAND_MAX+1.0));
    Page * p;
    if(! (i % 500) ) {
      printf("%d", i / 500); fflush(NULL);
    }

    rid.page = k;
    rid.slot = 0;
    rid.size = sizeof(int);

    /*  addPendingEvent(rid.page); */
    readRecord(1, rid, &j);
    assert(rid.page == k);
    /*    removePendingEvent(rid.page); */
    assert(k == j);
    
  }

  return NULL;
}

void * workerThreadWriting(void * q) {

  int offset = *(int*)q;
  recordid rids[RECORDS_PER_THREAD];
  for(int i = 0 ; i < RECORDS_PER_THREAD; i++) {
    /*    addPendingEvent(rids[i].page); */
    rids[i] = ralloc(1, sizeof(int));
    /*    removePendingEvent(rids[i].page); */
    
    /*    printf("\nRID:\t%d,%d\n", rids[i].page, rids[i].slot);  */
    fflush(NULL); 

    if(! (i % 1000) ) {
      printf("A%d", i / 1000); fflush(NULL);
    }

    

    sched_yield();
  }
  for(int i = 0;  i < RECORDS_PER_THREAD; i++) {
    int val = (i * 10000) + offset;
    int oldpage = rids[i].page;

    writeRecord(1, 0, rids[i], &val); 

    if(! (i % 1000) ) {
      printf("W%d", i / 1000); fflush(NULL);
    }

    sched_yield();
  }
  for(int i = 0;  i < RECORDS_PER_THREAD; i++) {
    int val;
    Page * p;

    readRecord(1, rids[i], &val); 

    if(! (i % 1000) ) {
      printf("R%d", i / 1000); fflush(NULL);
    }


    assert(val == (i * 10000)+offset);

    sched_yield();
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

START_TEST(pageSingleThreadWriterTest) {
  int i = 100;
  
  Tinit();
  
  workerThreadWriting(&i);

  Tdeinit();
}END_TEST

START_TEST(pageThreadedWritersTest) {
  pthread_t workers[RECORD_THREAD_COUNT];
  int i;

  Tinit();

  for(i = 0; i < RECORD_THREAD_COUNT; i++) {
    int * j = malloc(sizeof(int));
    *j = i;
    pthread_create(&workers[i], NULL, workerThreadWriting, j);
  }
  for(i = 0; i < RECORD_THREAD_COUNT; i++) {
    pthread_join(workers[i], NULL);
  }

  Tdeinit();
}END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("bufferManager");
  /* Begin a new test */
  TCase *tc = tcase_create("multithreaded");

  /* Sub tests are added, one per line, here */

  /*  tcase_add_test(tc, pageSingleThreadTest); */
  tcase_add_test(tc, pageLoadTest); 
    /*   tcase_add_test(tc, pageSingleThreadWriterTest);  */
  tcase_add_test(tc, pageThreadedWritersTest);

  /* --------------------------------------------- */
  
  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
