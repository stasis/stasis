#include <lladd/transactional.h>

#include <config.h>
#include <check.h>

#include <lladd/transactional.h>
#include "../../src/lladd/latches.h"
#include "../../src/lladd/page.h"
#include "../../src/lladd/page/slotted.h" 
#include <lladd/bufferManager.h>
#include <sched.h>
#include <assert.h>
#include "../check_includes.h"

#define LOG_NAME   "check_bufferMananger.log"

#define NUM_PAGES 1000
#define THREAD_COUNT 10
#define READS_PER_THREAD 100
#define RECORDS_PER_THREAD 100

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
    p = loadPage(-1, rid.page); 

    assert(p->id != -1); 
    slottedPostRalloc(-1, p, 0, rid);

    writeRecord(1, p, 1, rid, &i);

    p->LSN = 0;
    *lsn_ptr(p) = 0;
    releasePage(p);    
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
    if(! (i % 50) ) {
      printf("%d", i / 50); fflush(NULL);
    }

    rid.page = k;
    rid.slot = 0;
    rid.size = sizeof(int);

    p = loadPage(-1, rid.page);
    
    readRecord(1, p, rid, &j);

    assert(rid.page == k);
    
    p->LSN = 0;
    *lsn_ptr(p) = 0;
    releasePage(p);

    assert(k == j);
    
  }

  return NULL;
}
static pthread_mutex_t ralloc_mutex;
void * workerThreadWriting(void * q) {

  int offset = *(int*)q;
  recordid rids[RECORDS_PER_THREAD];
  for(int i = 0 ; i < RECORDS_PER_THREAD; i++) {
    Page * tmp;
    pthread_mutex_lock(&ralloc_mutex);
    rids[i] = slottedPreRalloc(1, sizeof(int), &tmp);
    slottedPostRalloc(-1, tmp, 1, rids[i]);
    tmp->LSN = 0;
    *lsn_ptr(tmp) = 0;
    releasePage(tmp);
    pthread_mutex_unlock(&ralloc_mutex);
    
    /*    printf("\nRID:\t%d,%d\n", rids[i].page, rids[i].slot);  */
    /*  fflush(NULL);  */

    if(! (i % 100) ) {
      printf("A%d", i / 100); fflush(NULL);

    }

    /*    sched_yield(); */
  }
  for(int i = 0;  i < RECORDS_PER_THREAD; i++) {
    int val = (i * 10000) + offset;
    int k;
    Page * p = loadPage(-1, rids[i].page);

    assert(p->id == rids[i].page);

    for(k = 0; k < 100; k++) {
      assert(p->id == rids[i].page);
    }
    
    /*    sched_yield(); */
    writeRecord(1, p, 0, rids[i], &val); 

    assert(p->id == rids[i].page);
    p->LSN = 0;
    *lsn_ptr(p) = 0;
    releasePage(p);

    if(! (i % 100) ) {
      printf("W%d", i / 100); fflush(NULL);
    }

    /*    sched_yield(); */
  }
  for(int i = 0;  i < RECORDS_PER_THREAD; i++) {
    int val;
    Page * p;


    p = loadPage(-1, rids[i].page);

    readRecord(1, p, rids[i], &val); 

    p->LSN = 0;
    *lsn_ptr(p) = 0;
    releasePage(p);

    if(! (i % 100) ) {
      printf("R%d", i / 100); fflush(NULL);
    }


    assert(val == (i * 10000)+offset);

    /*    sched_yield(); */
  }

  return NULL;
}



START_TEST(pageSingleThreadTest) {
  Tinit();

  initializePages();

  printf("Initialize pages returned.\n"); fflush(NULL);

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
  pthread_mutex_init(&ralloc_mutex, NULL);
  workerThreadWriting(&i);
  pthread_mutex_destroy(&ralloc_mutex);

  Tdeinit();
}END_TEST

START_TEST(pageThreadedWritersTest) {
  pthread_t workers[RECORD_THREAD_COUNT];
  int i;

  Tinit();
  pthread_mutex_init(&ralloc_mutex, NULL);
  for(i = 0; i < RECORD_THREAD_COUNT; i++) {
    int * j = malloc(sizeof(int));
    *j = i;
    pthread_create(&workers[i], NULL, workerThreadWriting, j);
  }
  for(i = 0; i < RECORD_THREAD_COUNT; i++) {
    pthread_join(workers[i], NULL);
  }
  pthread_mutex_destroy(&ralloc_mutex);
  Tdeinit();
}END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("bufferManager");
  /* Begin a new test */
  TCase *tc = tcase_create("multithreaded");

  /* Sub tests are added, one per line, here */

  tcase_add_test(tc, pageSingleThreadTest); 
  tcase_add_test(tc, pageLoadTest);  
  tcase_add_test(tc, pageSingleThreadWriterTest);   
  tcase_add_test(tc, pageThreadedWritersTest); 

  /* --------------------------------------------- */
  
  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
