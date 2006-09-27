#include <lladd/transactional.h>

#include <config.h>
#include <check.h>

#include <lladd/transactional.h>
#include "../../src/lladd/latches.h"
#include "../../src/lladd/page.h"
#include "../../src/lladd/pageFile.h"
#include "../../src/lladd/page/slotted.h" 
#include <lladd/bufferManager.h>
#include <sched.h>
#include <assert.h>
#include "../check_includes.h"

#define LOG_NAME   "check_bufferManager.log"
#ifdef LONG_TEST

#define THREAD_COUNT 200
#define NUM_PAGES (MAX_BUFFER_SIZE * 2)  // Otherwise, we run out of disk cache, and it takes forever to complete...
#define PAGE_MULT 10                     // This tells the system to only use every 10'th page, allowing us to quickly check >2 GB, >4 GB safeness.

#define READS_PER_THREAD (NUM_PAGES * 5)
#define RECORDS_PER_THREAD (NUM_PAGES * 2)


#else 
#define THREAD_COUNT 25
#define NUM_PAGES (MAX_BUFFER_SIZE * 3)
#define PAGE_MULT 1000

#define READS_PER_THREAD (NUM_PAGES * 5)
#define RECORDS_PER_THREAD (NUM_PAGES * 5)

#endif

#define MAX_TRANS_LENGTH 100 // Number of writes per transaction.  Keeping this low allows truncation.

void initializePages() {
  
  int i; 

  printf("Initialization starting\n"); fflush(NULL);

  for(i = 0 ; i < NUM_PAGES; i++) {
    Page * p;
    recordid rid;
    rid.page = PAGE_MULT * (i+1);
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
    if(! (i % (READS_PER_THREAD / 10)) ) {
      printf("%d", i / (READS_PER_THREAD / 10)); fflush(NULL);
    }

    rid.page = PAGE_MULT * (k+1);
    rid.slot = 0;
    rid.size = sizeof(int);

    p = loadPage(-1, rid.page);
    
    readRecord(1, p, rid, &j);

    releasePage(p);

    assert(k == j);
    
  }

  return NULL;
}
static pthread_mutex_t ralloc_mutex;
void * workerThreadWriting(void * q) {

  int offset = *(int*)q;
  recordid rids[RECORDS_PER_THREAD];

  int xid = Tbegin();
  int num_ops = 0;

  for(int i = 0 ; i < RECORDS_PER_THREAD; i++) {

    rids[i] = Talloc(xid, sizeof(int));
    /*    printf("\nRID:\t%d,%d\n", rids[i].page, rids[i].slot);  */
    /*  fflush(NULL);  */

    if(! (i % (RECORDS_PER_THREAD/10)) ) {
      printf("A%d", i / (RECORDS_PER_THREAD/10)); fflush(NULL);

    }

    if(num_ops == MAX_TRANS_LENGTH) { 
      num_ops = 0;
      Tcommit(xid);
      xid = Tbegin();
    } else { 
      num_ops++;
    }
    /*    sched_yield(); */
  }
  for(int i = 0;  i < RECORDS_PER_THREAD; i++) {
    int val = (i * 10000) + offset;
    int k;
    Page * p = loadPage(xid, rids[i].page);

    assert(p->id == rids[i].page);

    for(k = 0; k < 100; k++) {
      assert(p->id == rids[i].page);
    }
    
    /*    sched_yield(); */
    writeRecord(1, p, 0, rids[i], &val); 

    assert(p->id == rids[i].page);
    /*    p->LSN = 0;
     *lsn_ptr(p) = 0;  */
    /*    printf("LSN: %ld, %ld\n", p->LSN, *lsn_ptr(p)); */
    releasePage(p);

    if(! (i % (RECORDS_PER_THREAD/10)) ) {
      printf("W%d", i / (RECORDS_PER_THREAD/10)); fflush(NULL);
    }

    /*    sched_yield(); */
  }
  for(int i = 0;  i < RECORDS_PER_THREAD; i++) {
    int val;
    Page * p;


    p = loadPage(xid, rids[i].page);

    readRecord(1, p, rids[i], &val); 

    /*    p->LSN = 0;
     *lsn_ptr(p) = 0;  */
    /* printf("LSN: %ld, %ld\n", p->LSN, *lsn_ptr(p));*/
    releasePage(p);

    if(! (i % (RECORDS_PER_THREAD/10))) {
      printf("R%d", i / (RECORDS_PER_THREAD/10)); fflush(NULL);
    }


    assert(val == (i * 10000)+offset);

    /*    sched_yield(); */
  }
  
  Tcommit(xid);

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
  pthread_t workers[THREAD_COUNT];
  int i;

  Tinit();
  pthread_mutex_init(&ralloc_mutex, NULL);
  for(i = 0; i < THREAD_COUNT; i++) {
    int * j = malloc(sizeof(int));
    *j = i;
    pthread_create(&workers[i], NULL, workerThreadWriting, j);
  }
  for(i = 0; i < THREAD_COUNT; i++) {
    pthread_join(workers[i], NULL);
  }
  pthread_mutex_destroy(&ralloc_mutex);
  Tdeinit();
}END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("bufferManager");
  /* Begin a new test */
  TCase *tc = tcase_create("multithreaded");
  tcase_set_timeout(tc, 0); // disable timeouts
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
