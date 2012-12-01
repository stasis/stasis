#include "../check_includes.h"

#include <stasis/transactional.h>
#include <stasis/util/latches.h>
#include <stasis/page.h>
#include <stasis/bufferManager.h>
#include <stasis/bufferManager/bufferHash.h>
#include <stasis/bufferManager/concurrentBufferManager.h>
#include <stasis/bufferManager/legacy/legacyBufferManager.h>
#include <stasis/util/random.h>
#include <sched.h>
#include <assert.h>

#define LOG_NAME   "check_bufferManager.log"
//#define DBUG_TEST
#ifdef LONG_TEST

#define THREAD_COUNT 100
#define NUM_PAGES ((stasis_buffer_manager_size * 3)/2)  // Otherwise, we run out of disk cache, and it takes forever to complete...
#define PAGE_MULT 10                     // This tells the system to only use every 10'th page, allowing us to quickly check >2 GB, >4 GB safeness.

#define READS_PER_THREAD (NUM_PAGES * 5)
#define RECORDS_PER_THREAD (NUM_PAGES * 5)


#else
#ifdef DBUG_TEST

#define THREAD_COUNT 2
#define NUM_PAGES 10
#define PAGE_MULT 1

#define READS_PER_THREAD (NUM_PAGES * 2)
#define RECORDS_PER_THREAD (NUM_PAGES * 2)

#else
#define THREAD_COUNT 25
#define NUM_PAGES ((stasis_buffer_manager_size * 3)/2)
#define PAGE_MULT 1000

#define READS_PER_THREAD (NUM_PAGES * 2)
#define RECORDS_PER_THREAD (NUM_PAGES * 2)

#endif // DBUG_TEST
#endif // LONG_TEST
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
    writelock(p->rwlatch,0);
    stasis_page_slotted_initialize_page(p);
    stasis_record_alloc_done(-1, p, rid);
    int * buf = (int*)stasis_record_write_begin(-1, p, rid);
    *buf = i;
    stasis_record_write_done(-1,p,rid,(void*)buf);
    stasis_page_lsn_write(-1, p, 0);
    unlock(p->rwlatch);
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
      printf("%lld", i / ((unsigned long long)READS_PER_THREAD / 10)); fflush(NULL);
    }

    rid.page = PAGE_MULT * (k+1);
    rid.slot = 0;
    rid.size = sizeof(int);

    p = loadPage(-1, rid.page);
    readlock(p->rwlatch,0);
    stasis_record_read(1, p, rid, (byte*)&j);
    unlock(p->rwlatch);
    releasePage(p);

    assert(k == j);

  }

  return NULL;
}
static pthread_mutex_t ralloc_mutex;
void * workerThreadWriting(void * q) {

  int offset = *(int*)q;
  recordid * rids;
  rids = stasis_malloc(RECORDS_PER_THREAD, recordid);

  int xid = Tbegin();
  int num_ops = 0;

  for(int i = 0 ; i < RECORDS_PER_THREAD; i++) {

    rids[i] = Talloc(xid, sizeof(int));
    /*    printf("\nRID:\t%d,%d\n", rids[i].page, rids[i].slot);  */
    /*  fflush(NULL);  */

    if(! (i % (RECORDS_PER_THREAD/10)) ) {
      printf("A%lld", i / ((unsigned long long)RECORDS_PER_THREAD/10)); fflush(NULL);

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
    writelock(p->rwlatch,0);
    /*    sched_yield(); */
    assert(stasis_record_length_read(xid,p,rids[i]) == sizeof(int));
    lsn_t lsn = stasis_page_lsn_read(p);
    assert(lsn);
    p->LSN --; // XXX HACK -- Sooner or later this will break...
    stasis_record_write(1, p, rids[i], (byte*)&val);
    stasis_page_lsn_write(1,p,lsn);
    unlock(p->rwlatch);

    assert(p->id == rids[i].page);
    releasePage(p);

    if(! (i % (RECORDS_PER_THREAD/10)) ) {
      printf("W%lld", i / ((unsigned long long)RECORDS_PER_THREAD/10)); fflush(NULL);
    }

    /*    sched_yield(); */
  }
  for(int i = 0;  i < RECORDS_PER_THREAD; i++) {
    int val;
    Page * p;


    p = loadPage(xid, rids[i].page);
    assert(p->id == rids[i].page);
    writelock(p->rwlatch,0);
    assert(stasis_record_length_read(xid,p,rids[i]) == sizeof(int));
    stasis_record_read(1, p, rids[i], (byte*)&val);
    unlock(p->rwlatch);

    releasePage(p);

    if(! (i % (RECORDS_PER_THREAD/10))) {
      printf("R%lld", i / ((unsigned long long)RECORDS_PER_THREAD/10)); fflush(NULL);
    }


    assert(val == (i * 10000)+offset);

    /*    sched_yield(); */
  }

  Tcommit(xid);
  free(rids);
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
    int * j = stasis_alloc(int);
    *j = i;
    pthread_create(&workers[i], NULL, workerThreadWriting, j);
  }
  for(i = 0; i < THREAD_COUNT; i++) {
    pthread_join(workers[i], NULL);
  }
  pthread_mutex_destroy(&ralloc_mutex);
  Tdeinit();
}END_TEST

 //#define PINNED_PAGE_COUNT (stasis_buffer_manager_size - 1)
#define PINNED_PAGE_COUNT ((stasis_buffer_manager_size - 1) / THREAD_COUNT)
#define MAX_PAGE_ID  (stasis_buffer_manager_size * 2)
#ifdef LONG_TEST
#define BLIND_ITERS 100000
#else
#define BLIND_ITERS 1000000
#endif
void * blindRandomWorker(void * v) {
  //  int idx = *(int*)v;  /// Don't need index; want pinned pages to overlap!

  pageid_t * pageids = stasis_malloc(PINNED_PAGE_COUNT, pageid_t);
  Page ** pages = stasis_calloc(PINNED_PAGE_COUNT, Page*);

  for(int i = 0; i < PINNED_PAGE_COUNT; i++) {
    pageids[i] = -1;
  }

  for(int i = 0; i < BLIND_ITERS; i ++) {
    int j = stasis_util_random64(PINNED_PAGE_COUNT);
    if(pageids[j] == -1) {
      pageids[j] = stasis_util_random64(MAX_PAGE_ID);
      pages[j] = loadPage(-1, pageids[j]);
      assert(pages[j]->id == pageids[j]);
    } else {
      //      stasis_dirty_page_table_set_dirty(pages[j]); // Shouldn't affect buffermanager too much...
      assert(pages[j]->id == pageids[j]);
      releasePage(pages[j]);
      pageids[j] = -1;
      pages[j] = 0;
    }
  }
  for(int i =0 ; i < PINNED_PAGE_COUNT; i++) {
    if(pages[i]) {
      releasePage(pages[i]);
      pages[i] = 0;
      pageids[i] = -1;
    }
  }
  return 0;
}

START_TEST(pageBlindRandomTest) {

  Tinit();

  blindRandomWorker(0);

  Tdeinit();
} END_TEST
START_TEST(pageBlindThreadTest) {

  Tinit();

  printf("Spawning threads now.\n"); fflush(stdout);

  printf("each thread pins %lld pages (total: %lld)\n", (unsigned long long)PINNED_PAGE_COUNT, (unsigned long long)PINNED_PAGE_COUNT*THREAD_COUNT);

  pthread_t workers[THREAD_COUNT];

  for(int i = 0; i < THREAD_COUNT; i++) {
    pthread_create(&workers[i], NULL, blindRandomWorker, NULL);
  }
  for(int i = 0; i < THREAD_COUNT; i++) {
    pthread_join(workers[i], NULL);
  }

  Tdeinit();
} END_TEST

static void stalePinTestImpl(stasis_buffer_manager_t * (*fact)(stasis_log_t*, stasis_dirty_page_table_t*)) {
  stasis_buffer_manager_t * (*old_fact)(stasis_log_t*, stasis_dirty_page_table_t*) = stasis_buffer_manager_factory;
  stasis_buffer_manager_factory = fact;

  Tinit();

  Page * p[stasis_buffer_manager_size-1];
  for(int i = 0; i < stasis_buffer_manager_size-2; i++) {
    p[i] = loadUninitializedPage(-1, i);
  }
  for(int i = 0; i < stasis_buffer_manager_size; i++) {
    Page * foo = loadUninitializedPage(-1, i+stasis_buffer_manager_size);
    releasePage(foo);
  }
  for(int i = 0; i < stasis_buffer_manager_size-2; i++) {
    releasePage(p[i]);
  }
  Tdeinit();

  stasis_buffer_manager_factory = old_fact;
}
START_TEST(prefetchTest) {
  Tinit();
  prefetchPages(100, 100);
  Tdeinit();
} END_TEST
START_TEST(stalePinTest) {
  stalePinTestImpl(stasis_buffer_manager_hash_factory);
} END_TEST
START_TEST(stalePinTestConcurrentBufferManager) {
  stalePinTestImpl(stasis_buffer_manager_concurrent_hash_factory);
} END_TEST
//START_TEST(stalePinTestDeprecatedBufferManager) {
//  stalePinTestImpl(stasis_buffer_manager_deprecated_factory);
//} END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("bufferManager");
  /* Begin a new test */
  TCase *tc = tcase_create("multithreaded");
#ifndef DBUG_TEST
  tcase_set_timeout(tc, 3*3600); // three hour timeout
  /* Sub tests are added, one per line, here */
  tcase_add_test(tc, stalePinTest);
  tcase_add_test(tc, prefetchTest);
  //  tcase_add_test(tc, stalePinTestDeprecatedBufferManager); // Fails; do not intend to fix.
  tcase_add_test(tc, pageSingleThreadTest);
  tcase_add_test(tc, pageSingleThreadWriterTest);
#else
  stasis_buffer_manager_size = 20;
  stasis_truncation_automatic = 0;
#endif
  tcase_add_test(tc, pageLoadTest);
#ifndef DBUG_TEST
  tcase_add_test(tc, pageThreadedWritersTest);
  tcase_add_test(tc, pageBlindRandomTest);
  tcase_add_test(tc, stalePinTestConcurrentBufferManager);
  tcase_add_test(tc, pageBlindThreadTest);
#endif
  /* --------------------------------------------- */

  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
