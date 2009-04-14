#include "../check_includes.h"

#include <stasis/transactional.h>
#include <stasis/lockManager.h>

#include <pthread.h>
#include <assert.h>
#include <stdlib.h>

#define LOG_NAME   "check_lockManager.log"

/** Needs to be formatted as a floating point */
#define NUM_RECORDS 5000000.0
#define THREAD_COUNT 100
#define RIDS_PER_THREAD 1000

void * pageWorkerThread(void * j) {
  int xid = *(int*)j;
  globalLockManager.begin(xid);
  recordid rid;
  rid.page = 0;
  rid.size = 0;
  int k;
  int deadlocks = 0;
  for(k = 0; k < RIDS_PER_THREAD; k++) {
    int m =  (int) (NUM_RECORDS*random()/(RAND_MAX+1.0));
    int rw = random() % 2;

    if(rw) {
      // readlock
      int locked = 0;
      if(LLADD_DEADLOCK == globalLockManager.readLockPage(xid, m)) {
	k = 0;
	assert(compensation_error() == LLADD_DEADLOCK);
	compensation_clear_error();
	globalLockManager.abort(xid);
	deadlocks++;
	locked = 1;
      }
    } else {

      if(LLADD_DEADLOCK == globalLockManager.writeLockPage(xid, m)) {
	k = 0;
	globalLockManager.abort(xid);
	deadlocks++;
	int err = compensation_error();
	assert(err == LLADD_DEADLOCK);
	compensation_clear_error();
      }
    }
  }

  printf("%2d ", deadlocks); fflush(stdout);

  globalLockManager.commit(xid);

  return NULL;

}
void * ridWorkerThread(void * j) {

  int xid = *(int*)j;
  globalLockManager.begin(xid);
  recordid rid;
  rid.page = 0;
  rid.size = 0;
  int k;
  int deadlocks = 0;
  for(k = 0; k < RIDS_PER_THREAD; k++) {
    rid.slot = (int) (NUM_RECORDS*random()/(RAND_MAX+1.0));
    int rw = random() % 2;

    if(rw) {
      // readlock

      if(LLADD_DEADLOCK == globalLockManager.readLockRecord(xid, rid)) {
	k = 0;
	globalLockManager.abort(xid);
	deadlocks++;
      }

    } else {
      // writelock

      if(LLADD_DEADLOCK == globalLockManager.writeLockRecord(xid, rid)) {
	k = 0;
	globalLockManager.abort(xid);
	deadlocks++;
      }
    }
  }

  printf("%2d ", deadlocks); fflush(stdout);

  globalLockManager.commit(xid);

  return NULL;

}

START_TEST(recordidLockManagerTest) {
  printf("\n");


  setupLockManagerCallbacksRecord();

  pthread_t workers[THREAD_COUNT];
  int i;
  printf("The following numbers are deadlock counts for each thread.\n");
  for(i = 0; i < THREAD_COUNT; i++) {
    int *j = malloc(sizeof(int));
    *j = i;
    pthread_create(&workers[i], NULL, ridWorkerThread, j);
  }
  for(i = 0; i < THREAD_COUNT; i++) {
    pthread_join(workers[i], NULL);
  }
} END_TEST

START_TEST(pageLockManagerTest) {
  printf("\n");


  setupLockManagerCallbacksPage();

  pthread_t workers[THREAD_COUNT];
  int i;
  printf("The following numbers are deadlock counts for each thread.\n");
  for(i = 0; i < THREAD_COUNT; i++) {
    int *j = malloc(sizeof(int));
    *j = i;
    pthread_create(&workers[i], NULL, pageWorkerThread, j);
  }
  for(i = 0; i < THREAD_COUNT; i++) {
    pthread_join(workers[i], NULL);
  }

} END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("lockManager");
  /* Begin a new test */
  TCase *tc = tcase_create("multithreaded");
  tcase_set_timeout(tc, 0); // disable timeouts
  // kinda hacky, but here it is:
  compensations_init();

  /* Sub tests are added, one per line, here */

  tcase_add_test(tc, recordidLockManagerTest);
  tcase_add_test(tc, pageLockManagerTest);

  /* --------------------------------------------- */

  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
