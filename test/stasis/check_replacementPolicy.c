#include "../check_includes.h"

#include <stasis/transactional.h>
#include <stasis/replacementPolicy.h>
#include <stasis/util/random.h>

#include <assert.h>

#define LOG_NAME   "check_replacementPolicy.log"

#define OBJECT_COUNT 100
#define OP_COUNT     10000000

#define LONG_COUNT  100000000UL
#define SHORT_COUNT 10000000UL

#define THREAD_COUNT 10

typedef struct tracker {
  int inCache;
  pthread_mutex_t mut;
} tracker;

static pthread_mutex_t cached_count_mutex = PTHREAD_MUTEX_INITIALIZER;
static int threaded = 0;
static int cachedCount = 0;

tracker * t;
Page* pages;
void randomSetup() {
  time_t seed = time(0);
  printf("\nSeed = %ld\n", seed);
  srandom(seed);

  cachedCount = 0;

  t = calloc(OBJECT_COUNT, sizeof(tracker));
  pages = calloc(OBJECT_COUNT, sizeof(Page));
  for(int i = 0; i < OBJECT_COUNT; i++) {
    pages[i].id = i;
    pages[i].pinCount = 1;
    pages[i].next = 0;
  }

}
void randomTeardown() {
  free(t);
  free(pages);
}

void randomTest(replacementPolicy * lru, unsigned long count) {

  unsigned long progress_indicator = count / 10;

  for(unsigned long j = 0; j < count /*100000000UL*/; j++) {
    if(0 == (j % progress_indicator)) { printf("."); fflush(stdout); }
    int op = stasis_util_random64(100);

    int i = stasis_util_random64(OBJECT_COUNT);
    if(op < 10) {
      // TOGGLE IN CACHE
      pthread_mutex_lock(&t[i].mut);
      if(!t[i].inCache) {
        lru->insert(lru, &pages[i]);
        t[i].inCache = 1;
        pthread_mutex_lock(&cached_count_mutex);
        cachedCount ++;
        pthread_mutex_unlock(&cached_count_mutex);
      } else {
        Page *p = lru->remove(lru, &pages[i]);
        assert(p == &pages[i]);
        t[i].inCache = 0;
        pthread_mutex_lock(&cached_count_mutex);
        cachedCount --;
        pthread_mutex_unlock(&cached_count_mutex);
      }
      pthread_mutex_unlock(&t[i].mut);
    } else if(op < 30) {
      // Get stale + remove
      Page *p = lru->getStale(lru);
      if( p ) {
        pthread_mutex_lock(&t[p->id].mut);
        if(t[p->id].inCache) {
          assert(p == &pages[p->id]);
          p = lru->remove(lru, p);
          assert(p == &pages[p->id]);
          t[p->id].inCache = 0;
          pthread_mutex_lock(&cached_count_mutex);
          if(!threaded) assert(cachedCount != 0);
          cachedCount --;
          pthread_mutex_unlock(&cached_count_mutex);
        }
        pthread_mutex_unlock(&t[p->id].mut);
      } else {
        if(!threaded) {
          assert(cachedCount == 0);
        }
      }
    } else if(op < 50) {
      // Get stale
      Page * p = lru->getStale(lru);
      if(p) {
        pthread_mutex_lock(&t[p->id].mut);
        if(!threaded) assert(t[p->id].inCache);
        assert(p == &pages[p->id]);
        if(!threaded) assert(cachedCount != 0);
        pthread_mutex_unlock(&t[p->id].mut);
      } else {
        if(!threaded) assert(cachedCount == 0);
      }
    } else {
      // Hit
      pthread_mutex_lock(&t[i].mut);
      if(t[i].inCache) lru->hit(lru, &pages[i]);
      pthread_mutex_unlock(&t[i].mut);
    }
  }
}

void fillThenEmptyTest(replacementPolicy *lru) {
  for(int i = 0; i < OBJECT_COUNT; i++) {
    lru->insert(lru, &pages[i]);
  }
  int j = 0;
  while(lru->getStaleAndRemove(lru)) {
    j++;
  }
  assert(0 == lru->getStaleAndRemove(lru));
  assert(0 == lru->getStale(lru));
  assert(j == OBJECT_COUNT);
  for(int i = 0; i < OBJECT_COUNT; i++) {
    lru->insert(lru, &pages[i]);
    lru->remove(lru, &pages[i]);
  }
  j = 0;
  while(lru->getStaleAndRemove(lru)) {
    j++;
  }
  assert(j == 0);
}

START_TEST(replacementPolicyLRURandomTest) {
  replacementPolicy * lru = lruFastInit();
  threaded = 0;
  randomSetup();
  randomTest(lru, LONG_COUNT);
  lru->deinit(lru);
  randomTeardown();
} END_TEST
START_TEST(replacementPolicyLRUFastRandomTest) {
  replacementPolicy * lru = lruFastInit();
  threaded = 0;
  randomSetup();
  randomTest(lru, LONG_COUNT);
  lru->deinit(lru);
  randomTeardown();
} END_TEST
START_TEST(replacementPolicyThreadsafeRandomTest) {
  replacementPolicy * lru = lruFastInit();
  replacementPolicy * tsLru = replacementPolicyThreadsafeWrapperInit(lru);
  threaded = 0;
  randomSetup();
  randomTest(tsLru, LONG_COUNT);
  tsLru->deinit(tsLru);
  randomTeardown();

} END_TEST
START_TEST(replacementPolicyConcurrentRandomTest) {
  int LRU_COUNT = OBJECT_COUNT / 51;
  replacementPolicy * lru[LRU_COUNT];
  for(int i = 0; i < LRU_COUNT; i++) {
    lru[i] = lruFastInit();
  }
  threaded = 0;
  replacementPolicy * cwLru = replacementPolicyConcurrentWrapperInit(lru, LRU_COUNT);
  randomSetup();
  randomTest(cwLru, SHORT_COUNT);
  cwLru->deinit(cwLru);
  randomTeardown();
} END_TEST
START_TEST(replacementPolicyClockRandomTest) {
  threaded = 0;
  randomSetup();
  replacementPolicy * lru = replacementPolicyClockInit(pages, OBJECT_COUNT);
  randomTest(lru, LONG_COUNT);
  lru->deinit(lru);
  randomTeardown();
} END_TEST
replacementPolicy * worker_lru;
unsigned long worker_count;
void * randomTestWorker(void * arg) {
  randomTest(worker_lru, worker_count);
  return 0;
}

START_TEST(replacementPolicyThreadsafeThreadTest) {
  replacementPolicy * lru = lruFastInit();
  replacementPolicy * tsLru = replacementPolicyThreadsafeWrapperInit(lru);
  threaded = 1;
  worker_lru = tsLru;
  worker_count = LONG_COUNT / THREAD_COUNT;
  pthread_t threads[THREAD_COUNT];
  randomSetup();
  for(int i = 0; i < THREAD_COUNT; i++) {
    pthread_create(&threads[i], 0, randomTestWorker, 0);
  }
  for(int i = 0; i < THREAD_COUNT; i++) {
    pthread_join(threads[i], 0);
  }
  tsLru->deinit(tsLru);
  randomTeardown();
} END_TEST
START_TEST(replacementPolicyConcurrentThreadTest) {
  int LRU_COUNT = OBJECT_COUNT / 51;
  replacementPolicy * lru[LRU_COUNT];
  for(int i = 0; i < LRU_COUNT; i++) {
    lru[i] = lruFastInit();
  }
  replacementPolicy * cwLru = replacementPolicyConcurrentWrapperInit(lru, LRU_COUNT);
  threaded = 1;
  worker_lru = cwLru;
  worker_count = LONG_COUNT / THREAD_COUNT;
  pthread_t threads[THREAD_COUNT];
  randomSetup();
  for(int i = 0; i < THREAD_COUNT; i++) {
    pthread_create(&threads[i], 0, randomTestWorker, 0);
  }
  for(int i = 0; i < THREAD_COUNT; i++) {
    pthread_join(threads[i], 0);
  }

  cwLru->deinit(cwLru);
  randomTeardown();
} END_TEST

START_TEST(replacementPolicyClockThreadTest) {
  replacementPolicy * clockLru = replacementPolicyClockInit(pages, OBJECT_COUNT);
  threaded = 1;
  worker_lru = clockLru;
  worker_count = LONG_COUNT / THREAD_COUNT;
  pthread_t threads[THREAD_COUNT];
  randomSetup();
  for(int i = 0; i < THREAD_COUNT; i++) {
    pthread_create(&threads[i], 0, randomTestWorker, 0);
  }
  for(int i = 0; i < THREAD_COUNT; i++) {
    pthread_join(threads[i], 0);
  }
  clockLru->deinit(clockLru);
  randomTeardown();
} END_TEST


START_TEST(replacementPolicyEmptyFastLRUTest) {
  randomSetup();
  replacementPolicy *rp = lruFastInit();
  fillThenEmptyTest(rp);
  rp->deinit(rp);
  randomTeardown();
} END_TEST
START_TEST(replacementPolicyEmptyThreadsafeTest) {
  randomSetup();
  replacementPolicy *rpA = lruFastInit();
  replacementPolicy *rp  = replacementPolicyThreadsafeWrapperInit(rpA);
  fillThenEmptyTest(rp);
  rp->deinit(rp);
  randomTeardown();
} END_TEST
START_TEST(replacementPolicyEmptyConcurrentTest) {
  randomSetup();
  replacementPolicy *rpA[THREAD_COUNT];
  for(int i = 0; i < THREAD_COUNT; i++) {
    rpA[i] = lruFastInit();
  }
  replacementPolicy *rp
    = replacementPolicyConcurrentWrapperInit(rpA, THREAD_COUNT);
  fillThenEmptyTest(rp);
  rp->deinit(rp);
  randomTeardown();
} END_TEST
START_TEST(replacementPolicyEmptyClockTest) {
  randomSetup();
  replacementPolicy *rp  = replacementPolicyClockInit(pages, OBJECT_COUNT);
  fillThenEmptyTest(rp);
  rp->deinit(rp);
  randomTeardown();
} END_TEST
Suite * check_suite(void) {
  Suite *s = suite_create("replacementPolicy");
  /* Begin a new test */
  TCase *tc = tcase_create("multithreaded");
  tcase_set_timeout(tc, 1200); // twenty minute timeout
  /* Sub tests are added, one per line, here */
  tcase_add_test(tc, replacementPolicyEmptyFastLRUTest);
  tcase_add_test(tc, replacementPolicyEmptyThreadsafeTest);
  tcase_add_test(tc, replacementPolicyEmptyConcurrentTest);
  tcase_add_test(tc, replacementPolicyEmptyClockTest);
  tcase_add_test(tc, replacementPolicyLRURandomTest);
  tcase_add_test(tc, replacementPolicyLRUFastRandomTest);
  tcase_add_test(tc, replacementPolicyThreadsafeRandomTest);
  tcase_add_test(tc, replacementPolicyConcurrentRandomTest);
  tcase_add_test(tc, replacementPolicyClockRandomTest);
  tcase_add_test(tc, replacementPolicyThreadsafeThreadTest);
  tcase_add_test(tc, replacementPolicyConcurrentThreadTest);
  tcase_add_test(tc, replacementPolicyClockThreadTest);


  /* --------------------------------------------- */

  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
