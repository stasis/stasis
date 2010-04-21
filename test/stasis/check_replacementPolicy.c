#include "../check_includes.h"

#include <stasis/transactional.h>
#include <stasis/replacementPolicy.h>
#include <stasis/doubleLinkedList.h>

#include <assert.h>

#define LOG_NAME   "check_replacementPolicy.log"

#define OBJECT_COUNT 1000
#define OP_COUNT     10000000

#define LONG_COUNT  100000000UL
#define SHORT_COUNT 10000000UL

#define THREAD_COUNT 10

typedef struct LL_ENTRY(node_t) node_t;

typedef struct tracker {
  pageid_t val; // Must be pageid_t.  Otherwise, there will be mismatches inside of concurrentWrapper's hash.
  intptr_t key;
  intptr_t pinCount;
  int inCache;
  pthread_mutex_t mut;
} tracker;
static pthread_mutex_t cached_count_mutex = PTHREAD_MUTEX_INITIALIZER;
static int threaded = 0;
static int cachedCount = 0;

static node_t * getKey(void * page, void * ignore) {
  tracker * p = page;
  return (node_t*)p->key;
}
static void setKey(void * page, node_t * n, void * ignore) {
  tracker * p = page;
  p->key = (intptr_t) n;
}
static intptr_t * pinCount(void *page) {
  tracker * p = page;
  return &p->pinCount;
}


tracker * t;
void randomSetup() {
  time_t seed = time(0);
  printf("\nSeed = %ld\n", seed);
  srandom(seed);

  cachedCount = 0;

  t = calloc(OBJECT_COUNT, sizeof(tracker));
  for(int i = 0; i < OBJECT_COUNT; i++) {
    t[i].val = i;
    t[i].pinCount = 1;
  }

}
void randomTeardown() {
  free(t);
}

void randomTest(replacementPolicy * lru, unsigned long count) {

  unsigned long progress_indicator = count / 10;

  for(unsigned long j = 0; j < count /*100000000UL*/; j++) {
    if(0 == (j % progress_indicator)) { printf("."); fflush(stdout); }
    int op = myrandom(100);

    int i = myrandom(OBJECT_COUNT);
    if(op < 10) {
      // TOGGLE IN CACHE
      pthread_mutex_lock(&t[i].mut);
      if(!t[i].inCache) {
        lru->insert(lru, &t[i]);
        t[i].inCache = 1;
        pthread_mutex_lock(&cached_count_mutex);
        cachedCount ++;
        pthread_mutex_unlock(&cached_count_mutex);
      } else {
        void * v = lru->remove(lru, &t[i]);
        assert(v == &t[i]);
        t[i].inCache = 0;
        pthread_mutex_lock(&cached_count_mutex);
        cachedCount --;
        pthread_mutex_unlock(&cached_count_mutex);
      }
      pthread_mutex_unlock(&t[i].mut);
    } else if(op < 30) {
      // Get stale + remove
      tracker * tr = lru->getStale(lru);
      if( tr ) {
        pthread_mutex_lock(&t[tr->val].mut);
        if(tr->inCache) {
          assert(tr == &t[tr->val]);
          tr = lru->remove(lru, tr);
          assert(tr == &t[tr->val]);
          tr->inCache = 0;
          pthread_mutex_lock(&cached_count_mutex);
          if(!threaded) assert(cachedCount != 0);
          cachedCount --;
          pthread_mutex_unlock(&cached_count_mutex);
        }
        pthread_mutex_unlock(&t[tr->val].mut);
      } else {
        if(!threaded) {
          assert(cachedCount == 0);
        }
      }
    } else if(op < 50) {
      // Get stale
      tracker * tr = lru->getStale(lru);
      if(tr) {
        pthread_mutex_lock(&t[tr->val].mut);
        if(!threaded) assert(tr->inCache);
        assert(tr == &t[tr->val]);
        if(!threaded) assert(cachedCount != 0);
        pthread_mutex_unlock(&t[tr->val].mut);
      } else {
        if(!threaded) assert(cachedCount == 0);
      }
    } else {
      // Hit
      pthread_mutex_lock(&t[i].mut);
      if(t[i].inCache) lru->hit(lru, &t[i]);
      pthread_mutex_unlock(&t[i].mut);
    }
  }
}

void fillThenEmptyTest(replacementPolicy *lru) {
  for(int i = 0; i < OBJECT_COUNT; i++) {
    lru->insert(lru, &t[i]);
  }
  int j = 0;
  while(lru->getStaleAndRemove(lru)) {
    j++;
  }
  assert(0 == lru->getStaleAndRemove(lru));
  assert(0 == lru->getStale(lru));
  assert(j == OBJECT_COUNT);
  for(int i = 0; i < OBJECT_COUNT; i++) {
    lru->insert(lru, &t[i]);
    lru->remove(lru, &t[i]);
  }
  j = 0;
  while(lru->getStaleAndRemove(lru)) {
    j++;
  }
  assert(j == 0);
}

START_TEST(replacementPolicyLRURandomTest) {
  replacementPolicy * lru = lruFastInit(getKey, setKey, pinCount, 0);
  threaded = 0;
  randomSetup();
  randomTest(lru, LONG_COUNT);
  lru->deinit(lru);
  randomTeardown();
} END_TEST
START_TEST(replacementPolicyLRUFastRandomTest) {
  replacementPolicy * lru = lruFastInit(getKey, setKey, pinCount, 0);
  threaded = 0;
  randomSetup();
  randomTest(lru, LONG_COUNT);
  lru->deinit(lru);
  randomTeardown();
} END_TEST
START_TEST(replacementPolicyThreadsafeRandomTest) {
  replacementPolicy * lru = lruFastInit(getKey, setKey, pinCount, 0);
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
    lru[i] = lruFastInit(getKey, setKey, pinCount, 0);
  }
  threaded = 0;
  replacementPolicy * cwLru = replacementPolicyConcurrentWrapperInit(lru, LRU_COUNT);
  randomSetup();
  randomTest(cwLru, SHORT_COUNT);
  cwLru->deinit(cwLru);
  randomTeardown();
} END_TEST

replacementPolicy * worker_lru;
unsigned long worker_count;
void * randomTestWorker(void * arg) {
  randomTest(worker_lru, worker_count);
  return 0;
}

START_TEST(replacementPolicyThreadsafeThreadTest) {
  replacementPolicy * lru = lruFastInit(getKey, setKey, pinCount, 0);
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
    lru[i] = lruFastInit(getKey, setKey, pinCount, 0);
  }
  replacementPolicy * cwLru = replacementPolicyConcurrentWrapperInit(lru, THREAD_COUNT);
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
START_TEST(replacementPolicyEmptyFastLRUTest) {
  randomSetup();
  replacementPolicy *rp = lruFastInit(getKey, setKey, pinCount, 0);
  fillThenEmptyTest(rp);
  rp->deinit(rp);
  randomTeardown();
} END_TEST
START_TEST(replacementPolicyEmptyThreadsafeTest) {
  randomSetup();
  replacementPolicy *rpA = lruFastInit(getKey, setKey, pinCount, 0);
  replacementPolicy *rp  = replacementPolicyThreadsafeWrapperInit(rpA);
  fillThenEmptyTest(rp);
  rp->deinit(rp);
  randomTeardown();
} END_TEST
START_TEST(replacementPolicyEmptyConcurrentTest) {
  randomSetup();
  replacementPolicy *rpA[THREAD_COUNT];
  for(int i = 0; i < THREAD_COUNT; i++) {
    rpA[i] = lruFastInit(getKey, setKey, pinCount, 0);
  }
  replacementPolicy *rp
    = replacementPolicyConcurrentWrapperInit(rpA, THREAD_COUNT);
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
  tcase_add_test(tc, replacementPolicyLRURandomTest);
  tcase_add_test(tc, replacementPolicyLRUFastRandomTest);
  tcase_add_test(tc, replacementPolicyThreadsafeRandomTest);
  tcase_add_test(tc, replacementPolicyConcurrentRandomTest);
  tcase_add_test(tc, replacementPolicyThreadsafeThreadTest);
  tcase_add_test(tc, replacementPolicyConcurrentThreadTest);


  /* --------------------------------------------- */

  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
