#include <check.h>

#include <config.h>
#include <lladd/transactional.h>
#include <lladd/replacementPolicy.h>

#include <assert.h>
#include "../check_includes.h"

#define LOG_NAME   "check_replacementPolicy.log"

long myrandom(long x) {
  double xx = x;
  double r = random();
  double max = ((uint64_t)RAND_MAX)+1;
  max /= xx;
  return (long)((r/max));
}

#define OBJECT_COUNT 1000
#define OP_COUNT     10000000

typedef struct tracker { 
  long key;
  int inCache;
} tracker;
START_TEST(replacementPolicyRandomTest) {
  time_t seed = time(0);
  printf("\nSeed = %ld\n", seed);
  srandom(seed);

  replacementPolicy * lru = lruInit();
  int cachedCount = 0;
  
  tracker * t = calloc(OBJECT_COUNT, sizeof(tracker));
  for(int i = 0; i < OBJECT_COUNT; i++) { 
    t[i].key = i;
  }
  for(unsigned long j = 0; j < 100000000UL; j++) { 
    int op = myrandom(100);

    int i = myrandom(OBJECT_COUNT);
    if(op < 10) { 
      // TOGGLE IN CACHE
      if(!t[i].inCache) { 
	lru->insert(lru, t[i].key, &t[i]);
	t[i].inCache = 1;
	cachedCount ++;
      } else { 
	void * v = lru->remove(lru, t[i].key);
	assert(v == &t[i]);
	t[i].inCache = 0;
	cachedCount --;
      }
    } else if(op < 30) { 
      // Get stale + remove
      tracker * tr = lru->getStale(lru);
      if( tr ) { 
	assert(tr->inCache);
	assert(tr == &t[tr->key]);
	tr = lru->remove(lru, tr->key);
	assert(tr == &t[tr->key]);
	tr->inCache = 0;
	assert(cachedCount != 0);
	cachedCount --;
      } else { 
	assert(cachedCount == 0);
      }
    } else if(op < 50) { 
      // Get stale
      tracker * tr = lru->getStale(lru);
      if(tr) { 
	assert(tr->inCache);
	assert(tr == &t[tr->key]);
	assert(cachedCount != 0);
      } else { 
	assert(cachedCount == 0);
      }
    } else { 
      // Hit
      if(t[i].inCache) lru->hit(lru, t[i].key);
    }
  }


} END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("replacemenPolicy");
  /* Begin a new test */
  TCase *tc = tcase_create("multithreaded");
  tcase_set_timeout(tc, 1200); // twenty minute timeout
  /* Sub tests are added, one per line, here */
  tcase_add_test(tc, replacementPolicyRandomTest); 

  /* --------------------------------------------- */
  
  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
