#include <check.h>
#include <assert.h>
#include <lladd/transactional.h>
#define LOG_NAME   "check_rangeTracker.log"

#include <lladd/io/rangeTracker.h>

#include "../check_includes.h"

#include <sys/time.h>
#include <time.h>

long myrandom(long x) {
  double xx = x;
  double r = random();
  double max = ((uint64_t)RAND_MAX)+1;
  max /= xx;
  return (long)((r/max));
}

void printRT(rangeTracker * rt) { 
  const transition ** ts = rangeTrackerEnumerate(rt);
  int i = 0;
  printf("Range tracker:\n");
  while(ts[i]) { 
    printf("%s\n", transitionToString(ts[i]));
    i++;
  }
  free (ts);
}

START_TEST(rangeTracker_smokeTest) {
  rangeTracker * rt = rangeTrackerInit(512);
 
  const transition ** ts = rangeTrackerEnumerate(rt);

  assert(ts[0] == 0);

  free(ts);

  //  printRT(rt);


  range r;
  r.start = 10;
  r.stop   = 100;

  rangeTrackerAdd(rt, &r);

  //  printRT(rt);

  ts = rangeTrackerEnumerate(rt);

  //  10   20             80   90  100 
  //  |    |              |    |   |
  //  |1111|11111111111111|1111|111|
  //  |    |              |    |   |

  assert(ts[0]->pos == 10 && ts[0]->delta ==  1 && ts[0]->pins ==  0 &&
	 ts[1]->pos == 100&& ts[1]->delta == -1 && ts[1]->pins ==  1 && !ts[2]);
	 

  r.start = 20;
  r.stop = 80;

  rangeTrackerRemove(rt, &r);

  //  printRT(rt);

  ts = rangeTrackerEnumerate(rt);

  //  10   20             80   90  100 
  //  |    |              |    |   |
  //  |1111|              |1111|111|
  //  |    |              |    |   |

  assert(ts[0]->pos == 10 && ts[0]->delta ==  1 && ts[0]->pins ==  0 &&
	 ts[1]->pos == 20 && ts[1]->delta == -1 && ts[1]->pins ==  1 &&
	 ts[2]->pos == 80 && ts[2]->delta ==  1 && ts[2]->pins ==  0 &&
	 ts[3]->pos == 100&& ts[3]->delta == -1 && ts[3]->pins ==  1 && !ts[4]);


  rangeTrackerAdd(rt, &r);

  //  printRT(rt);

  ts = rangeTrackerEnumerate(rt);

  //  10   20             80   90  100 
  //  |    |              |    |   |
  //  |1111|11111111111111|1111|111|
  //  |    |              |    |   |


  assert(ts[0]->pos == 10 && ts[0]->delta ==  1 && ts[0]->pins ==  0 &&
	 ts[1]->pos == 100&& ts[1]->delta == -1 && ts[1]->pins ==  1 && !ts[2]);
	 
  rangeTrackerAdd(rt, &r);

  //  printRT(rt);

  ts = rangeTrackerEnumerate(rt);

  //  10   20             80   90  100 
  //  |    |              |    |   |
  //  |1111|22222222222222|1111|111|
  //  |    |              |    |   |

  assert(ts[0]->pos == 10 && ts[0]->delta ==  1 && ts[0]->pins ==  0 &&
	 ts[1]->pos == 20 && ts[1]->delta ==  1 && ts[1]->pins ==  1 &&
	 ts[2]->pos == 80 && ts[2]->delta == -1 && ts[2]->pins ==  2 &&
	 ts[3]->pos == 100&& ts[3]->delta == -1 && ts[3]->pins ==  1 && !ts[4]);

	 
  rangeTrackerRemove(rt, &r);
  ts = rangeTrackerEnumerate(rt);

  //  10   20             80   90  100 
  //  |    |              |    |   |
  //  |1111|11111111111111|1111|111|
  //  |    |              |    |   |


  assert(ts[0]->pos == 10 && ts[0]->delta ==  1 && ts[0]->pins ==  0 &&
	 ts[1]->pos == 100&& ts[1]->delta == -1 && ts[1]->pins ==  1 && !ts[2]);
  rangeTrackerRemove(rt, &r);

  //  printRT(rt);

  ts = rangeTrackerEnumerate(rt);

  //  10   20             80   90  100 
  //  |    |              |    |   |
  //  |1111|              |1111|111|
  //  |    |              |    |   |

  assert(ts[0]->pos == 10 && ts[0]->delta ==  1 && ts[0]->pins ==  0 &&
	 ts[1]->pos == 20 && ts[1]->delta == -1 && ts[1]->pins ==  1 &&
	 ts[2]->pos == 80 && ts[2]->delta ==  1 && ts[2]->pins ==  0 &&
	 ts[3]->pos == 100&& ts[3]->delta == -1 && ts[3]->pins ==  1 && !ts[4]);

  r.start = 80;
  r.stop = 90;


  
  rangeTrackerAdd(rt, &r);

  //  printRT(rt);

  ts = rangeTrackerEnumerate(rt);

  //  10   20             80   90  100 
  //  |    |              |    |   |
  //  |1111|              |2222|111|
  //  |    |              |    |   |


  assert(ts[0]->pos == 10 && ts[0]->delta ==  1 && ts[0]->pins ==  0 &&
	 ts[1]->pos == 20 && ts[1]->delta == -1 && ts[1]->pins ==  1 &&
	 ts[2]->pos == 80 && ts[2]->delta ==  2 && ts[2]->pins ==  0 &&
	 ts[3]->pos == 90 && ts[3]->delta == -1 && ts[3]->pins ==  2 &&
	 ts[4]->pos == 100&& ts[4]->delta == -1 && ts[4]->pins ==  1 && !ts[5]);

  r.start = 80;
  r.stop = 100;
  rangeTrackerRemove(rt, &r);

  //  printRT(rt);
  ts = rangeTrackerEnumerate(rt);

  //  10   20             80   90  100 
  //  |    |              |    |   |
  //  |1111|              |1111|   |
  //  |    |              |    |   |


  assert(ts[0]->pos == 10 && ts[0]->delta ==  1 && ts[0]->pins ==  0 &&
	 ts[1]->pos == 20 && ts[1]->delta == -1 && ts[1]->pins ==  1 &&
	 ts[2]->pos == 80 && ts[2]->delta ==  1 && ts[2]->pins ==  0 &&
	 ts[3]->pos == 90 && ts[3]->delta == -1 && ts[3]->pins ==  1 && !ts[4]);


  r.start = 10;
  r.stop = 20;
  rangeTrackerRemove(rt, &r);

  //  printRT(rt);
  ts = rangeTrackerEnumerate(rt);

  //  10   20             80   90  100 
  //  |    |              |    |   |
  //  |    |              |1111|   |
  //  |    |              |    |   |


  assert(ts[0]->pos == 80 && ts[0]->delta ==  1 && ts[0]->pins ==  0 &&
	 ts[1]->pos == 90 && ts[1]->delta == -1 && ts[1]->pins ==  1 && !ts[2]);


  r.start = 80;
  r.stop = 90;
  
  rangeTrackerRemove(rt, &r);

  //  printRT(rt);
  ts = rangeTrackerEnumerate(rt);

  //  10   20             80   90  100 
  //  |    |              |    |   |
  //  |    |              |    |   |
  //  |    |              |    |   |


  assert(!ts[0]); 
  rangeTrackerDeinit(rt);
}
END_TEST

#define RANGE_SIZE 1000
#define ITERATIONS 1000
#define RANGE_COUNT 100
void randomRange(range * r) { 
  long start = myrandom(RANGE_SIZE-1);
  long len = 1+myrandom(RANGE_SIZE - start - 1);

  r->start = start;
  r->stop = start + len;
  assert(r->start < RANGE_SIZE);
  assert(r->stop < RANGE_SIZE);
}

void checkRangeTrackerConsistency(rangeTracker * rt) {
  const transition ** t = rangeTrackerEnumerate(rt);
  
  int pins = 0;
  long pos = -1;
  int i = 0;
  while(t[i]) { 

    assert(pos < t[i]->pos);
    assert(pins == t[i]->pins);
    assert(pins >= 0);
    assert(t[i]->delta);
    
    pos = t[i]->pos;
    pins += t[i]->delta;
    
    i++;
  }
  
  assert(pins == 0);

  free(t);
}


START_TEST (rangeTracker_randomTest) { 

  struct timeval time; 

  gettimeofday(&time,0);
  
  long seed = time.tv_usec + time.tv_sec * 1000000;
  printf("\nSeed = %ld\n", seed);
  srandom(seed);

  range * ranges = malloc(sizeof(range) * RANGE_COUNT);
  int * pins = calloc(RANGE_COUNT, sizeof(int));
  rangeTracker * rt = rangeTrackerInit(512);
  for(long i = 0; i < RANGE_COUNT; i++) { 
    randomRange(&(ranges[i]));
  }

  char * bitmask = calloc(RANGE_SIZE, sizeof(char));

  for(long i = 0; i < ITERATIONS; i++) { 
    
    int range = myrandom(RANGE_COUNT);

    switch(myrandom(3)) {
    case 0: { // add range
      rangeTrackerAdd(rt, &ranges[range]);
      pins[range]++;
      checkRangeTrackerConsistency(rt);
      break;
    }
    case 1: { // del range
      if(pins[range]) { 
	rangeTrackerRemove(rt, &ranges[range]);
	pins[range]--;
      }
      checkRangeTrackerConsistency(rt);
      break;
    }
    case 2: { // change range
      for(long i = 0; i < RANGE_COUNT; i++) { 
	if(!pins[i]) { 
	  randomRange(&ranges[i]);
	}
      }
      break;
    }
    default: 
      abort();
    }
  }
  
  for(long i = 0; i < RANGE_COUNT; i++) { 
    while(pins[i]) { 
      rangeTrackerRemove(rt, &ranges[i]);
      pins[i]--;
    }
  }

  free (bitmask);

  rangeTrackerDeinit(rt);

} END_TEST

/** 
  Add suite declarations here
*/
Suite * check_suite(void) {
  Suite *s = suite_create("rangeTracker");
  /* Begin a new test */
  TCase *tc = tcase_create("rangeTracker");
  tcase_set_timeout(tc, 600); // ten minute timeout

  /* Sub tests are added, one per line, here */
  tcase_add_test(tc, rangeTracker_smokeTest);
  tcase_add_test(tc, rangeTracker_randomTest);

  /* --------------------------------------------- */
  tcase_add_checked_fixture(tc, setup, teardown);
  suite_add_tcase(s, tc);


  return s;
}

#include "../check_setup.h"
