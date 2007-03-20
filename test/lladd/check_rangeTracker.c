#include <config.h>
#include <check.h>
#include <assert.h>
#include <lladd/transactional.h>
#define LOG_NAME   "check_rangeTracker.log"

#define QUANTIZATION 7

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

void rangeTrackerFreeRet(range ** ret)  {
  for(int i = 0; ret[i]; i++) { 
    free(ret[i]);
  }
  free(ret);
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

void check_no_overlap(range * r, int * explicit_pins) { 
  for(int j = r->start; j < r->stop; j++) {
    assert(!explicit_pins[j]);
  }
}
void check_overlap(range * r, int * explicit_pins) { 
  assert(!(r->start % QUANTIZATION));
  assert(!(r->stop % QUANTIZATION));
  
  int overlap = 0;
  for(int j = r->start; j < r->stop; j++) { 
    if(explicit_pins[j]) { overlap = 1; }
    if(! (j+1 % QUANTIZATION)) { 
      assert(overlap);
      overlap = 0;
    }
  }
}

START_TEST(rangeTracker_smokeTest) {
  rangeTracker * rt = rangeTrackerInit(QUANTIZATION);
 
  const transition ** ts = rangeTrackerEnumerate(rt);

  assert(ts[0] == 0);

  free(ts);

  //  printRT(rt);


  range r;
  r.start = 10;
  r.stop   = 100;

  rangeTrackerFreeRet(rangeTrackerAdd(rt, &r));


  //  printRT(rt);

  ts = rangeTrackerEnumerate(rt);

  //  10   20             80   90  100 
  //  |    |              |    |   |
  //  |1111|11111111111111|1111|111|
  //  |    |              |    |   |

  assert(ts[0]->pos == 10 && ts[0]->delta ==  1 && ts[0]->pins ==  0 &&
	 ts[1]->pos == 100&& ts[1]->delta == -1 && ts[1]->pins ==  1 && !ts[2]);
	 

  free(ts);

  r.start = 20;
  r.stop = 80;

  rangeTrackerFreeRet(rangeTrackerRemove(rt, &r));

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


  rangeTrackerFreeRet(rangeTrackerAdd(rt, &r));

  //  printRT(rt);

  free(ts);
  ts = rangeTrackerEnumerate(rt);

  //  10   20             80   90  100 
  //  |    |              |    |   |
  //  |1111|11111111111111|1111|111|
  //  |    |              |    |   |


  assert(ts[0]->pos == 10 && ts[0]->delta ==  1 && ts[0]->pins ==  0 &&
	 ts[1]->pos == 100&& ts[1]->delta == -1 && ts[1]->pins ==  1 && !ts[2]);
	 
  rangeTrackerFreeRet(rangeTrackerAdd(rt, &r));

  //  printRT(rt);
  free(ts);
  ts = rangeTrackerEnumerate(rt);

  //  10   20             80   90  100 
  //  |    |              |    |   |
  //  |1111|22222222222222|1111|111|
  //  |    |              |    |   |

  assert(ts[0]->pos == 10 && ts[0]->delta ==  1 && ts[0]->pins ==  0 &&
	 ts[1]->pos == 20 && ts[1]->delta ==  1 && ts[1]->pins ==  1 &&
	 ts[2]->pos == 80 && ts[2]->delta == -1 && ts[2]->pins ==  2 &&
	 ts[3]->pos == 100&& ts[3]->delta == -1 && ts[3]->pins ==  1 && !ts[4]);

  free(ts);
  rangeTrackerFreeRet(rangeTrackerRemove(rt, &r));
  ts = rangeTrackerEnumerate(rt);

  //  10   20             80   90  100 
  //  |    |              |    |   |
  //  |1111|11111111111111|1111|111|
  //  |    |              |    |   |


  assert(ts[0]->pos == 10 && ts[0]->delta ==  1 && ts[0]->pins ==  0 &&
	 ts[1]->pos == 100&& ts[1]->delta == -1 && ts[1]->pins ==  1 && !ts[2]);
  free(ts);

  rangeTrackerFreeRet(rangeTrackerRemove(rt, &r));

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
  free(ts);
  r.start = 80;
  r.stop = 90;


  
  rangeTrackerFreeRet(rangeTrackerAdd(rt, &r));

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
  free(ts);
  r.start = 80;
  r.stop = 100;

  rangeTrackerFreeRet(rangeTrackerRemove(rt, &r));

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

  free(ts);
  r.start = 10;
  r.stop = 20;
  rangeTrackerFreeRet(rangeTrackerRemove(rt, &r));

  //  printRT(rt);
  ts = rangeTrackerEnumerate(rt);

  //  10   20             80   90  100 
  //  |    |              |    |   |
  //  |    |              |1111|   |
  //  |    |              |    |   |


  assert(ts[0]->pos == 80 && ts[0]->delta ==  1 && ts[0]->pins ==  0 &&
	 ts[1]->pos == 90 && ts[1]->delta == -1 && ts[1]->pins ==  1 && !ts[2]);
  free(ts);

  r.start = 80;
  r.stop = 90;
  
  rangeTrackerFreeRet(rangeTrackerRemove(rt, &r));

  //  printRT(rt);
  ts = rangeTrackerEnumerate(rt);

  //  10   20             80   90  100 
  //  |    |              |    |   |
  //  |    |              |    |   |
  //  |    |              |    |   |
  assert(!ts[0]); 
  free(ts);

  rangeTrackerDeinit(rt);
}
END_TEST

#define RANGE_SIZE 1000
#define ITERATIONS 10000 //1000
#define RANGE_COUNT 1000 // 100
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
  
  int * explicit_pins = calloc(RANGE_SIZE, sizeof(int));

  long seed =  time.tv_usec + time.tv_sec * 1000000; //1170807889195512; //time.tv_usec + time.tv_sec * 1000000; //1170729550013502; //time.tv_usec + time.tv_sec * 1000000;// 1170727703805787; // 1170810757441165; 1170811024737237; 1171329519584370;

  printf("\nSeed = %ld\n", seed);
  srandom(seed);

  range ** r_arry;
  range * ranges = malloc(sizeof(range) * RANGE_COUNT);
  int * pins = calloc(RANGE_COUNT, sizeof(int));
  rangeTracker * rt = rangeTrackerInit(QUANTIZATION);
  for(long i = 0; i < RANGE_COUNT; i++) { 
    randomRange(&(ranges[i]));
  }

  char * s;

  for(long i = 0; i < ITERATIONS; i++) { 
    
    int range = myrandom(RANGE_COUNT);
    switch(myrandom(3)) {
    case 0: { // add range
      s = rangeToString(&ranges[range]);
      //      printf("pin   %s\n", s);
      free(s);
      r_arry = rangeTrackerAdd(rt, &ranges[range]);
      for(int i = 0; r_arry[i]; i++) { 
	check_no_overlap(r_arry[i], explicit_pins);
      }

      for(int i = ranges[range].start; i < ranges[range].stop; i++) { 
	explicit_pins[i]++;
      }
      for(int i = 0; r_arry[i]; i++) { 
	s = rangeToString(r_arry[i]);
	//	printf(" add returned %s\n", s);

	check_overlap(r_arry[i], explicit_pins);

	assert(r_arry[i]->start >= 
	       rangeTrackerRoundDown(ranges[range].start, QUANTIZATION));
	assert(r_arry[i]->stop <= 
	       rangeTrackerRoundUp(ranges[range].stop, QUANTIZATION));
	free(s);
	free(r_arry[i]);
      }
      free(r_arry);
      pins[range]++;
      checkRangeTrackerConsistency(rt);
      break;
    }
    case 1: { // del range
      if(pins[range]) { 
	s = rangeToString(&ranges[range]);
	//	printf("unpin %s\n", s);
	free(s);
	r_arry = rangeTrackerRemove(rt, &ranges[range]);
	for(int i = 0; r_arry[i]; i++) { 
	  check_overlap(r_arry[i], explicit_pins);
	}
	for(int i = ranges[range].start; i < ranges[range].stop; i++)  {
	  explicit_pins[i]--;
	  assert(explicit_pins[i] >= 0);
	}
	// return value should no longer overlap pins.
	for(int i = 0; r_arry[i]; i++) { 
	  check_no_overlap(r_arry[i], explicit_pins);
	  //	  for(int j = r_arry[i]->start; j < r_arry[i]->stop; j++) {
	  //	    assert(!explicit_pins[j]);
	  //	  }
	  s = rangeToString(r_arry[i]);
	  assert(r_arry[i]->start >= 
		 rangeTrackerRoundDown(ranges[range].start, QUANTIZATION));
	  assert(r_arry[i]->stop <= 
		 rangeTrackerRoundUp(ranges[range].stop, QUANTIZATION));
	  //	  printf(" del returned %s\n", s);
	  free(s);
	  free(r_arry[i]);
	}
	free(r_arry);
	pins[range]--;
      }
      checkRangeTrackerConsistency(rt);
      break;
    }
    case 2: { // change range
      if(!myrandom(100)) {
	for(long i = 0; i < RANGE_COUNT; i++) { 
	  if(!pins[i]) { 
	    randomRange(&ranges[i]);
	  }
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
      s = rangeToString(&ranges[i]);
      //      printf("unpin %s\n", s);
      free(s);
      range ** r_arry =  rangeTrackerRemove(rt, &ranges[i]);
      for(int i = 0; r_arry[i]; i++) { 
	check_overlap(r_arry[i], explicit_pins);
      }
      for(int j = ranges[i].start; j < ranges[i].stop; j++)  {
	explicit_pins[j]--;
	assert(explicit_pins[j] >= 0);
      }
      for(int i = 0; r_arry[i]; i++) { 
	s = rangeToString(r_arry[i]);
	//	printf(" del returned %s\n", s);
	check_no_overlap(r_arry[i], explicit_pins);
	free(s);
	free(r_arry[i]);
      }
      free(r_arry);
      pins[i]--;
    }
  }

  for(int i =0 ; i < RANGE_SIZE; i++ ) { 
    assert(explicit_pins[i] == 0);
  }

  free (ranges);
  free (pins);
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
