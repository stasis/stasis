#include <check.h>
#include <config.h>
#include <assert.h>
#include "../check_includes.h"
#define LOG_NAME   "check_rangeTracker.log"
#include <lladd/io/rangeTracker.h>

void printRT(rangeTracker * rt) { 
  const transition ** ts = enumTransitions(rt);
  ts = enumTransitions(rt);
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
 
  const transition ** ts = enumTransitions(rt);

  assert(ts[0] == 0);

  free(ts);

  //  printRT(rt);


  range r;
  r.start = 10;
  r.end   = 100;

  rangeTrackerAdd(rt, &r);

  //  printRT(rt);

  ts = enumTransitions(rt);

  //  10   20             80   90  100 
  //  |    |              |    |   |
  //  |1111|11111111111111|1111|111|
  //  |    |              |    |   |

  assert(ts[0]->pos == 10 && ts[0]->delta ==  1 && ts[0]->pins ==  0 &&
	 ts[1]->pos == 100&& ts[1]->delta == -1 && ts[1]->pins ==  1 && !ts[2]);
	 

  r.start = 20;
  r.end = 80;

  rangeTrackerRemove(rt, &r);

  //  printRT(rt);

  ts = enumTransitions(rt);

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

  ts = enumTransitions(rt);

  //  10   20             80   90  100 
  //  |    |              |    |   |
  //  |1111|11111111111111|1111|111|
  //  |    |              |    |   |


  assert(ts[0]->pos == 10 && ts[0]->delta ==  1 && ts[0]->pins ==  0 &&
	 ts[1]->pos == 100&& ts[1]->delta == -1 && ts[1]->pins ==  1 && !ts[2]);
	 
  rangeTrackerAdd(rt, &r);

  //  printRT(rt);

  ts = enumTransitions(rt);

  //  10   20             80   90  100 
  //  |    |              |    |   |
  //  |1111|22222222222222|1111|111|
  //  |    |              |    |   |

  assert(ts[0]->pos == 10 && ts[0]->delta ==  1 && ts[0]->pins ==  0 &&
	 ts[1]->pos == 20 && ts[1]->delta ==  1 && ts[1]->pins ==  1 &&
	 ts[2]->pos == 80 && ts[2]->delta == -1 && ts[2]->pins ==  2 &&
	 ts[3]->pos == 100&& ts[3]->delta == -1 && ts[3]->pins ==  1 && !ts[4]);

	 
  rangeTrackerRemove(rt, &r);
  ts = enumTransitions(rt);

  //  10   20             80   90  100 
  //  |    |              |    |   |
  //  |1111|11111111111111|1111|111|
  //  |    |              |    |   |


  assert(ts[0]->pos == 10 && ts[0]->delta ==  1 && ts[0]->pins ==  0 &&
	 ts[1]->pos == 100&& ts[1]->delta == -1 && ts[1]->pins ==  1 && !ts[2]);
  rangeTrackerRemove(rt, &r);

  //  printRT(rt);

  ts = enumTransitions(rt);

  //  10   20             80   90  100 
  //  |    |              |    |   |
  //  |1111|              |1111|111|
  //  |    |              |    |   |

  assert(ts[0]->pos == 10 && ts[0]->delta ==  1 && ts[0]->pins ==  0 &&
	 ts[1]->pos == 20 && ts[1]->delta == -1 && ts[1]->pins ==  1 &&
	 ts[2]->pos == 80 && ts[2]->delta ==  1 && ts[2]->pins ==  0 &&
	 ts[3]->pos == 100&& ts[3]->delta == -1 && ts[3]->pins ==  1 && !ts[4]);

  r.start = 80;
  r.end = 90;


  
  rangeTrackerAdd(rt, &r);

  //  printRT(rt);

  ts = enumTransitions(rt);

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
  r.end = 100;
  rangeTrackerRemove(rt, &r);

  //  printRT(rt);
  ts = enumTransitions(rt);

  //  10   20             80   90  100 
  //  |    |              |    |   |
  //  |1111|              |1111|   |
  //  |    |              |    |   |


  assert(ts[0]->pos == 10 && ts[0]->delta ==  1 && ts[0]->pins ==  0 &&
	 ts[1]->pos == 20 && ts[1]->delta == -1 && ts[1]->pins ==  1 &&
	 ts[2]->pos == 80 && ts[2]->delta ==  1 && ts[2]->pins ==  0 &&
	 ts[3]->pos == 90 && ts[3]->delta == -1 && ts[3]->pins ==  1 && !ts[4]);


  r.start = 10;
  r.end = 20;
  rangeTrackerRemove(rt, &r);

  //  printRT(rt);
  ts = enumTransitions(rt);

  //  10   20             80   90  100 
  //  |    |              |    |   |
  //  |    |              |1111|   |
  //  |    |              |    |   |


  assert(ts[0]->pos == 80 && ts[0]->delta ==  1 && ts[0]->pins ==  0 &&
	 ts[1]->pos == 90 && ts[1]->delta == -1 && ts[1]->pins ==  1 && !ts[2]);


  r.start = 80;
  r.end = 90;
  
  rangeTrackerRemove(rt, &r);

  //  printRT(rt);
  ts = enumTransitions(rt);

  //  10   20             80   90  100 
  //  |    |              |    |   |
  //  |    |              |    |   |
  //  |    |              |    |   |


  assert(!ts[0]);
  rangeTrackerDeinit(rt);
}
END_TEST

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

  /* --------------------------------------------- */
  tcase_add_checked_fixture(tc, setup, teardown);
  suite_add_tcase(s, tc);


  return s;
}

#include "../check_setup.h"
