/** @file

  Helper functions for profiling tools.

*/

#include <sys/time.h>
#include <time.h>


typedef struct {
  long sum_spin;
  long sum_spin2;
  double max_spin;
  double sum_hold;
  double sum_hold2;
  double max_hold;
  struct timeval last_acquired;
  long count;
} profile_tuple;

void acquired_lock(profile_tuple * tup, long spin_count);
void released_lock(profile_tuple * tup);
void print_profile_tuple(profile_tuple * tup);
void init_tuple(profile_tuple * tup);
