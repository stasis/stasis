#include <config.h>
#include <lladd/common.h>
#include "latches.h"

#include <math.h>

void acquired_lock(profile_tuple * tup, long spin_count) {
  tup->sum_spin += spin_count;
  tup->sum_spin2 += (spin_count * spin_count);
  if(tup->max_spin < spin_count) { tup->max_spin = spin_count; }
  tup->count++;
  gettimeofday(&(tup->last_acquired), NULL);
}

void released_lock(profile_tuple * tup) {
  struct timeval now;
  double microseconds;
  gettimeofday(&now, NULL);

  microseconds = (double)((now.tv_sec - tup->last_acquired.tv_sec) * 1000000.0) +
    (double)(now.tv_usec - tup->last_acquired.tv_usec);

  tup->sum_hold += microseconds;
  tup->sum_hold2 += (microseconds * microseconds);
  if(tup->max_hold < microseconds) { tup->max_hold = microseconds; }

}

void print_profile_tuple(profile_tuple * tup) {
  /* Dump profiling info to stdout */
  if(tup->count) {
    double mean_spin = ((double)tup->sum_spin) / ((double)tup->count);
    double std_spin = sqrt((((double)tup->sum_spin2) / ((double)tup->count)) - (mean_spin * mean_spin));
    double mean_hold = ((double)tup->sum_hold)/ ((double)tup->count);
    double std_hold = sqrt((((double)tup->sum_hold2) / ((double)tup->count)) - (mean_hold * mean_hold));
    
    printf("{count=%ld spin[%1.4lf %1.4lf %0.0lf] held[%1.4lf %1.4lf %0.0lf]us}", tup->count, 
	   mean_spin, std_spin, tup->max_spin, 
	   mean_hold, std_hold, tup->max_hold);
  } else {
    printf("{count=0}");
  }
}

void init_tuple(profile_tuple * tup) {
  tup->sum_spin = 0;
  tup->sum_spin2 = 0;
  tup->max_spin = 0;
  tup->sum_hold = 0;
  tup->sum_hold2 = 0;
  tup->max_hold = 0;
  tup->count = 0;
}
