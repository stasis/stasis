/*
 * histogram.h
 *
 *  Created on: Sep 28, 2010
 *      Author: sears
 */
#ifndef HISTOGRAM_H_
#define HISTOGRAM_H_

#include <stasis/common.h>
BEGIN_C_DECLS

#include <assert.h>
#include <stdio.h>

#include <stasis/util/log2.h>
#include <stasis/util/time.h>

/** @todo move to a .c */
static inline void stasis_histogram_thread_destroy(void * p) { free (p); }

#define DECLARE_HISTOGRAM_64(x)                                                \
  stasis_histogram_64_t x;                                                     \
  extern void stasis_histogram_ctor_##x() __attribute__((constructor));        \
  void stasis_histogram_ctor_##x(void) { stasis_histogram_64_clear(&x);        \
                                         pthread_key_create(&(x.tls), stasis_histogram_thread_destroy); \
                                         stasis_auto_histogram_count++;        \
                                         stasis_auto_histograms = stasis_realloc(stasis_auto_histograms, stasis_auto_histogram_count, stasis_histogram_64_t*); \
                                         stasis_auto_histogram_names = stasis_realloc(stasis_auto_histogram_names, stasis_auto_histogram_count, char*); \
                                         stasis_auto_histograms[stasis_auto_histogram_count - 1] = &x; \
                                         stasis_auto_histogram_names[stasis_auto_histogram_count - 1] = __FILE__":"#x; \
                                       }

//  extern void stasis_histogram_dtor_##x() __attribute__((destructor));
//  void stasis_histogram_dtor_##x(void) { /*printf("%s: Histogram %s\n", __FILE__, #x);*/
//                                       stasis_histogram_pretty_print_64(&x); }

typedef struct {
  uint64_t buckets[64];
  pthread_key_t tls;
} stasis_histogram_64_t;

extern stasis_histogram_64_t**stasis_auto_histograms;
extern char** stasis_auto_histogram_names;
extern int stasis_auto_histogram_count;

typedef struct {
  uint64_t buckets[32];
  pthread_key_t tls;
} stasis_histogram_32_t;

static inline void stasis_histogram_64_clear(stasis_histogram_64_t * hist) {
  for(int i = 0; i < 64; i++) { hist->buckets[i] = 0; }
}
static inline void stasis_histogram_32_clear(stasis_histogram_32_t * hist) {
  for(int i = 0; i < 32; i++) { hist->buckets[i] = 0; }
}

static inline void stasis_histogram_insert_log_uint64_t(stasis_histogram_64_t* hist, uint64_t val) {
  hist->buckets[stasis_log_2_64(val)]++;
}
static inline void stasis_histogram_insert_log_uint32_t(stasis_histogram_32_t* hist, uint64_t val) {
  hist->buckets[stasis_log_2_32(val)]++;
}
static inline void stasis_histogram_insert_log_timeval(stasis_histogram_64_t* hist, const struct timeval val) {
  hist->buckets[stasis_log_2_timeval(val)]++;
}

static inline void stasis_histogram_tick(stasis_histogram_64_t* hist) {
  struct timeval * val = (struct timeval *)pthread_getspecific(hist->tls);
  if(!val) { val = stasis_alloc(struct timeval); pthread_setspecific(hist->tls, val); }
  gettimeofday(val,0);
}
static inline void stasis_histogram_tock(stasis_histogram_64_t* hist) {
  struct timeval * val = (struct timeval *)pthread_getspecific(hist->tls);
  assert(val);
  struct timeval now;
  gettimeofday(&now,0);
  stasis_histogram_insert_log_timeval(hist, stasis_subtract_timeval(now, *val));
}

static inline uint64_t stasis_histogram_earth_movers_distance_64(stasis_histogram_64_t* a, stasis_histogram_64_t* b) {
  double moved_so_far = 0.0;
  double in_shovel = 0.0;
  double a_mass = 0.0, b_mass = 0.0;
  for(int i = 0; i < 64; i++) {
    a_mass += ((double)a->buckets[i]);
    b_mass += ((double)b->buckets[i]);
  }
  if(a_mass == 0.0 || b_mass == 0.0) { return (uint64_t)-1; }
  for(int i = 0; i < 64; i++) {
    in_shovel += (b_mass * (double)a->buckets[i]) - ((double)b->buckets[i]);
    moved_so_far += (in_shovel >= 0 ? in_shovel : -in_shovel);
  }
  return moved_so_far / a_mass;
}

static inline uint64_t stasis_histogram_earth_movers_distance_32(stasis_histogram_32_t* a, stasis_histogram_32_t* b) {
  double moved_so_far = 0.0;
  double in_shovel = 0.0;
  double a_mass = 0.0, b_mass = 0.0;
  for(int i = 0; i < 32; i++) {
    a_mass += ((double)stasis_log_2_64(a->buckets[i]));
    b_mass += ((double)stasis_log_2_64(b->buckets[i]));
  }
  if(a_mass == 0.0 || b_mass == 0.0) { return (uint64_t)-1; }
  for(int i = 0; i < 32; i++) {
    in_shovel += (b_mass * (double)stasis_log_2_64(a->buckets[i])) - ((double)stasis_log_2_64(b->buckets[i]));
    moved_so_far += (in_shovel >= 0 ? in_shovel : -in_shovel);
  }
  return moved_so_far / a_mass;
}
/** @todo move all of these to a .c */
static inline void stasis_histogram_pretty_print_64(stasis_histogram_64_t* a);
static inline void stasis_histogram_pretty_print_32(stasis_histogram_32_t* a);
void stasis_histograms_auto_dump(void);

static inline double stasis_histogram_nth_percentile_64(stasis_histogram_64_t* a, int pctile) {
  long sum = 0;
  for(int i = 0; i < 64; i++) {
    sum += a->buckets[i];
  }
  sum *= pctile;
  sum /= 100;
  long newsum = 0;
  int i;
  for(i = 0; i < 64; i++) {
    newsum += a->buckets[i];
    if(newsum >= sum) { break; }
  }
  long ret = 1;
  for(int j = 0; j < i; j++) {
    ret *= 2;
  }
  return ((double)ret)/1000000.0;
}

void stasis_histogram_pretty_print_64(stasis_histogram_64_t* a) {
  uint8_t logs[64];
  int max_log = 0;
  for(int i = 0; i < 64; i++) {
    logs[i] = stasis_log_2_64(a->buckets[i]);
    if(logs[i] > max_log) { max_log = logs[i]; }
  }
  for(int i = max_log; i > 0; i--) {
    if(!(i % 10)) {
      printf("2^%2d ", i);
    } else {
      printf("     ");
    }
    for(int j = 0; j < 64; j++) {
      printf(logs[j] >= i ?  "#" : " ");
    }
    printf("\n");
  }
  if(max_log == 0) {
    printf("[empty]\n");
  } else {
    printf("     us        ms        s         ks        Ms\n");
  }
}
void stasis_histogram_pretty_print_32(stasis_histogram_32_t* a) {
  uint8_t logs[32];
  int max_log = 0;
  for(int i = 0; i < 32; i++) {
    logs[i] = stasis_log_2_64(a->buckets[i]);
    if(logs[i] > max_log) { max_log = logs[i]; }
  }
  for(int i = max_log; i >= 0; i--) {
    if(!(i % 10)) {
      printf("2^%2d ", i);
    } else {
      printf("     ");
    }
    for(int j = 0; j < 32; j++) {
      printf(logs[j] >= i ?  "#" : " ");
    }
    printf("\n");
  }
  if(max_log == 0) {
    printf("[empty]\n");
  } else {
               //01234567890123456789012345678901234567890
    printf("     us        ms        s         ks        Ms\n");
  }
}
END_C_DECLS
#endif /* HISTOGRAM_H_ */
