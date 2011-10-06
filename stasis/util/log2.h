/*
 * log2.h
 *
 *  Created on: Sep 28, 2010
 *      Author: sears
 *
 *
 *  Methods that use table lookups to compute log base two of integers.
 *
 *  These methods are based on the public domain methods at:
 *
 *  http://graphics.stanford.edu/~seander/bithacks.html#IntegerLogLookup
 *
 */

#ifndef LOG2_H_
#define LOG2_H_


#include <stasis/common.h>

extern const uint8_t LogTable256[256];

/*Taken from
  http://graphics.stanford.edu/~seander/bithacks.html

  @todo extend to handle unsigned long (this will mean 64bit on 64bit
  platforms; need compiler macro to test for sizeof(long), test
  harness to compare logBase2Slow's output with logBase2's output,
  etc...)
*/

/**
 * @param v 32-bit word to find the log of
 * @return lg_2(v)
 */
static inline uint8_t stasis_log_2_32(uint32_t v) {
  uint8_t r;     // r will be lg(v)
  register uint32_t t, tt; // temporaries

  if ((tt = v >> 16))
  {
    r = ((t = tt >> 8)) ? 24 + LogTable256[t] : 16 + LogTable256[tt];
  }
  else
  {
    r = ((t = v >> 8)) ? 8 + LogTable256[t] : LogTable256[v];
  }
  return r;
}
static inline uint8_t stasis_log_2_64(uint64_t v) {
  uint8_t r;     // r will be lg(v)
  register uint64_t tt; // temporaries

  if((tt = v >> 32))
  {
    r = stasis_log_2_32(tt) + 32;
  } else {
    r = stasis_log_2_32(v);
  }
  return r;
}

static inline uint64_t stasis_round_up_to_power_of_two(uint64_t v) {
  if(v) {
    return 1 << (stasis_log_2_64(v)-1);
  } else {
    return 0;
  }
}

#endif /* LOG2_H_ */
