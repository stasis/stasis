/*
 * @file Random number generators.
 *
 *  Created on: Aug 31, 2011
 *      Author: sears
 */

#include <config.h>
#include <stasis/common.h>
#include <stasis/util/random.h>

uint64_t stasis_util_random64(uint64_t x) {
  double xx = x;
  double r = random();
  double max = ((uint64_t)RAND_MAX)+1;
  max /= xx;
  return (uint64_t)((r/max));
}
