
#include <stasis/util/crc32.h>
#include <stasis/common.h>

#ifndef __HASH_H
#define __HASH_H

#ifndef HASH_ENTRY
#define HASH_ENTRY(x) stasis_linear_hash##x
#define HASH_FCN(x,y,z) stasis_crc32(x,y,z)
#endif

static inline unsigned long stasis_util_two_to_the (char x) {
  return (1 << ((long)x));
}
uint32_t logBase2(uint64_t value);

/**
   This function maps from the length of the bucket list to a appropriate set
   of linear hash parameters to fill that size.
*/
static inline void HASH_ENTRY(_get_size_params)(uint64_t desiredSize,
		      unsigned char * tableBits, uint64_t* nextExtension) {
  *tableBits = logBase2(desiredSize)+1;
  *nextExtension = ((desiredSize) - stasis_util_two_to_the(*tableBits-1));
}

/**
   @todo despite it's interface, stasis_linear_hash can't return values > 2^32!
*/
static inline uint64_t HASH_ENTRY()(const void * val, uint64_t val_length,
		  unsigned char tableBits, uint64_t nextExtension) {
  // Calculate the hash value as it was before this round of splitting.
  unsigned int oldTableLength = stasis_util_two_to_the(tableBits - 1);
  unsigned int unmixed = HASH_FCN(val, val_length, (unsigned int)-1);
  unsigned int ret = unmixed & (oldTableLength - 1); 

  // If the hash value is before the point in this round where we've split,
  // use the new value instead.  (The new value may be the same as the 
  // old value.)
  if(ret < nextExtension) { /* Might be too low. */
    unsigned int tableLength =  stasis_util_two_to_the(tableBits);
    ret = unmixed & (tableLength - 1);
  }
  return  ret;
}

#endif /*__HASH_H */

