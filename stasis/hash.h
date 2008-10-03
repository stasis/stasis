
#include <stasis/crc32.h>
#include <stasis/common.h>

#ifndef __HASH_H
#define __HASH_H
/**
   @todo write a test harness for this...
*/

uint64_t max_bucket(unsigned char tableBits, uint64_t nextExtension);
/**
   This function maps from the length of the bucket list to a appropriate set 
   of linear hash parameters to fill that size.
*/
void hashGetParamsForSize(uint64_t desiredSize, unsigned char *tableBits,
		      uint64_t* nextExtension);
/**
   XXX despite it's interface, hash can't return values > 2^32!
*/
uint64_t hash(const void * val, uint64_t val_length, unsigned char tableBits, uint64_t nextExtension);
#define twoToThe(x) (1 << (x))
uint32_t logBase2(uint64_t value);
#endif /*__HASH_H */
