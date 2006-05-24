
#include <lladd/crc32.h>

#ifndef __HASH_H
#define __HASH_H
unsigned int max_bucket(unsigned char tableBits, unsigned int nextExtension);
/**
   This function maps from the length of the bucket list to a appropriate set 
   of linear hash parameters to fill that size.
*/
void hashGetParamsForSize(unsigned int desiredSize, unsigned char *tableBits,
		      unsigned int* nextExtension);
unsigned int hash(const void * val, long val_length, unsigned char tableBits, unsigned int nextExtension);
#define twoToThe(x) (1 << (x))
/** @todo logBase2 should be able to handle 64 bit values, but
    currently doesn't...*/
unsigned int logBase2(unsigned int value);
#endif /*__HASH_H */
