
#include <lladd/crc32.h>

#ifndef __HASH_H
#define __HASH_H
/** @todo replace() powl in hash with something more efficient, if hash() becomes a bottleneck. */
unsigned int hash(const void * val, long val_length, unsigned char tableBits, unsigned long nextExtension);
#define twoToThe(x) (1 << (x))

#endif /*__HASH_H */
