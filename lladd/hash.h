#include <lladd/crc32.h>

/** @todo replace() powl in hash with something more efficient, if hash() becomes a bottleneck. */
unsigned int hash(void * val, long val_length, unsigned char tableBits, unsigned long nextExtension);
