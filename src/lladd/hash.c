#include <lladd/hash.h>
/*#include <math.h> */
/*static int  thomasWangs32BitMixFunction(int key);
  static unsigned long thomasWangs64BitMixFunction(unsigned long key);*/

unsigned int max_bucket(unsigned char tableBits, unsigned long nextExtension) {
  unsigned int oldTableLength = twoToThe(tableBits - 1);
  return oldTableLength + nextExtension - 1;
}

/** @todo replace powl in hash with something more efficient, if hash() becomes a bottleneck. */

unsigned int hash(const void * val, long val_length, unsigned char tableBits, unsigned long nextExtension) {
  unsigned int oldTableLength = /*powl(2, tableBits - 1); */ twoToThe(tableBits - 1);
  unsigned int unmixed = crc32(val, val_length, (unsigned int)-1);
  unsigned int ret = unmixed & (oldTableLength - 1); 

  /* What would the low hash value be? */
  if(ret < nextExtension) { /* Might be too low. */
    unsigned int tableLength = /* powl(2, tableBits); */ twoToThe(tableBits);
    ret = unmixed & (tableLength - 1);
  }
  return (int) ret;
}

/*static unsigned long thomasWangs64BitMixFunction(unsigned long key) 
{
  key += ~(key << 32L);
  key ^= (key >> 22L);
  key += ~(key << 13L);
  key ^= (key >> 8L);
  key += (key << 3L);
  key ^= (key >> 15L);
  key += ~(key << 27L);
  key ^= (key >> 31L);
  return key;
}

static int thomasWangs32BitMixFunction(int key)
{
  key += ~(key << 15);
  key ^=  (key >> 10);
  key +=  (key << 3);
  key ^=  (key >> 6);
  key += ~(key << 11);
  key ^=  (key >> 16);
  return key;
}

*/
