#include <lladd/hash.h>
#include <math.h>
/*static int  thomasWangs32BitMixFunction(int key);
  static unsigned long thomasWangs64BitMixFunction(unsigned long key);*/

/** @todo replace powl in hash with something more efficient, if hash() becomes a bottleneck. */
unsigned int hash(void * val, long val_length, unsigned char tableBits, unsigned long nextExtension) {
  unsigned long tableLength = powl(2, tableBits);
  unsigned long oldTableLength = powl(2, tableBits - 1);
  unsigned long unmixed = crc32(val, val_length, (unsigned long)-1L);

  unsigned long ret = unmixed & (oldTableLength - 1); 

  if(*(int*)val == 4090) {
    printf("here too!");
  }

  /* What would the low hash value be? */
  /*  printf("hash(%d, bits=%d, ext=%ld) first: %ld ", *(int*)val, tableBits, nextExtension, ret); */
  if(ret < nextExtension) { /* Might be too low. */
    ret = unmixed & (tableLength - 1);
    /*    printf("second: %ld", ret); */
  }
  /*   printf("\n"); */

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
