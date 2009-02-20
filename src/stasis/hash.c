#include <stasis/hash.h>
#include <assert.h>
//#include <stdio.h>
/**
 @todo Make hash.c 32/64bit little/big-endian clean...

 @file Hash functions for stasis' in memory and persistent hash tables.
*/

#ifdef THOMAS_WANG_32
static inline int thomasWangs32BitMixFunction(int key)
{
  key += ~(key << 15);
  key ^=  (key >> 10);
  key +=  (key << 3);
  key ^=  (key >> 6);
  key += ~(key << 11);
  key ^=  (key >> 16);
  return key;
}
#else 
#ifdef THOMAS_WANG_64

static inline unsigned long thomasWangs64BitMixFunction(unsigned long key) 
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

#endif
#endif

static const char LogTable256[] = 
{
  0, 0, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3,
  4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
  5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
  5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
  6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
  6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
  6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
  6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
  7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
  7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
  7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
  7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
  7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
  7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
  7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
  7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7
};

/*Taken from 
  http://graphics.stanford.edu/~seander/bithacks.html

  @todo extend to handle unsigned long (this will mean 64bit on 64bit
  platforms; need compiler macro to test for sizeof(long), test
  harness to compare logBase2Slow's output with logBase2's output,
  etc...)
*/
uint32_t logBase2(uint64_t v) { 
  uint32_t r = 0; // r will be lg(v)
  uint32_t t, tt; // temporaries
  
  if ((tt = v >> 16))
    {
      r = (t = v >> 24) ? 24 + LogTable256[t] : 16 + LogTable256[tt & 0xFF];
    }
  else 
    {
      r = (t = v >> 8) ? 8 + LogTable256[t] : LogTable256[v];
    }
  return r;
}

uint32_t logBase2Slow(uint64_t v) { 
  uint32_t r = 0; // r will be lg(v)

  while (v >>= 1) // unroll for more speed...
    {
      r++;
    }
  return r;
}
