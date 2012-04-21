/*
 * random.h
 *
 *  Created on: Aug 31, 2011
 *      Author: sears
 */

#ifndef RANDOM_H_
#define RANDOM_H_

#include <stasis/common.h>

BEGIN_C_DECLS

uint64_t stasis_util_random64(uint64_t x);

/**
 * The remainder of this file is a cleaned up and reentrant version of George
 * Marsaglia's generators from stat.sci.math in 1999.
 *
 * I was concerned that replacing the macros with static inlines would hurt
 * the optimizer, but for some reason, on my machine, and with -O3,
 * check_kiss.c is about 2x faster than the version from the newsgroup posting.
 *
 * @see check_kiss.c for the original, unmodified code.
 */
typedef struct {
  uint32_t x;
  uint32_t y;
  uint32_t bro;
  uint8_t  c;
  uint32_t z;
  uint32_t w;
  uint32_t jsr;
  uint32_t jcong;
  uint32_t a;
  uint32_t b;
  uint32_t t[256];

} kiss_table_t;
static inline uint32_t stasis_util_random_kiss_znew(kiss_table_t* k) {
  return k->z=36969*(k->z&65535)+(k->z>>16);
}
static inline uint32_t stasis_util_random_kiss_wnew(kiss_table_t* k) {
  return k->w=18000*(k->w&65535)+(k->w>>16);
}
static inline uint32_t stasis_util_random_kiss_MWC(kiss_table_t* k) {
  return (stasis_util_random_kiss_znew(k)<<16)+stasis_util_random_kiss_wnew(k);
}
static inline uint32_t stasis_util_random_kiss_SHR3(kiss_table_t* k) {
  return (k->jsr^=(k->jsr<<17), k->jsr^=(k->jsr>>13), k->jsr^=(k->jsr<<5));
}
static inline uint32_t stasis_util_random_kiss_CONG(kiss_table_t* k) {
  return k->jcong=69069*k->jcong+1234567;
}
static inline uint32_t stasis_util_random_kiss_FIB(kiss_table_t* k) {
  return ((k->b=k->a+k->b),(k->a=k->b-k->a));
}
static inline uint32_t stasis_util_random_kiss_KISS(kiss_table_t* k) {
  return (stasis_util_random_kiss_MWC(k)^stasis_util_random_kiss_CONG(k))
      + stasis_util_random_kiss_SHR3(k);
}
static inline uint32_t stasis_util_random_kiss_LFIB4(kiss_table_t* k) {
  (k->c)++;
  return
      k->t[k->c]=k->t[k->c]
                +k->t[(uint8_t)(k->c+58)]
                +k->t[(uint8_t)(k->c+119)]
                +k->t[(uint8_t)(k->c+178)];
}
static inline uint32_t stasis_util_random_kiss_SWB(kiss_table_t* k) {
  return (k->c++,
      k->bro=(k->x<k->y),
      k->t[k->c]=(k->x=k->t[(uint8_t)(k->c+34)])-(k->y=k->t[(uint8_t)(k->c+19)]+k->bro));
}
static inline float stasis_util_random_kiss_UNI(kiss_table_t *k) {
  return stasis_util_random_kiss_KISS(k) * 2.328306e-10;
}
static inline float stasis_util_random_kiss_VNI(kiss_table_t *k) {
  return ((int32_t) stasis_util_random_kiss_KISS(k))*4.656613e-10;
}
static inline void stasis_util_random_kiss_settable(kiss_table_t *k,
   uint32_t i1, uint32_t i2, uint32_t i3,
   uint32_t i4, uint32_t i5, uint32_t i6) {
  k->x=0;k->y=0;k->c=0;k->z=i1;k->w=i2,k->jsr=i3; k->jcong=i4; k->a=i5; k->b=i6;
  for(int i = 0; i < 256; i++) { k->t[i] = stasis_util_random_kiss_KISS(k); }
}
END_C_DECLS
#endif /* RANDOM_H_ */
