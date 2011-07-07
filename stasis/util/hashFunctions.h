/*
 * hashFunctions.h
 *
 *  Created on: Oct 3, 2010
 *      Author: sears
 */

#ifndef HASHFUNCTIONS_H_
#define HASHFUNCTIONS_H_

/**
 * This file contains a number of hash function implementations.
 *
 * Some are expensive, but general-purpose, and choose values with good
 * mathematical properties (i.e. CRC32).  Others are faster, but special
 * purpose (assume their input is an integer), or provide weaker guarantees
 * about their outputs.
 *
 * FNV-1 hash implementations all have the following form:
 * <code>
 * hash = offset_basis
 * for each octet_of_data to be hashed
 *   hash = hash * FNV_prime
 *   hash = hash xor octet_of_data
 * return hash
 * </code>
 *
 * (From http://isthe.com/chongo/tech/comp/fnv/)
 */

#include <stasis/common.h>
#include <stasis/util/crc32.h>
#define stasis_hash_util_define_fnv_1(TYPE, FNV_prime, offset_basis)          \
static inline TYPE stasis_util_hash_fnv_1_##TYPE(const byte* octets, int len){\
  TYPE hash = offset_basis;                                                   \
                                                                              \
  for(int i = 0; i < len; i++) {                                              \
    hash = hash * FNV_prime;                                                  \
    hash ^= octets[i];                                                        \
  }                                                                           \
  return hash;                                                                \
}                                                                             \
/**
 * Implementation of FNV-1 (32-bit).  Function is called stasis_util_hash_fnv_uint32_t().
 */
stasis_hash_util_define_fnv_1(uint32_t, 16777619U, 2166136261U)
/**
 * Implementation of FNV-1 (64-bit).  Function is called stasis_util_hash_fnv_uint64_t().
 */
stasis_hash_util_define_fnv_1(uint64_t, 1099511628211ULL, 14695981039346656037ULL)

/**
 * Macro to define xor folding for dynamically set of bits.  This is static inline,
 * so you can call the function and pass a constant value in as the second argument.
 * With gcc and -O2, it should propagate the constant, and this is as good as a
 * separte macro for each possible value of bits.
 *
 * Do not call this function if bits is less than 16.
 */
#define stasis_util_hash_define_fnv_xor_fold_big(TYPE)                        \
static inline TYPE stasis_util_hash_fnv_xor_fold_big_##TYPE(TYPE val, uint8_t bits) {\
  const TYPE mask = ((TYPE)1<<bits)-1;                                        \
  return (val>>bits) ^ (val & mask);                                          \
}

stasis_util_hash_define_fnv_xor_fold_big(uint32_t)
stasis_util_hash_define_fnv_xor_fold_big(uint64_t)

/**
 * @see stasis_util_hash_define_fnv_xor_fold_big
 *
 * Do not call this function if bits is greater than 16.
 */
#define stasis_util_hash_define_fnv_xor_fold_tiny(TYPE)                       \
static inline TYPE stasis_util_hash_fnv_xor_fold_tiny_##TYPE(TYPE val, uint8_t bits) {\
  const TYPE mask = ((TYPE)1<<bits)-1;                                        \
  return (val>>bits ^ val) & mask;                                            \
}

stasis_util_hash_define_fnv_xor_fold_tiny(uint32_t)

static inline uint64_t stasis_util_hash_fnv_xor_fold_uint64_t(uint64_t val, uint8_t bits) {
  if(bits < 16) {
    return
      stasis_util_hash_fnv_xor_fold_tiny_uint32_t(
        stasis_util_hash_fnv_xor_fold_big_uint64_t(val, 32), bits
       );
  } else if(bits < 32) {
    return
      stasis_util_hash_fnv_xor_fold_big_uint32_t(
        stasis_util_hash_fnv_xor_fold_big_uint64_t(val, 32), bits
        );
  } else {
    return stasis_util_hash_fnv_xor_fold_big_uint64_t(val, bits);
  }
}
static inline uint32_t stasis_util_hash_fnv_xor_fold_uint32_t(uint32_t val, uint8_t bits) {
  if(bits < 16) {
    return
      stasis_util_hash_fnv_xor_fold_tiny_uint32_t(val, bits);
  } else {
    return
      stasis_util_hash_fnv_xor_fold_big_uint32_t(val, bits);
  }
}

#endif /* HASHFUNCTIONS_H_ */
