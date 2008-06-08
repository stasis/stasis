#ifndef _ROSE_COMPRESSION_BINARY_SEARCH_H__
#define _ROSE_COMPRESSION_BINARY_SEARCH_H__

// From wikipedia

int binary_search(int * A, int value, intptr_t high, intptr_t low) {
  int N = high;
  //  low = 0;
  //  high = N;
  while (low < high) {
    intptr_t mid = low + ((high - low) / 2);
    if (A[mid] < value)
      low = mid + 1; 
    else
      //can't be high = mid-1: here A[mid] >= value,
      //so high can't be < mid if A[mid] == value
      high = mid; 
  }
  // high == low, using high or low depends on taste 
  if ((low < N) && (A[low] == value))
    return low; // found
  else
    return -1; // not found        
}


#define rose_binary_search(accessor) do{				\
    int64_t bs_N = bs_high;						\
    int64_t bs_mid;							\
    while (bs_low < bs_high) {						\
      bs_mid = bs_low + ((bs_high - bs_low) / 2);			\
      if (*accessor(bs_mid) /*A[mid]*/ < bs_value)			\
	bs_low = bs_mid + 1;						\
      else								\
	/*can't be high = mid-1: here A[mid] >= value,*/		\
	/*so high can't be < mid if A[mid] == value*/			\
	bs_high = bs_mid;						\
    }									\
    /* high == low, using high or low depends on taste */		\
    if ((bs_low < bs_N) && (*accessor(bs_low)/*A[low]*/ == bs_value))	\
      bs_ret = bs_low; /* found */					\
    else								\
      bs_ret = -1; /* not found */					\
  } while(0)

/** Find the insertion point for a value in the array.  Assumes that items stored in the array are unique. */

#define rose_binary_search_greatest_lower(accessor) do{				\
    int64_t bs_N = bs_high;						\
    int64_t bs_mid;							\
    while (bs_low < bs_high) {						\
      bs_mid = bs_low + ((bs_high - bs_low) / 2);			\
      if (accessor(bs_mid) /*A[mid]*/ < bs_value)			\
	bs_low = bs_mid + 1;						\
      else								\
	/*can't be high = mid-1: here A[mid] >= value,*/		\
	/*so high can't be < mid if A[mid] == value*/			\
	bs_high = bs_mid;						\
    }									\
    /* high == low, using high or low depends on taste */		\
    if ((bs_low < bs_N))						\
      bs_ret = bs_low; /* found */					\
    else								\
      bs_ret = -1; /* not found */					\
  } while(0)


#endif // _ROSE_COMPRESSION_BINARY_SEARCH_H__
