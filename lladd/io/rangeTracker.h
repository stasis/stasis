typedef struct range { 
  long start;
  long stop;
} range;

typedef struct transition { 
  long pos;
  // negative if end of range.  Never zero
  int delta;  
  // >= abs(delta).  Number of times the range immediately less than
  // this point is pinned.
  int pins;   
} transition;

typedef struct rangeTracker rangeTracker;

rangeTracker * rangeTrackerInit(int quantization);
void rangeTrackerDeinit(rangeTracker * rt);

/** 
    Add a new range

    @return a null terminated array of newly-pinned, quantized ranges.
    This array might contain ranges that were already pinned, and/or
    ones that overlap (this aspect of the behavior is intentionally
    left unspecified).
*/
range ** rangeTrackerAdd(rangeTracker * rt, const range * r);
/** 
    Remove a range 

    @return a null terminated array of unpinned, quantized ranges.
    
    @see rangeTrackerAdd for a discussion of approximations that may
    be applied to rangeTrackerRemove's return value.
*/
range ** rangeTrackerRemove(rangeTracker * rt, const range * r);

const transition ** rangeTrackerEnumerate(rangeTracker * rt);
char * rangeToString(const range * r);
char * transitionToString(const transition * t);

