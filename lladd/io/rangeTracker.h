typedef struct range { 
  long start;
  long end;
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
    @return a null terminated array of newly-pinned, quantized ranges 
*/
range ** rangeTrackerAdd(rangeTracker * rt, const range * r);
/** 
    Remove a range
    @return a null terminated array of newly-unpinned, quantized ranges
*/
range ** rangeTrackerRemove(rangeTracker * rt, const range * r);

const transition ** enumTransitions(rangeTracker * rt);
char * rangeToString(const range * r);
char * transitionToString(const transition * t);

