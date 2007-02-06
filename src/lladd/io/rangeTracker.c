#define _GNU_SOURCE
#include <stdio.h>
#include <lladd/io/rangeTracker.h>
#include <lladd/redblack.h>
#include <stdlib.h>
#include <assert.h>


struct rangeTracker { 
  struct RB_ENTRY(tree)* ranges;
  int quantization;
};

static int cmp_transition(const void * a, const void * b, const void * arg) { 
  const transition * ta = a;
  const transition * tb = b;
  
  return ta->pos - tb->pos;

}

rangeTracker * rangeTrackerInit(int quantization) { 
  rangeTracker * ret = malloc(sizeof(rangeTracker));
  ret->ranges = RB_ENTRY(init)(cmp_transition, 0);
  ret->quantization = quantization;
  return ret;
}

void rangeTrackerDeinit(rangeTracker * rt) { 
  RBLIST * l = RB_ENTRY(openlist)(rt->ranges);
  const transition * t;
  while((t = RB_ENTRY(readlist)(l))) { 
    RB_ENTRY(delete)(t, rt->ranges);
    fprintf(stderr, "WARNING: Detected leaked range in rangeTracker!\n");
    // Discard const to free t
    free((void*)t);
  }
  RB_ENTRY(closelist)(l);
  RB_ENTRY(destroy)(rt->ranges);
  free(rt);
}

static void rangeTrackerDelta(rangeTracker * rt, const range * r, int delta) { 

  // Abort on no-op requests.
  assert(delta);  
  assert(r->start < r->stop);


  /** Find predecessor of range */
  transition key;
  int curpin;
  key.pos = r->start;
  // Discarding const.
  transition * t = (transition *)RB_ENTRY(lookup)(RB_LULTEQ, &key, rt->ranges);

  if(t) {
    assert(t->delta);
    assert(t->pins >= 0);
    assert(t->pins + t->delta >= 0);
    if(t->pos != r->start) { 
      int newpins = t->pins + t->delta;
      t = malloc(sizeof(transition));
      t->pos = r->start;
      t->delta = delta;
      t->pins = newpins;
      assert(newpins >= 0);
      RB_ENTRY(search)(t, rt->ranges); // insert
      curpin = t->pins + t->delta;
    } else { 
      t->delta += delta;
      curpin = t->pins + t->delta;
      assert(curpin >= 0);
      if(t->delta == 0) { 
	RB_ENTRY(delete)(t, rt->ranges);
	key.pos = t->pos;
	free(t);
	t = 0;
	t = &key;
      }
    }
  } else { 
    t = malloc(sizeof(transition));
    t->pos = r->start;
    t->delta = delta;
    t->pins = 0; 
    RB_ENTRY(search)(t, rt->ranges); // insert
    curpin = t->pins + t->delta;
    assert(curpin >= 0);
  }

  // t is now set; iterate over the tree until we reach a transition
  // with a >= pos.  Increment each intermediate transition; allocate
  // ranges as necessary.

  // libredblack does not provide a traversal function that starts at
  // a particular point in the tree...
  
  // Discarding const.
  while((t = (transition *) rblookup(RB_LUGREAT, t, rt->ranges)) && t->pos < r->stop) { 
    assert(t);
    t->pins += delta;
    assert(t->delta);
    assert(t->pins == curpin);
    curpin = t->pins + t->delta;
    assert(curpin >= 0);
  }
  if(!t || t->pos != r->stop) { 
    // Need to allocate new transition
    t = malloc(sizeof(transition));
    t->pos = r->stop;
    t->delta = 0-delta;
    t->pins = curpin;
    assert(curpin >= 0);
    RB_ENTRY(search)(t, rt->ranges); // insert
  } else { 
    // Found existing transition at end of range.

    assert(t->pos == r->stop);
    t->pins += delta;
    assert(t->pins == curpin);
    t->delta -= delta;
    
    if(t->delta == 0) { 
      RB_ENTRY(delete)(t, rt->ranges);
      free(t);
    }
  }
}

static range ** rangeTrackerToArray(rangeTracker * rt) { 
  // count ranges.
  int range_count = 0;
  const transition * t;
  int in_range = 0;

  RBLIST * list = RB_ENTRY(openlist) (rt->ranges);
  while((t = RB_ENTRY(readlist)(list))) {
    if(!(t->pins + t->delta)) { 
      // end of a range.
      in_range = 0;
      range_count++;
    } else { 
      in_range = 1;
    }
  }
  RB_ENTRY(closelist)(list);
  if(in_range) { 
    range_count++;
  }

  range ** ret = calloc(range_count + 1, sizeof(range *));

  int next_range = 0;
  in_range = 0;
  list = RB_ENTRY(openlist) (rt->ranges);
  t = RB_ENTRY(readlist)(list);
  if(!t) { 
    assert(range_count == 0);
    RB_ENTRY(closelist)(list);
    return ret;
  } else { 
    assert(!t->pins);
    assert(t->delta);
    assert(! ret[next_range] );
    ret[next_range] = malloc(sizeof(range));
    ret[next_range]->start = t->pos;
    in_range = 1;
  }
  while((t = RB_ENTRY(readlist)(list))) { 
    if(t->pins + t->delta) { 
      if(!in_range) { 
	assert(! ret[next_range]);
	ret[next_range] = malloc(sizeof(range));
	ret[next_range]->start = t->pos;
	in_range = 1;
      }
    } else { 
      // end of range.
      assert(in_range);
      ret[next_range]->stop = t->pos;
      in_range = 0;
      next_range ++;
    }
  }

  RB_ENTRY(closelist)(list);
  assert(next_range == range_count);
  
  return ret;
}


static inline long roundDown(long x, long quant) { 
  return (x / quant) * quant;
}
static inline long roundUp(long x, long quant) { 
  return (((x-1) / quant) + 1) * quant;
}

/** 
    @return a set of ranges that are pinned, and that overlap the request range. 
*/
static rangeTracker * pinnedRanges(const rangeTracker * rt, const range * request, rangeTracker * ret, int delta) { 
  transition key;
  const transition * t;

  key.pos = roundDown(request->start, rt->quantization);

  t = rblookup(RB_LUGTEQ, &key, rt->ranges);

  if(!t) {
    // No ranges after request->start, so no ranges can overlap.
    return ret;
  }
  long range_start;

  // zero if we just encountered the end of a range.  The range runs from
  // range_start to putative_range_stop.  It is possible that a new range 
  // begins on the page that putative_range_stop falls on, so we do not
  // output the range without looking at the next transition.
  int in_range;
  // This is only meaningful if in_range = 0.
  long putative_range_stop;

  if(t) {
    if(roundDown(t->pos, rt->quantization) >= roundUp(request->stop, rt->quantization)) { 
      if(t->pins) { 
	// entire range is pinned.
	range tmp_r;
	tmp_r.start = roundDown(request->start, rt->quantization);
	tmp_r.stop  = roundUp(request->stop, rt->quantization);

	assert(tmp_r.start >= roundDown(request->start, rt->quantization) && tmp_r.stop <= roundUp(request->stop, rt->quantization));
	rangeTrackerDelta(ret, &tmp_r, delta);

	//	printf("0 %s\n", rangeToString(&tmp_r));
      } else { 
	// none of the range is pinned.
      }
      return ret;
    }
    if(t->pins) { 
      // The beginning of request is a range.
      range_start = roundDown(request->start, rt->quantization);
      if(0 == t->pins + t->delta) { 
	in_range = 0;
	// even though we're not in range, we need to see if the next
	// transition starts a range on the same page before returning a
	// new range.
	putative_range_stop = roundUp(t->pos, rt->quantization);
      } else { 
	in_range = 1;
      }
    } else { 
      // The beginning of the request is not a range.
      range_start = roundDown(t->pos, rt->quantization);
      in_range = 1;
      assert(t->delta);
    }
  }
  while((t = rblookup(RB_LUGREAT, t, rt->ranges))) { 
    assert(t->delta);

    if(roundUp(t->pos, rt->quantization) >= roundUp(request->stop, rt->quantization)) { 
      if(in_range) { 
	// if we're in range, part of the last page must be pinned.
	in_range = 0;
	putative_range_stop = roundUp(request->stop, rt->quantization);
      } else {
	// is this transition in the last page?  If so, part of the last page must be pinned.
	if(t->pos < roundUp(request->stop, rt->quantization)) {
	  // in_range == 0
	  assert(t->pins == 0);
	  range_start = roundDown(t->pos, rt->quantization);
	  putative_range_stop = roundUp(request->stop, rt->quantization);
	}
      }
      break;
    }
    if(t->pins) { 
      assert(in_range);

      if(!(t->pins + t->delta)) { 
	putative_range_stop = roundUp(t->pos, rt->quantization);
	in_range = 0;
      } 

    } else { // ! t->pins
      assert(!in_range);

      if(putative_range_stop < roundDown(t->pos, rt->quantization)) { 
	// output a new range
	range tmp_r;
	tmp_r.start = range_start;
	tmp_r.stop  = putative_range_stop;
	if(tmp_r.start != tmp_r.stop) { 
	  assert(tmp_r.start >= roundDown(request->start, rt->quantization) && tmp_r.stop <= roundUp(request->stop, rt->quantization));
	  rangeTrackerDelta(ret, &tmp_r, delta);
	  //	  printf("1 %s\n", rangeToString(&tmp_r));
	}
	range_start = roundDown(t->pos, rt->quantization);
      } else { 
	// extend old range.
      }
      in_range = 1;
    }
  }
  assert(!in_range);
  range tmp_r;
  tmp_r.start = range_start;
  tmp_r.stop  = putative_range_stop;
  if(tmp_r.start != tmp_r.stop) { 
    assert(tmp_r.start >= roundDown(request->start, rt->quantization) && tmp_r.stop <= roundUp(request->stop, rt->quantization));
    rangeTrackerDelta(ret, &tmp_r, delta);
    //    printf("2 %s\n", rangeToString(&tmp_r));
    
  }
  return ret;
}

range ** rangeTrackerAdd(rangeTracker * rt, const range * rng) { 
  //  printf("pinnedRanges before add %s\n", rangeToString(rng));
  rangeTracker * ret = rangeTrackerInit(rt->quantization);
  pinnedRanges(rt, rng, ret, 1);
  rangeTrackerDelta(rt, rng, 1);
  //  printf("pinnedRanges after  add\n");
  rangeTracker * ret2 = rangeTrackerInit(rt->quantization);
  pinnedRanges(rt, rng, ret2, 1);
  
  range ** ret_arry = rangeTrackerToArray(ret);

  // remove the first array from the second...
  int i = 0;
  
  while(ret_arry[i]) { 
    rangeTrackerDelta(ret2, ret_arry[i], -1);
    // while we're at it, deinit the first range tracker
    rangeTrackerDelta(ret, ret_arry[i], -1);
    free(ret_arry[i]);
    i++;
  }
  free(ret_arry);
  rangeTrackerDeinit(ret);
  
  i = 0;
  ret_arry = rangeTrackerToArray(ret2);

  while(ret_arry[i]) { 
    rangeTrackerDelta(ret2, ret_arry[i], -1);
    i++;
  }
  rangeTrackerDeinit(ret2);

  return ret_arry;

  /*  // Need to return pinned ranges that overlap r.

  transition key;
  const transition * t;

  key.pos = rng->start;
  
  t = rblookup(RB_LULTEQ, &key, rt->ranges);  // could be less than if the new range touches an existing range.

  assert(t);
  
  range r;
  int in_range = 1;
  r.start = roundDown(t->pos, rt->quantization);

  while((t = rblookup(RB_LUGREAT, t, rt->ranges))) { 
    if(!(t->pins + t->delta)) { 
      assert(in_range);
      in_range = 0;
      r.stop = roundUp(t->pos, rt->quantization);
      //      printf("add range: [%lld-%lld]\n", (long long)r.start, (long long)r.stop);
    } else if(!in_range) { 
	assert(t->pins == 0);
	in_range = 1;
	r.start = roundDown(t->pos, rt->quantization);
    }
    if(t->pos >= rng->stop) { break; }
    } */
}

/** 
    Remove a range
    @return a null terminated array of newly-unpinned, quantized ranges
*/

range ** rangeTrackerRemove(rangeTracker * rt, const range * rang) { 
  rangeTracker * ret = rangeTrackerInit(rt->quantization);
  //  printf("pinnedRanges, before del %s\n", rangeToString(rang));
  pinnedRanges(rt, rang, ret, 1);
  rangeTrackerDelta(rt, rang, -1);
  //  printf("pinnedRanges, after  del\n");
  pinnedRanges(rt, rang, ret, -1);

  range ** ret_arry = rangeTrackerToArray(ret);

  int i = 0;
  while(ret_arry[i]) { 
    rangeTrackerDelta(ret, ret_arry[i], -1);
    i++;
  }
  rangeTrackerDeinit(ret);

  return ret_arry;

  /*  // Need to return completely unpinned ranges that overlap r.

  range bigger;
  bigger.start = roundDown(rang->start, rt->quantization);
  bigger.stop = roundUp(rang->stop, rt->quantization);

  transition key;

  key.pos = bigger.start;

  int unpinned_range = 0;
  range r;

  const transition * t = RB_ENTRY(lookup)(RB_LUGTEQ, &key, rt->ranges);

  long last_end = bigger.start;
  
  // special case beginning of range
  if(! t) { 
    t = RB_ENTRY(lookup)(RB_LULESS, &key, rt->ranges);
    if(!t || 0 == t->pins + t->delta) { 
      //      printf("A %s\n", rangeToString(&bigger));
      return;
    }
  } else if(t->pins == 0) { 
    r.start = bigger.start;
    r.stop = roundDown(t->pos, rt->quantization);
    if(r.start < r.stop) { 
      //      printf("0 %s\n", rangeToString(&r));
    }
    unpinned_range = 1;
    assert(0 != t->pins + t->delta);
    t = RB_ENTRY(lookup)(RB_LUGREAT, t, rt->ranges);
  }
  if(t) { 
    unpinned_range = (0 == t->pins + t->delta);
  }

  while(t) { 

    if(t->pins == 0) { 
      // XXX don't care if range bleeds outside of initially pinned range (for now; the difference could be huge in sparse applications)
      r.start = roundUp(last_end, rt->quantization);
      r.stop   = roundDown(t->pos, rt->quantization);
      assert(unpinned_range);
      if(r.start < r.stop) {
	//	printf("B %s\n", rangeToString(&r));
      } else { 
	//	printf(".. %s  bigger = %s\n", rangeToString(&r), rangeToString(&bigger));
      }
    }
    // break after processing transition that runs over the edge...
    if(t->pos >= bigger.stop) { break; }

    last_end = t->pos;
    unpinned_range = (0 == t->pins + t->delta);
    t = RB_ENTRY(lookup)(RB_LUGREAT, t, rt->ranges);
  }
  if(! t && unpinned_range) { 
    r.start = roundUp(last_end, rt->quantization);
    r.stop = bigger.stop;
    if(r.start < r.stop) { 
      //      printf("C %s\n", rangeToString(&r));
    }
  }

  // plan:  (1) Write roundDown, roundUp macros
  // enumerate pinned pages before and after operation.  (Note that we need to round down and up to get stuff outside the range, but on common pages)
  // return before - after, or after - before, depending on whether this is an add or a remove.
  */

}

const transition ** rangeTrackerEnumerate(rangeTracker * rt) {
  int transitionCount = 0;
  const transition * t;
  RBLIST * list =  RB_ENTRY(openlist) (rt->ranges);
  while((t = RB_ENTRY(readlist)(list))) { 
    transitionCount++;
  }
  RB_ENTRY(closelist)(list);

  const transition ** ret = malloc(sizeof(transition **) * (transitionCount + 1));

  list =  RB_ENTRY(openlist) (rt->ranges);
  int i = 0;

  while((t = RB_ENTRY(readlist)(list))) { 
    ret[i] = t;
    i++;
  }
  ret[i] = 0;
  RB_ENTRY(closelist)(list);
  return ret;
}

char * rangeToString(const range * r) { 
  char * ret;
  int err = asprintf(&ret, "[range %lld-%lld]", (long long)r->start, (long long)r->stop);
  assert(err !=-1);
  return ret;
}

char * transitionToString(const transition * t) { 
  char * ret;
  int err = asprintf(&ret, "[transition pos=%lld delta=%d pins=%d]", (long long)t->pos, t->delta, t->pins);
  assert(err !=-1);
  return ret;
}

