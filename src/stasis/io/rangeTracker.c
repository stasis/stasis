#include <config.h>
#include <stasis/common.h>
#undef STLSEARCH // XXX
#include <stasis/util/redblack.h>


#include <stasis/io/rangeTracker.h>

#include <stdio.h>
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
  rangeTracker * ret = stasis_alloc(rangeTracker);
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
      t = stasis_alloc(transition);
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
    t = stasis_alloc(transition);
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
    t = stasis_alloc(transition);
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

  range ** ret = stasis_calloc(range_count + 1, range *);

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
    ret[next_range] = stasis_alloc(range);
    ret[next_range]->start = t->pos;
    in_range = 1;
  }
  while((t = RB_ENTRY(readlist)(list))) {
    if(t->pins + t->delta) {
      if(!in_range) {
	assert(! ret[next_range]);
	ret[next_range] = stasis_alloc(range);
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


static void pinnedRanges(const rangeTracker * rt, const range * request, rangeTracker * ret, int delta) {
  transition key;
  // we will start looking at the tree at the first transition after key.pos.
  key.pos = rangeTrackerRoundDown(request->start, rt->quantization);
  const transition * t = &key;

  int in_range = 0;   // 0 if the last transition marked the end of a range.
  int have_range = 0; // 1 once we have encountered the first range.
  range cur;

  range expanded_range;
  expanded_range.start = rangeTrackerRoundDown(request->start, rt->quantization);
  expanded_range.stop  = rangeTrackerRoundUp(request->stop, rt->quantization);

  while((t = rblookup(RB_LUGREAT, t, rt->ranges))) {
    assert(t->delta);
    if(t->pos >= expanded_range.stop) {
      if(in_range) {
	assert(t->pins);
	assert(have_range);
	cur.stop = expanded_range.stop;
	assert(cur.stop != cur.start);
	in_range = 0;
      } else {
	if(!have_range) {
	  // we are at first transition
	  if(t->pins) {
	    cur = expanded_range;
	    have_range = 1;
	  } else {
	    // no ranges are pinned.
	  }
	}
      }
      // Pretend we hit the end of the tree.
      break;
    } else {
      if(in_range) {
	assert(have_range);
	assert(t->pins);
	if(!(t->pins + t->delta)) {
	  cur.stop = rangeTrackerRoundUp(t->pos, rt->quantization);
	  assert(cur.stop != cur.start);
	  in_range = 0;
	} else {
	  assert(in_range);
	}
      } else {
	// not in range.
	if(!have_range) {
	  // we are at first transition
	  if(t->pins) {
	    cur.start = expanded_range.start;
	    in_range = t->pins + t->delta;
	    if(! in_range) {
	      cur.stop = rangeTrackerRoundUp(t->pos, rt->quantization);
	      assert(cur.stop != cur.start);
	    }
	  } else {
	    cur.start = rangeTrackerRoundDown(t->pos, rt->quantization);
	    in_range = 1;
	    assert(t->pins + t->delta);
	  }
	  have_range = 1;
	} else {
	  assert(! t->pins);
	  assert(t->pins + t->delta);
	  // do we need to merge this transition with the range, or output the old range?
	  if(cur.stop >= rangeTrackerRoundDown(t->pos, rt->quantization)) {
	    // do nothing; start position doesn't change.
	  } else {
	    // output old range, reset start position
	    rangeTrackerDelta(ret, &cur, delta);
	    cur.start = rangeTrackerRoundDown(t->pos, rt->quantization);
	  }
	  in_range = 1;
	}
      }
    }
  }

  assert(!in_range);
  if(have_range) {
    rangeTrackerDelta(ret, &cur, delta);
  }

}

range ** rangeTrackerAdd(rangeTracker * rt, const range * rng) {
  rangeTracker * ret = rangeTrackerInit(rt->quantization);
  pinnedRanges(rt, rng, ret, 1);
  rangeTrackerDelta(rt, rng, 1);
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
}

/**
    Remove a range
    @return a null terminated array of newly-unpinned, quantized ranges
*/

range ** rangeTrackerRemove(rangeTracker * rt, const range * rang) {
  rangeTracker * ret = rangeTrackerInit(rt->quantization);
  pinnedRanges(rt, rang, ret, 1);
  rangeTrackerDelta(rt, rang, -1);
  pinnedRanges(rt, rang, ret, -1);

  range ** ret_arry = rangeTrackerToArray(ret);

  int i = 0;
  while(ret_arry[i]) {
    rangeTrackerDelta(ret, ret_arry[i], -1);
    i++;
  }
  rangeTrackerDeinit(ret);

  return ret_arry;

}

const transition ** rangeTrackerEnumerate(rangeTracker * rt) {
  int transitionCount = 0;
  const transition * t;
  RBLIST * list =  RB_ENTRY(openlist) (rt->ranges);
  while((t = RB_ENTRY(readlist)(list))) {
    transitionCount++;
  }
  RB_ENTRY(closelist)(list);

  const transition ** ret = stasis_malloc(transitionCount + 1, const transition*);

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
