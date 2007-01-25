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
    if(t->pos != r->start) { 
      int newpins = t->pins + t->delta;
      t = malloc(sizeof(transition));
      t->pos = r->start;
      t->delta = delta;
      t->pins = newpins;
      RB_ENTRY(search)(t, rt->ranges); // insert
      curpin = t->pins + t->delta;
    } else { 
      t->delta += delta;
      curpin = t->pins + t->delta;
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
  }
  if(!t || t->pos != r->stop) { 
    // Need to allocate new transition
    t = malloc(sizeof(transition));
    t->pos = r->stop;
    t->delta = 0-delta;
    t->pins = curpin;
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

range ** rangeTrackerAdd(rangeTracker * rt, const range * r) { 
  rangeTrackerDelta(rt, r, 1);
}

/** 
    Remove a range
    @return a null terminated array of newly-unpinned, quantized ranges
*/

range ** rangeTrackerRemove(rangeTracker * rt, const range * r) { 
  rangeTrackerDelta(rt, r, -1);
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
