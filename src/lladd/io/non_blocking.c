#include <lladd/io/handle.h>
#include <lladd/redblack.h>
#include <pthread.h>
#include <errno.h>
#include <stdlib.h>
#include <assert.h>
#include <stdio.h>
#include <linkedlist.h>
/*

  Design:

  data structures: A rb tree holds a set of "fast" handles that manage
  disjoint regions.  Each "fast" handle corresponds to an outstanding
  write.  Worker threads then flush "fast" handle contents into the
  "slow" handle.  Reads are serviced from the fast handles, and the
  slow handle is used to fill any holes that exist within the read
  range.  (This implementation resorts to copies when necessary...  it
  is possible for a read or write to block on a memcpy(), but writes
  may not block on disk i/o.)

*/

static inline stasis_read_buffer_t * alloc_read_buffer_error(stasis_handle_t * h, int error) {
  assert(error);
  stasis_read_buffer_t * r = malloc(sizeof(stasis_read_buffer_t));
  r->h = h;
  r->buf = 0;
  r->len = 0;
  r->impl = 0;
  r->error = error;
  return r;
}

static inline stasis_write_buffer_t * alloc_write_buffer_error(stasis_handle_t * h, int error) {
  assert(error);
  stasis_write_buffer_t * w = malloc(sizeof(stasis_write_buffer_t));
  w->h = h;
  w->off = 0;
  w->buf = 0;
  w->len = 0;
  w->impl = 0;
  w->error = error;
  return w;
}

typedef struct tree_node { 
  lsn_t start_pos;
  lsn_t end_pos;
  stasis_handle_t * h;
  int pin_count;  // held when the handle can not be deallocated, (when it's being written, read, or flushed to disk)
  int dirty;      // set to 1 when the handle is written to, 0 when the handle is written back to disk
} tree_node;

static int cmp_handle(const void * ap, const void * bp, const void * ignored) {
  tree_node * a = (tree_node*)ap;
  tree_node * b = (tree_node*)bp;
  if(a->start_pos == b->start_pos && 
     a->start_pos == a->end_pos &&
     b->start_pos == b->end_pos ) { 
    return 0;  // Special case:  two zero length regions that start at the same place are equal.
  }
  // Otherwise, the regions must overlap:
  // (0,0) == (0,0) ; (1,0) == (0,4) ; (0,0) < (0,4)
  if(a->end_pos <= b->start_pos) { 
    return -1;
  } else if(a->start_pos >= b->end_pos) {
    return 1;
  } else { 
    return 0;
  }

}


typedef struct nbw_read_buffer_impl { 
  stasis_read_buffer_t * buffer;
  tree_node * n;  
} nbw_read_buffer_impl;

typedef struct nbw_impl {
  pthread_mutex_t mut;
  // Need more than one....
  stasis_handle_t * (*slow_factory)(void * arg);
  void * slow_factory_arg;
  
  stasis_handle_t * (*fast_factory)(lsn_t off, lsn_t len, void * arg);
  void * fast_factory_arg;
  struct RB_ENTRY(tree) * fast_handles;

  pthread_t * workers;
  int worker_count;
  
  tree_node * last_flushed;

  pthread_cond_t pending_writes_cond;
  LinkedList pending_writes;

  int still_open; 

  lsn_t buffer_size;
  lsn_t used_buffer_size;

  LinkedList * slow_handles;
  int slow_handle_count;

  // The rest of the values are caches maintained for efficiency.

  /*  int num_copies;
      int num_copies_buffer; */
  lsn_t start_pos;
  lsn_t end_pos; 
} nbw_impl;

static stasis_handle_t * getSlowHandle(nbw_impl * impl) { 
  stasis_handle_t * slow = (stasis_handle_t*) popMaxVal(&impl->slow_handles);
  if((long)slow == -1) {
    slow = impl->slow_factory(impl->slow_factory_arg);
    impl->slow_handle_count++;
  }
  return slow;
}

static void releaseSlowHandle(nbw_impl * impl, stasis_handle_t * slow) { 
  addVal(&impl->slow_handles, (long) slow);
}
static inline stasis_handle_t * getFastHandle(nbw_impl * impl, lsn_t off, lsn_t len, int allocIfMissing) { 
  tree_node * np = malloc(sizeof(tree_node));
  np->start_pos = off;
  np->end_pos = off+len;
  if(allocIfMissing) { 
    //    printf("lookup (%ld, %ld); ", np->start_pos, np->end_pos);
    const tree_node * n = RB_ENTRY(search)(np, impl->fast_handles);
    if(n != np) {
      //      printf("found\n");
      // The node was already in the tree
      assert(np->start_pos == n->start_pos && np->end_pos == n->end_pos);
      free(np);
    } else {
      //      printf("not found\n");
      assert(RB_ENTRY(find)(n, impl->fast_handles));
      // The node wasn't in the tree and search inserted it for us.
      // Assignment is safe because we're writing to a field of n that is ignored by cmp.
      ((tree_node*)n)->h = impl->fast_factory(off, len, impl->fast_factory_arg);
    }
    //    fflush(stdout);
    ((tree_node*)n)->pin_count++;
    return n->h;
  } else { 
    const tree_node * n = RB_ENTRY(find)(np, impl->fast_handles);
    free(np);
    if(!n) { 
      return 0;
    } else { 
      ((tree_node*)n)->pin_count++;
      return n->h;
    }
  }
}

static inline int releaseFastHandle(nbw_impl * impl, lsn_t off, lsn_t len, 
				    stasis_handle_t * fast, int setDirty) {
  tree_node * np = malloc(sizeof(tree_node));
  np->start_pos = off;
  np->end_pos = off+len;
  if(fast) { 
    assert(off == fast->start_position(fast));
    assert(off + len == fast->end_position(fast));
  }
  const tree_node * n = RB_ENTRY(find)(np, impl->fast_handles);
  free(np);
  assert(n);
  if(fast) { 
    assert(n->h == fast);
  }
  ((tree_node*)n)->pin_count--;
  if(setDirty) ((tree_node*)n)->dirty = 1;

  return 0;
}

static int nbw_num_copies(stasis_handle_t * h) { 
  nbw_impl * impl = (nbw_impl*) h->impl;
  stasis_handle_t * slow = getSlowHandle(impl);
  int slow_num_copies = slow->num_copies(slow);
  releaseSlowHandle(impl, slow);
  stasis_handle_t * fast = impl->fast_factory(0, 0, impl->fast_factory_arg);
  int fast_num_copies = fast->num_copies(fast);
  fast->close(fast);
  return slow_num_copies > fast_num_copies ? slow_num_copies : fast_num_copies;
}
static int nbw_num_copies_buffer(stasis_handle_t * h) { 
  nbw_impl * impl = (nbw_impl*) h->impl; 
  stasis_handle_t * slow = getSlowHandle(impl);
  int slow_num_copies_buffer = slow->num_copies_buffer(slow);
  releaseSlowHandle(impl, slow);
  stasis_handle_t * fast = impl->fast_factory(0, 0, impl->fast_factory_arg);
  int fast_num_copies_buffer = fast->num_copies_buffer(fast);
  fast->close(fast);
  return slow_num_copies_buffer > fast_num_copies_buffer ? slow_num_copies_buffer : fast_num_copies_buffer;
}
static int nbw_close(stasis_handle_t * h) { 
  printf("Warning: nbw_close leaks fast handles, and never flushes them to the slow handle...\n");
  nbw_impl * impl = h->impl;
  stasis_handle_t * slow;

  pthread_mutex_lock(&impl->mut);
  impl->still_open = 0;
  pthread_mutex_unlock(&impl->mut);
  pthread_cond_broadcast(&impl->pending_writes_cond);
  
  for(int i = 0; i < impl->worker_count; i++) { 
    pthread_join(impl->workers[i], 0);
  }
  
  free(impl->workers);

  // foreach fast handle .. close fast handle...

  printf("nbw had %d slow handles\n", impl->slow_handle_count);
  fflush(stdout);

  while(-1 != (long)(slow = (stasis_handle_t *)popMaxVal(&(impl->slow_handles)))) { 
    slow->close(slow);
    impl->slow_handle_count --;
  }
  destroyList(&(impl->slow_handles));
  assert(impl->slow_handle_count == 0);

  free(h->impl);
  free(h);
  return 0;
}
static lsn_t nbw_start_position(stasis_handle_t *h) { 
  nbw_impl * impl = (nbw_impl*) h->impl;
  pthread_mutex_lock(&(impl->mut));
  lsn_t ret = impl->start_pos;
  pthread_mutex_unlock(&(impl->mut));
  return ret;
}
static lsn_t nbw_end_position(stasis_handle_t *h) { 
  nbw_impl * impl = (nbw_impl*) h->impl;
  pthread_mutex_lock(&(impl->mut));
  lsn_t ret = impl->start_pos;
  pthread_mutex_unlock(&(impl->mut));
  return ret;
}
static stasis_write_buffer_t * nbw_write_buffer(stasis_handle_t * h, 
						lsn_t off, lsn_t len) {
  nbw_impl * impl = (nbw_impl*) h->impl;
  int error = 0;

  stasis_write_buffer_t * w = 0;

  pthread_mutex_lock(&impl->mut);
  if(off < impl->start_pos) { 
    error = EDOM;
  } else { 
    if(off + len >= impl->end_pos) { 
      impl->end_pos = off + len;
    }
    stasis_handle_t * fast = getFastHandle(impl, off, len, 1);
    w = fast->write_buffer(fast, off, len);
  }
  pthread_mutex_unlock(&impl->mut);
  
  if(!w) { 
    w = alloc_write_buffer_error(h, error);
  } else { 
    stasis_write_buffer_t * ret = malloc(sizeof(stasis_write_buffer_t));
    ret->h = h;
    if(!w->error) { 
      assert(w->off == off);
      assert(w->len == len);
    }
    ret->off = off;
    ret->len = len;
    ret->buf = w->buf;
    ret->impl = w; 
    ret->error = w->error;
    w = ret;
  }
  return w;
  
}
static stasis_write_buffer_t * nbw_append_buffer(stasis_handle_t * h, 
						   lsn_t len) { 
  nbw_impl * impl = (nbw_impl*) h->impl;
  pthread_mutex_lock(&(impl->mut));
  lsn_t off = impl->end_pos;
  impl->end_pos = off+len;
  stasis_handle_t * fast = getFastHandle(impl, off, len, 1);
  stasis_write_buffer_t * w = fast->write_buffer(fast, off, len);
  pthread_mutex_unlock(&(impl->mut));
  // XXX error handling?!?
  
  stasis_write_buffer_t * ret = malloc(sizeof(stasis_write_buffer_t));
  ret->h = h;
  ret->off = off;
  ret->len = len;
  ret->buf = w->buf;
  ret->impl = w;
  ret->error = w->error;
  w = ret;

  return w;
}
static int nbw_release_write_buffer(stasis_write_buffer_t * w) { 
  nbw_impl * impl = w->h->impl;
  pthread_mutex_lock(&impl->mut);
  if(w->error) { 
  } else {
    stasis_write_buffer_t * w_wrapped = ((stasis_write_buffer_t*)w->impl);
    w_wrapped->h->release_write_buffer(w_wrapped);
    releaseFastHandle(w->h->impl, w->off, w->len, 0, 1);
    //    printf("singalling workers\n");
    pthread_cond_signal(&impl->pending_writes_cond);
  }
  free(w);
  pthread_mutex_unlock(&impl->mut);
  return 0;
}
static stasis_read_buffer_t * nbw_read_buffer(stasis_handle_t * h,
					      lsn_t off, lsn_t len) { 
  nbw_impl * impl = (nbw_impl*) h->impl;
  pthread_mutex_lock(&impl->mut);
  stasis_read_buffer_t * r = 0;
  int error = 0;
  if(off < impl->start_pos || off + len > impl->end_pos) { 
    error = EDOM;
  } else { 
    stasis_handle_t * fast = getFastHandle(impl, off, len, 0);

    if(fast) { 
      r = fast->read_buffer(fast, off, len);
      if(r) { 
	stasis_read_buffer_t * ret = malloc(sizeof(stasis_read_buffer_t));
	ret->h = h;
	ret->off = off;
	ret->len = len;
	ret->buf = r->buf;
	ret->impl = r;
	ret->error = r->error;
	r = ret;
      }
    } else { 
      stasis_handle_t * slow = getSlowHandle(impl);
      r = slow->read_buffer(slow, off, len);
      releaseSlowHandle(impl, slow);
    }
  }
  if(!r) { 
    r = alloc_read_buffer_error(h, error);
  }
  pthread_mutex_unlock(&impl->mut);
  return r;
}
static int nbw_release_read_buffer(stasis_read_buffer_t * r) { 
  nbw_impl * impl = r->h->impl;
  pthread_mutex_lock(&impl->mut);
  
  if(r->error) { 
  } else { 
    stasis_read_buffer_t * r_wrapped = ((stasis_read_buffer_t*)r->impl);
    r_wrapped->h->release_read_buffer(r_wrapped);
    releaseFastHandle(r->h->impl, r->off, r->len, 0, 0);
  }
  free(r);
  pthread_mutex_unlock(&impl->mut);
  return 0;

}
static int nbw_write(stasis_handle_t * h, lsn_t off, 
		     const byte * dat, lsn_t len) { 
  nbw_impl * impl = (nbw_impl*) h->impl;
  pthread_mutex_lock(&impl->mut);
  int error = 0;
  if(off < impl->start_pos) { 
    error = EDOM;
  } else {
    stasis_handle_t * fast = getFastHandle(impl, off, len, 1);
    if(off + len > impl->end_pos) { 
      impl->end_pos = off + len;
    }
    error = fast->write(fast, off, dat, len);
    releaseFastHandle(impl, off, len, fast, 1);
    //    printf("singailling workers\n");
    pthread_cond_signal(&impl->pending_writes_cond);
  }
  pthread_mutex_unlock(&impl->mut);
  return error;
}
static int nbw_append(stasis_handle_t * h, lsn_t * off, 
		      const byte * dat, lsn_t len) { 
  nbw_impl * impl = (nbw_impl *) h->impl;
  int error = 0;
  pthread_mutex_lock(&impl->mut);
  *off = impl->end_pos;
  stasis_handle_t * fast = getFastHandle(impl, *off, len, 1);
  impl->end_pos = *off + len;
  error = fast->write(fast, *off, dat, len);
  releaseFastHandle(impl, *off, len, fast, 1);
  //  printf("singalling workers\n");
  pthread_cond_signal(&impl->pending_writes_cond);
  pthread_mutex_unlock(&impl->mut);
  return error;
}
static int nbw_read(stasis_handle_t * h, 
		    lsn_t off, byte * buf, lsn_t len) { 
  nbw_impl * impl = (nbw_impl *) h->impl;
  int error = 0;
  pthread_mutex_lock(&impl->mut);
  //  printf("got lock"); fflush(stdout);
  if(off < impl->start_pos || off + len > impl->end_pos) {
    error = EDOM;
    //    printf("error"); fflush(stdout);
  } else {
    //    printf("getting handle"); fflush(stdout);
    stasis_handle_t * fast = getFastHandle(impl, off, len, 0);
    //    printf("got handle"); fflush(stdout);
    if(fast) { 
      //      printf("fast"); fflush(stdout);
      error = fast->read(fast, off, buf, len);
      releaseFastHandle(impl, off, len, fast, 0);
    } else  { 
      //      printf("slow"); fflush(stdout);
      stasis_handle_t * slow = getSlowHandle(impl);
      pthread_mutex_unlock(&impl->mut);
      error = slow->read(slow, off, buf, len);
      pthread_mutex_lock(&impl->mut);
      releaseSlowHandle(impl, slow);
    }
    //    printf("done"); fflush(stdout);
  }
  pthread_mutex_unlock(&impl->mut);
  return error;
}
static int nbw_truncate_start(stasis_handle_t * h, lsn_t new_start) { 
  nbw_impl * impl = h->impl;
  int error = 0;
  pthread_mutex_lock(&impl->mut);
  if(new_start <= impl->end_pos && new_start > impl->start_pos) { 
    impl->start_pos = new_start;
  } else { 
    error = EDOM;
  }
  pthread_mutex_unlock(&impl->mut);
  return error;
}

struct stasis_handle_t nbw_func = {
  .num_copies = nbw_num_copies,
  .num_copies_buffer = nbw_num_copies_buffer,
  .close = nbw_close,
  .start_position = nbw_start_position,
  .end_position = nbw_end_position,
  .write = nbw_write,
  .append = nbw_append,
  .write_buffer = nbw_write_buffer,
  .append_buffer = nbw_append_buffer,
  .release_write_buffer = nbw_release_write_buffer,
  .read = nbw_read,
  .read_buffer = nbw_read_buffer,
  .release_read_buffer = nbw_release_read_buffer,
  .truncate_start = nbw_truncate_start,
  .error = 0
};

static void * nbw_worker(void * handle) {
  stasis_handle_t * h = (stasis_handle_t*)handle;
  nbw_impl * impl = (nbw_impl*)h->impl;

  pthread_mutex_lock(&impl->mut);

  while(impl->still_open) { 
    //    printf("still open");
    // pick next node.
    tree_node * to_flush;
    int checked_from_start = 0;
    if(impl->last_flushed) { 
      // cast removes const
      to_flush = (tree_node*)RB_ENTRY(lookup)(RB_LUGREAT, impl->last_flushed, 
				  impl->fast_handles);
    } else { 
      to_flush = 0;
    }
    while((to_flush || ! checked_from_start) && (to_flush != impl->last_flushed || to_flush == 0)) {
      if(to_flush) { 
	if(to_flush->dirty) { break; }
	//	printf("clean node..");
	// strip const
	to_flush = (tree_node*)RB_ENTRY(lookup)(RB_LUGREAT, to_flush, 
				    impl->fast_handles);
      } else { 	
	//	printf("looking at beginning of tree..");
	// strip const
	to_flush = (tree_node*)RB_ENTRY(min)(impl->fast_handles);
	checked_from_start = 1;
      }
    }
    if(!to_flush) { 
      //      printf("nothing to flush");
      pthread_cond_wait(&impl->pending_writes_cond, &impl->mut);
    } else { 
      impl->last_flushed = to_flush;
      to_flush->pin_count++;

      stasis_handle_t * slow = getSlowHandle(impl);
      
      pthread_mutex_unlock(&impl->mut);
      
      
      // @todo need a sendfile-style thing...
      
      stasis_handle_t * fast = to_flush->h;
      
      lsn_t off = fast->start_position(fast);
      lsn_t len = fast->end_position(fast) - off;
      stasis_read_buffer_t * r = fast->read_buffer(fast, off, len);

      //      printf("%ld, Flushing %ld to %ld\n", pthread_self(), off, off+len);fflush(stdout);
      
      slow->write(slow, off, r->buf, len);
      
      r->h->release_read_buffer(r);
      
      pthread_mutex_lock(&impl->mut);
      
      releaseSlowHandle(impl, slow);
      to_flush->pin_count--;

    }
  }

  pthread_mutex_unlock(&impl->mut);
  return 0;
}


stasis_handle_t * stasis_handle(open_non_blocking)(stasis_handle_t * (*slow_factory)(void * arg),
						   void * slow_factory_arg,
						   stasis_handle_t * (*fast_factory)(lsn_t, lsn_t, void *),
						   void * fast_factory_arg,
						   int worker_thread_count,
						   lsn_t buffer_size) {
  nbw_impl * impl = malloc(sizeof(nbw_impl));
  pthread_mutex_init(&impl->mut, 0);
  impl->slow_factory = slow_factory;
  impl->slow_factory_arg = slow_factory_arg;
  impl->fast_factory = fast_factory;
  impl->fast_factory_arg = fast_factory_arg;
  impl->fast_handles = RB_ENTRY(init)(cmp_handle, 0);
  impl->slow_handles = 0;
  impl->slow_handle_count = 0;
  impl->workers = 0;
  impl->worker_count = worker_thread_count;
  impl->buffer_size = buffer_size;
  impl->used_buffer_size = 0;
  
  stasis_handle_t * slow = getSlowHandle(impl);

  impl->workers = malloc(impl->worker_count * sizeof(pthread_t));
  
  impl->still_open = 1;
  pthread_cond_init(&impl->pending_writes_cond, 0);
  impl->last_flushed = 0;

  impl->start_pos = slow->start_position(slow);
  impl->end_pos = slow->end_position(slow);
  
  releaseSlowHandle(impl,slow);

  stasis_handle_t * ret = malloc(sizeof(stasis_handle_t));
  *ret = nbw_func;
  ret->impl = impl;

  for(int i = 0; i < impl->worker_count; i++) { 
    pthread_create(&(impl->workers[i]), 0, nbw_worker, ret);
  }

  return ret;
}
