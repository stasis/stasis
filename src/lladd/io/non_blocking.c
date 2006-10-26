#include <lladd/io/handle.h>
#include <lladd/redblack.h>
#include <pthread.h>
#include <errno.h>
#include <stdlib.h>
#include <assert.h>
#include <stdio.h>

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


typedef struct nbw_thread {
  pthread_t thread;
  // Other stuff?
} nbw_thread;

typedef struct tree_node { 
  lsn_t start_pos;
  lsn_t end_pos;
  stasis_handle_t * h;
} tree_node;

static int cmp_handle(const void * ap, const void * bp, const void * ignored) {
  tree_node * a = (tree_node*)ap;
  tree_node * b = (tree_node*)bp;

  // @todo It would probably be faster to malloc a wrapper and cache
  // these values, instead of invoking 4 virtual methods each time we visit
  // a tree node.

  if(a->end_pos <= b->start_pos) { 
    return -1;
  } else if(a->start_pos >= b->end_pos) {
    return 1;
  } else { 
    return 0;
  }

}

typedef struct nbw_impl {
  pthread_mutex_t mut;
  stasis_handle_t * slow;
  stasis_handle_t * (*fast_factory)(lsn_t off, lsn_t len, void * arg);
  void * fast_factory_arg;
  struct RB_ENTRY(tree) * fast_handles;
  nbw_thread * workers;
  int worker_count;

  // The rest of the values are caches maintained for efficiency.

  /*  int num_copies;
      int num_copies_buffer; */
  lsn_t start_pos;
  lsn_t end_pos; 
} nbw_impl;

static inline stasis_handle_t * getFastHandle(nbw_impl * impl, lsn_t off, lsn_t len, int allocIfMissing) { 
  tree_node * np = malloc(sizeof(tree_node));
  np->start_pos = off;
  np->end_pos = off+len;
  if(allocIfMissing) { 
    const tree_node * n = RB_ENTRY(search)(np, impl->fast_handles);
    if(n != np) {
      // The node was already in the tree
      assert(np->start_pos == n->start_pos && np->end_pos == n->end_pos);
      free(np);
    } else {
      // The node wasn't in the tree and search inserted it for us.
      // Assignment is safe because we're writing to a field of n that is ignored by cmp.
      ((tree_node*)n)->h = impl->fast_factory(off, len, impl->fast_factory_arg);
    }
    return n->h;
  } else { 
    const tree_node * n = RB_ENTRY(find)(np, impl->fast_handles);
    free(np);
    if(!n) { 
      return 0;
    } else { 
      return n->h;
    }
  }
}

static int nbw_num_copies(stasis_handle_t * h) { 
  nbw_impl * impl = (nbw_impl*) h->impl;
  int slow_num_copies = impl->slow->num_copies(impl->slow);
  stasis_handle_t * fast = impl->fast_factory(0, 0, impl->fast_factory_arg);
  int fast_num_copies = fast->num_copies(fast);
  fast->close(fast);
  return slow_num_copies > fast_num_copies ? slow_num_copies : fast_num_copies;
}
static int nbw_num_copies_buffer(stasis_handle_t * h) { 
  nbw_impl * impl = (nbw_impl*) h->impl;
  int slow_num_copies_buffer = impl->slow->num_copies_buffer(impl->slow);
  stasis_handle_t * fast = impl->fast_factory(0, 0, impl->fast_factory_arg);
  int fast_num_copies_buffer = fast->num_copies_buffer(fast);
  fast->close(fast);
  return slow_num_copies_buffer > fast_num_copies_buffer ? slow_num_copies_buffer : fast_num_copies_buffer;
}
static int nbw_close(stasis_handle_t * h) { 
  printf("Warning: nbw_close leaks fast handles, and never flushes them to the slow handle...\n");
  nbw_impl * impl = h->impl;
  impl->slow->close(impl->slow);
  // foreach fast handle .. close fast handle...
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
    stasis_handle_t * h = getFastHandle(impl, off, len, 1);
    w = h->write_buffer(h, off, len);
  }
  pthread_mutex_unlock(&impl->mut);
  
  if(!w) { 
    w = alloc_write_buffer_error(h, error);
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
  return w;
}
static int nbw_release_write_buffer(stasis_write_buffer_t * w) { 
  if(w->error) { 
    free(w);
  } else {
    abort();
  }
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
    } else { 
      r = impl->slow->read_buffer(impl->slow, off, len);
    }
  }
  if(!r) { 
    r = alloc_read_buffer_error(h, error);
  }
  pthread_mutex_unlock(&impl->mut);
  return r;
}
static int nbw_release_read_buffer(stasis_read_buffer_t * r) { 
  if(r->error) { 
    free(r);
  } else { 
    abort();
  }
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
    } else  { 
      //      printf("slow"); fflush(stdout);
      error = impl->slow->read(impl->slow, off, buf, len);
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


stasis_handle_t * stasis_handle(open_non_blocking)(stasis_handle_t * slow, 
						   stasis_handle_t * (*fast_factory)(lsn_t, lsn_t, void *),
						   void * fast_factory_arg) {
  nbw_impl * impl = malloc(sizeof(nbw_impl));
  pthread_mutex_init(&impl->mut, 0);
  impl->slow = slow;
  impl->fast_factory = fast_factory;
  impl->fast_factory_arg = fast_factory_arg;
  impl->fast_handles = RB_ENTRY(init)(cmp_handle, 0);
  impl->workers = 0;
  impl->worker_count = 0;
  impl->start_pos = slow->start_position(slow);
  impl->end_pos = slow->end_position(slow);

  stasis_handle_t * ret = malloc(sizeof(stasis_handle_t));
  *ret = nbw_func;
  ret->impl = impl;
  return ret;
}
