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

  Latching protocol: 

    Each static function that manipulates the tree or lists grabs a
    latch.  Functions that call such functions should not hold a latch
    when the function is called.  If a function must atomically update
    the handle's state (eg: append), they should oobtain the latch,
    and release it before calling another function or returning.

    Exception: freeFastHandle should be called while holding the
               latch.

*/

#define INVALID_NODE 2

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
/** Wraps stasis_handle_t so that it can be stored in an rbtree. */
typedef struct tree_node { 
  lsn_t start_pos;
  lsn_t end_pos;
  stasis_handle_t * h;
  /** The number of threads accessing this handle.  The handle cannot
      be deallocated unless this is zero. */
  int pin_count;
  /** set to 1 when the handle is written to, 0 when the handle is
      written back to disk, INVALID_NODE when the handle is not in 
      the tree.  */
  int dirty;      
} tree_node;

/** Wrapper for write buffers */
typedef struct write_buffer_impl {  
  /** The tree node that contains this buffer */
  const tree_node * n;
  /** The underlying buffer. */
  stasis_write_buffer_t * w;
} write_buffer_impl;
typedef struct read_buffer_impl { 
  /** The tree node that contains this buffer, or NULL if the buffer 
      is from a slow handle. */
  const tree_node * n;
  /** The underlying buffer. */
  stasis_read_buffer_t * r;
} read_buffer_impl;

/** 
    Compare two tree_node structs.  Two tree nodes are equal if they
    are zero length, and start at the same point, or if they overlap.
 */
static int cmp_handle(const void * ap, const void * bp, const void * ignored) {
  tree_node * a = (tree_node*)ap;
  tree_node * b = (tree_node*)bp;
  if(a->start_pos == b->start_pos && 
     a->start_pos == a->end_pos &&
     b->start_pos == b->end_pos ) { 
    return 0;
  }
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

  // Handle state
  lsn_t start_pos;
  lsn_t end_pos; 

  // Fields to manage slow handles
  stasis_handle_t * (*slow_factory)(void * arg);
  void * slow_factory_arg;

  LinkedList * slow_handles;
  int slow_handle_count;
  
  // Fields to manage fast handles
  stasis_handle_t * (*fast_factory)(lsn_t off, lsn_t len, void * arg);
  void * fast_factory_arg;

  struct RB_ENTRY(tree) * fast_handles;
  int fast_handle_count;
  int max_fast_handles;
  lsn_t used_buffer_size;
  lsn_t max_buffer_size;

  // Fields to manage and signal worker threads
  pthread_t * workers;
  int worker_count;
  pthread_cond_t pending_writes_cond;
  int still_open; 
} nbw_impl;

static stasis_handle_t * getSlowHandle(nbw_impl * impl) { 
  pthread_mutex_lock(&impl->mut);
  stasis_handle_t * slow = (stasis_handle_t*)popMaxVal(&impl->slow_handles);
  assert(slow);
  if((long)slow == -1) {
    impl->slow_handle_count++;
    pthread_mutex_unlock(&impl->mut);
    slow = impl->slow_factory(impl->slow_factory_arg);
  } else { 
    pthread_mutex_unlock(&impl->mut);
  }
  return slow;
}
static void releaseSlowHandle(nbw_impl * impl, stasis_handle_t * slow) { 
  assert(slow);
  pthread_mutex_lock(&impl->mut);
  addVal(&impl->slow_handles, (long)slow);
  pthread_mutex_unlock(&impl->mut);
}

static tree_node * allocTreeNode(lsn_t off, lsn_t len) { 
  tree_node * ret = malloc(sizeof(tree_node));
  ret->start_pos = off;
  ret->end_pos = off + len;
  ret->dirty = 0;
  ret->pin_count = 1;
  return ret;
}

static inline const tree_node * allocFastHandle(nbw_impl * impl, lsn_t off, lsn_t len) {
  tree_node * np = allocTreeNode(off, len);
  pthread_mutex_lock(&impl->mut);
  const tree_node * n = RB_ENTRY(search)(np, impl->fast_handles);
  if(n == np) { // not found 
    if(impl->fast_handle_count > impl->max_fast_handles || 
       impl->used_buffer_size > impl->max_buffer_size) { 
      RB_ENTRY(delete)(np, impl->fast_handles);
      np->dirty = INVALID_NODE;
      pthread_mutex_unlock(&impl->mut);

      np->h = getSlowHandle(impl);
    } else { 
      impl->fast_handle_count++;
      impl->used_buffer_size += len;
      pthread_mutex_unlock(&impl->mut);

      np->h = impl->fast_factory(off,len,impl->fast_factory_arg);
    }
  } else {
    ((tree_node*)n)->pin_count++;
    pthread_mutex_unlock(&impl->mut);

    free(np);
  }
  return n;
}
static inline const tree_node * findFastHandle(nbw_impl * impl, lsn_t off, lsn_t len) { 
  tree_node * np = allocTreeNode(off, len);

  pthread_mutex_lock(&impl->mut);
  const tree_node * n = RB_ENTRY(find)(np, impl->fast_handles);
  if(n) ((tree_node*)n)->pin_count++;
  pthread_mutex_unlock(&impl->mut);

  free(np);
  return n;
}
/** Unlke all of the other fastHandle functions, the caller 
    should hold the mutex when calling freeFastHandle. */
static inline void freeFastHandle(nbw_impl * impl, const tree_node * n) { 
  RB_ENTRY(delete)(n, impl->fast_handles);
  impl->fast_handle_count--;
  impl->used_buffer_size -= (n->end_pos - n->start_pos);
  n->h->close(n->h);
  free((void*)n);
}
static inline int releaseFastHandle(nbw_impl * impl, const tree_node * n, 
				    int setDirty) {
  if(n->dirty == INVALID_NODE) { 
    // Not in tree; cast removes "const"
    releaseSlowHandle(impl, n->h);
    free((void*)n);
    return 0;
  } else { 
    assert(setDirty == 0 || setDirty == 1);
    assert(n->dirty == 0 || n->dirty == 1);
    pthread_mutex_lock(&impl->mut);
    ((tree_node*)n)->pin_count--;
    if(n->dirty == 0) { 
      ((tree_node*)n)->dirty = setDirty;
    }
    pthread_mutex_unlock(&impl->mut);
    pthread_cond_signal(&impl->pending_writes_cond);
    return 0;
  }
}
/** @todo nbw_num_copies is unimplemented. */
static int nbw_num_copies(stasis_handle_t * h) { 
  return 0;
}
/** @todo nbw_num_copies_buffer is unimplemented. */
static int nbw_num_copies_buffer(stasis_handle_t * h) { 
  return 0;
}
static int nbw_close(stasis_handle_t * h) { 
  nbw_impl * impl = h->impl;
  
  pthread_mutex_lock(&impl->mut);
  impl->still_open = 0;
  pthread_mutex_unlock(&impl->mut);
  pthread_cond_broadcast(&impl->pending_writes_cond);

  for(int i = 0; i < impl->worker_count; i++) { 
    pthread_join(impl->workers[i], 0);
  }
  
  // No longer need latch; this is the only thread allowed to touch the handle.

  free(impl->workers);

  // printf("nbw had %d slow handles\n", impl->slow_handle_count); 
  // fflush(stdout);

  assert(impl->fast_handle_count == 0);
  assert(impl->used_buffer_size == 0);

  RB_ENTRY(destroy)(impl->fast_handles);
  pthread_mutex_destroy(&impl->mut);
  stasis_handle_t * slow;
  while(-1 != (long)(slow = (stasis_handle_t*)popMaxVal(&impl->slow_handles))) {
    slow->close(slow);
    impl->slow_handle_count--;
  }
  destroyList(&impl->slow_handles);

  assert(impl->slow_handle_count == 0);
  
  free(h->impl);
  free(h);
  return 0;
}
static lsn_t nbw_start_position(stasis_handle_t *h) { 
  nbw_impl * impl = h->impl;
  pthread_mutex_lock(&impl->mut);
  lsn_t ret = impl->start_pos;
  pthread_mutex_unlock(&impl->mut);
  return ret;
}
static lsn_t nbw_end_position(stasis_handle_t *h) { 
  nbw_impl * impl = h->impl;
  pthread_mutex_lock(&impl->mut);
  lsn_t ret = impl->end_pos;
  pthread_mutex_unlock(&impl->mut);
  return ret;
}
static stasis_write_buffer_t * nbw_write_buffer(stasis_handle_t * h, 
						lsn_t off, lsn_t len) {
  nbw_impl * impl = h->impl;
  const tree_node * n = allocFastHandle(impl, off, len);
  stasis_write_buffer_t * w = n->h->write_buffer(n->h, off, len);

  write_buffer_impl * w_impl = malloc(sizeof(write_buffer_impl));
  w_impl->n = n; 
  w_impl->w = w;

  stasis_write_buffer_t * ret = malloc(sizeof(stasis_write_buffer_t));
  ret->h     = h; 
  ret->off   = w->off; 
  ret->len   = w->len; 
  ret->buf   = w->buf; 
  ret->error = w->error;
  ret->impl  = w_impl; 
  
  if(!ret->error) { 
    pthread_mutex_lock(&impl->mut);
    assert(impl->start_pos <= impl->end_pos);
    if(off < impl->start_pos) { 
      // Note: We're returning a valid write buffer to space before
      // the handle's truncation point.  Spooky.
      ret->error = EDOM;
    } else if(off + len > impl->end_pos) {
      impl->end_pos = off+len;
    }
    pthread_mutex_unlock(&impl->mut);
  }

  return ret;
}
static stasis_write_buffer_t * nbw_append_buffer(stasis_handle_t * h, 
						   lsn_t len) { 
  nbw_impl * impl = h->impl;

  pthread_mutex_lock(&impl->mut);
  lsn_t off = impl->end_pos;
  impl->end_pos += len;
  pthread_mutex_unlock(&impl->mut);

  return nbw_write_buffer(h, off, len);
}
static int nbw_release_write_buffer(stasis_write_buffer_t * w) { 
  nbw_impl * impl = w->h->impl;
  write_buffer_impl * w_impl = w->impl;
  const tree_node * n = w_impl->n;
  w_impl->w->h->release_write_buffer(w_impl->w);
  releaseFastHandle(impl, n, 1);
  //  pthread_cond_signal(&impl->pending_writes_cond);
  free(w_impl);
  free(w);
  return 0;
}
static stasis_read_buffer_t * nbw_read_buffer(stasis_handle_t * h,
					      lsn_t off, lsn_t len) { 
  nbw_impl * impl = h->impl;
  const tree_node * n = findFastHandle(impl, off, len);
  stasis_read_buffer_t * r;
  stasis_handle_t * r_h = n ? n->h : getSlowHandle(impl);
  r = r_h->read_buffer(r_h, off, len);

  read_buffer_impl * r_impl = malloc(sizeof(read_buffer_impl));
  r_impl->n = n;
  r_impl->r = r;
				     
  stasis_read_buffer_t * ret = malloc(sizeof(stasis_write_buffer_t));
  ret->h = h;
  ret->off = r->off;
  ret->len = r->len;
  ret->buf = r->buf;
  ret->error = r->error;
  ret->impl = r_impl;

  return ret;
}
static int nbw_release_read_buffer(stasis_read_buffer_t * r) { 
  nbw_impl * impl = r->h->impl;
  read_buffer_impl * r_impl = r->impl;
  const tree_node * n = r_impl->n;
  stasis_handle_t * oldHandle = r_impl->r->h;
  r_impl->r->h->release_read_buffer(r_impl->r);
  // XXX shouldn't need to check for this here; getFastHandle does something similar...
  if(n) { 
    releaseFastHandle(impl, n, 0);
  } else { 
    assert(oldHandle);
    releaseSlowHandle(impl, oldHandle);
  }
  free(r_impl);
  free(r);
  return 0;
}
static int nbw_write(stasis_handle_t * h, lsn_t off, 
		     const byte * dat, lsn_t len) { 
  nbw_impl * impl = h->impl;
  const tree_node * n = allocFastHandle(impl, off, len);
  int ret = n->h->write(n->h, off, dat, len);
  releaseFastHandle(impl, n, 1);
  if(!ret) { 
    pthread_mutex_lock(&impl->mut);
    assert(impl->start_pos <= impl->end_pos);
    if(off < impl->start_pos) { 
      ret = EDOM;
    } else if(off + len > impl->end_pos) {
      impl->end_pos = off+len;
    }
    pthread_mutex_unlock(&impl->mut);
  }
  return ret;
}
static int nbw_append(stasis_handle_t * h, lsn_t * off, 
		      const byte * dat, lsn_t len) { 
  nbw_impl * impl = h->impl;

  pthread_mutex_lock(&impl->mut);
  *off = impl->end_pos;
  impl->end_pos+= len;
  pthread_mutex_unlock(&impl->mut);

  int ret = nbw_write(h, *off, dat, len);
  return ret;
}
static int nbw_read(stasis_handle_t * h, 
		    lsn_t off, byte * buf, lsn_t len) { 
  nbw_impl * impl = h->impl;
  const tree_node * n = findFastHandle(impl, off, len);
  int ret;
  // XXX should be handled by releaseFastHandle.
  if(n) { 
    ret = n->h->read(n->h, off, buf, len);
    releaseFastHandle(impl, n, 0);
  } else { 
    stasis_handle_t * slow = getSlowHandle(impl);
    ret = slow->read(slow, off, buf, len);
    releaseSlowHandle(impl, slow);
  }
  return ret;
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
  if(!error) { 
    // XXX close all slow handles; truncate of them. (ie: implement truncate)
  }
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
  stasis_handle_t * h = handle;
  nbw_impl * impl = h->impl;

  pthread_mutex_lock(&impl->mut);
  while(1) { 
    // cast strips const.
    tree_node * node = (tree_node*)RB_ENTRY(min)(impl->fast_handles);
    int writes = 0;
    while(node) { 
      if(node->dirty && !node->pin_count) { 
	node->dirty = 0;
	node->pin_count++;
	pthread_mutex_unlock(&impl->mut);
	writes++;
	stasis_handle_t * slow = getSlowHandle(impl);
	stasis_handle_t * fast = node->h;
	lsn_t off = fast->start_position(fast);
	lsn_t len = fast->end_position(fast) - off;
	stasis_read_buffer_t * r = fast->read_buffer(fast, off, len);
	slow->write(slow, off, r->buf, len);
	r->h->release_read_buffer(r);
	releaseSlowHandle(impl, slow);
	pthread_mutex_lock(&impl->mut);
	node->pin_count--;
      }      
      tree_node * new_node = (tree_node*)RB_ENTRY(lookup)(RB_LUGREAT, node, impl->fast_handles);
      if(!node->dirty && !node->pin_count) { 
	freeFastHandle(impl, node);
      }
      node = new_node;
    }
    if(!impl->fast_handle_count || !writes) { 
      if(impl->still_open) {
	pthread_cond_wait(&impl->pending_writes_cond, &impl->mut);
      } else {
	break;
      }
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
						   lsn_t buffer_size, int max_fast_handles) {
  nbw_impl * impl = malloc(sizeof(nbw_impl));
  pthread_mutex_init(&impl->mut, 0);

  impl->start_pos = 0;
  impl->end_pos = 0;

  impl->slow_factory = slow_factory;
  impl->slow_factory_arg = slow_factory_arg;

  impl->slow_handles = 0;
  impl->slow_handle_count = 0;

  impl->fast_factory = fast_factory;
  impl->fast_factory_arg = fast_factory_arg;

  impl->fast_handles = RB_ENTRY(init)(cmp_handle, 0);
  impl->fast_handle_count = 0;
  impl->max_fast_handles = max_fast_handles;

  impl->max_buffer_size = buffer_size;
  impl->used_buffer_size = 0;

  impl->workers = malloc(worker_thread_count * sizeof(pthread_t));
  impl->worker_count = worker_thread_count;

  pthread_cond_init(&impl->pending_writes_cond, 0);
  
  impl->still_open = 1;

  stasis_handle_t *h = malloc(sizeof(stasis_handle_t));
  *h = nbw_func;
  h->impl = impl;

  for(int i = 0; i < impl->worker_count; i++) { 
    int err = pthread_create(&(impl->workers[i]), 0, nbw_worker, h);
    if(err) { 
      perror("Coudln't spawn worker thread for non_blocking io");
    }
  }
  return h;
}
