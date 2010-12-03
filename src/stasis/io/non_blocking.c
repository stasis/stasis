
#include <stasis/common.h>

#include <stasis/constants.h>
#include <stasis/io/handle.h>
#include <stasis/linkedlist.h>
#include <stasis/redblack.h>

#include <stdio.h>
#include <assert.h>

/**

  @file

  File handle that avoids blocking on writes.  It attempts to ensure
  that the code calling it never waits for a "slow handle" to perform
  a write.  Instead, when a write request is received, it is
  temporarily stored in a "fast handle".  The caller provides factory
  methods that instantiate fast and slow handles.

  For efficiency, this file handle imposes a special restriction upon
  its callers.  It implicitly partitions the underlying file into
  blocks based upon the read and write requests it receives.  Future
  reads and writes must access complete blocks, and may not span
  multiple blocks.  This works well for page files (where each page is
  a block), and log files, where each log entry is a block, as is the
  header that stasis appends to the log entry.

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

#define INVALID_NODE 3
#define NEEDS_FORCE  2
#define DIRTY        1
#define CLEAN        0
/*
  This should probably be enabled by default.  It causes the writeback
  threads to coalesce write requests before passing them to the
  kernel.
*/
#define MERGE_WRITES

/** @return a read buffer indicating an error has occured */
static inline stasis_read_buffer_t* alloc_read_buffer_error(stasis_handle_t *h,
                                                             int error) {
  assert(error);
  stasis_read_buffer_t * r = malloc(sizeof(stasis_read_buffer_t));
  r->h = h;
  r->buf = 0;
  r->len = 0;
  r->impl = 0;
  r->error = error;
  return r;
}
/** @return a read buffer indicating a write error has occured */
static inline stasis_write_buffer_t* alloc_write_buffer_error
    (stasis_handle_t *h, int error) {
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
  /** set to DIRTY when the handle is written to, NEEDS_FORCE when
      force() is called, and the node is dirty, CLEAN (0) when the
      handle is written back to disk, INVALID_NODE when the handle is
      not in the tree.  */
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
  int (*slow_factory_close)(void * arg);
  void * slow_factory_arg;
  int slow_force_once;

  LinkedList * available_slow_handles;
  int available_slow_handle_count;

  stasis_handle_t ** all_slow_handles;
  int all_slow_handle_count;

  // These two track statistics on write coalescing.
  lsn_t requested_bytes_written;
  lsn_t total_bytes_written;

  // Fields to manage fast handles
  stasis_handle_t * (*fast_factory)(lsn_t off, lsn_t len, void * arg);
  void * fast_factory_arg;

  struct rbtree * fast_handles;
  int fast_handle_count;
  int max_fast_handles;
  int min_fast_handles;
  lsn_t used_buffer_size;
  lsn_t max_buffer_size;

  // Fields to manage and signal worker threads
  pthread_t * workers;
  int worker_count;
  pthread_cond_t force_completed_cond;
  pthread_cond_t pending_writes_cond;
  int still_open;
  int refcount;
} nbw_impl;

static inline void freeFastHandle(nbw_impl * impl, const tree_node * n);

/** Obtain a slow handle from the pool of existing ones, or obtain a new one
    by calling impl.slow_factory. */
static stasis_handle_t * getSlowHandle(nbw_impl * impl) {
  pthread_mutex_lock(&impl->mut);
  stasis_handle_t * slow
    = (stasis_handle_t*)popMaxVal(&impl->available_slow_handles);
  assert(slow);
  if((long)slow == -1) {
    impl->available_slow_handle_count++;
    pthread_mutex_unlock(&impl->mut);
    slow = impl->slow_factory(impl->slow_factory_arg);
    pthread_mutex_lock(&impl->mut);
    impl->all_slow_handle_count++;
    impl->all_slow_handles
      = realloc(impl->all_slow_handles,
                (impl->all_slow_handle_count) * sizeof(stasis_handle_t*));
    impl->all_slow_handles[(impl->all_slow_handle_count)-1] = slow;
    pthread_mutex_unlock(&impl->mut);
  } else {
    pthread_mutex_unlock(&impl->mut);
  }
  return slow;
}
/** Release a file handle back into the pool of slow handles. */
static void releaseSlowHandle(nbw_impl * impl, stasis_handle_t * slow) {
  assert(slow);
  pthread_mutex_lock(&impl->mut);
  addVal(&impl->available_slow_handles, (long)slow);
  pthread_mutex_unlock(&impl->mut);
}

static tree_node * allocTreeNode(lsn_t off, lsn_t len) {
  tree_node * ret = malloc(sizeof(tree_node));
  ret->start_pos = off;
  ret->end_pos = off + len;
  ret->dirty = CLEAN;
  ret->pin_count = 1;
  return ret;
}

static inline const tree_node * allocFastHandle(nbw_impl * impl, lsn_t off,
                                                lsn_t len) {
  tree_node * np = allocTreeNode(off, len);

hack:

  pthread_mutex_lock(&impl->mut);

  DEBUG("allocFastHandle(%lld)\n", off/PAGE_SIZE);

  const tree_node * n = rblookup(RB_LULTEQ, np, impl->fast_handles);
  // this code only works when writes / reads are aligned to immutable
  // boundaries, and never cross boundaries.
  if((!n) ||
     !(n->start_pos <= off &&
       n->end_pos >= off + len)) {

    // no completely overlapping range found; allocate space in np.
    if(impl->fast_handle_count >= impl->max_fast_handles ||
       impl->used_buffer_size + len > impl->max_buffer_size) {

      if(impl->fast_handle_count >= impl->max_fast_handles) {
        DEBUG("Blocking on write.  %d handles (%d max)\n",
               impl->fast_handle_count, impl->max_fast_handles);
      }
      if(impl->used_buffer_size + len > impl->max_buffer_size) {
        DEBUG("Blocking on write.  %lld bytes (%lld max)\n",
               impl->used_buffer_size, impl->max_buffer_size);
      }

      pthread_mutex_unlock(&impl->mut);

      struct timespec ts = { 0, 1000000 /*ns*/ };
      nanosleep(&ts,0);

      goto hack;

//      np->dirty = INVALID_NODE;
//      // @todo should non_blocking fall back on slow handles for backpressure?
//      np->h = getSlowHandle(impl);

//      return np;

    } else {
      impl->fast_handle_count++;
      DEBUG("inc fast handle count %d", impl->fast_handle_count);
      impl->used_buffer_size += len;

      np->h = impl->fast_factory(off,len,impl->fast_factory_arg);
      rbsearch(np, impl->fast_handles);
      n = np;
     }
  } else {
    ((tree_node*)n)->pin_count++;
    free(np);
  }

  pthread_mutex_unlock(&impl->mut);

  return n;
}
static inline const tree_node * findFastHandle(nbw_impl * impl, lsn_t off,
                                               lsn_t len) {
  tree_node * np = allocTreeNode(off, len);

  pthread_mutex_lock(&impl->mut);
  const tree_node * n = rbfind(np, impl->fast_handles);
  if(n) ((tree_node*)n)->pin_count++;
  pthread_mutex_unlock(&impl->mut);

  free(np);
  return n;
}
/** Unlke all of the other fastHandle functions, the caller
    should hold the mutex when calling freeFastHandle. */
static inline void freeFastHandle(nbw_impl * impl, const tree_node * n) {
  assert(impl->fast_handle_count>=0);
  impl->fast_handle_count--;
  DEBUG("dec fast handle count %d", impl->fast_handle_count);
  rbdelete(n, impl->fast_handles);
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
    assert(setDirty == CLEAN || setDirty == DIRTY);
    assert(n->dirty == CLEAN || n->dirty == DIRTY || n->dirty == NEEDS_FORCE);
    pthread_mutex_lock(&impl->mut);
    ((tree_node*)n)->pin_count--;
    if(n->dirty == CLEAN) {
      ((tree_node*)n)->dirty = setDirty;
    }
    pthread_mutex_unlock(&impl->mut);
    if(impl->fast_handle_count > impl->min_fast_handles) {
      pthread_cond_signal(&impl->pending_writes_cond);
    }
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

  (impl->refcount)--;
  if(impl->refcount) {
    pthread_mutex_unlock(&impl->mut);
    return 0;
  }
  impl->still_open = 0;
  pthread_mutex_unlock(&impl->mut);
  pthread_cond_broadcast(&impl->pending_writes_cond);

  for(int i = 0; i < impl->worker_count; i++) {
    pthread_join(impl->workers[i], 0);
  }

  // No longer need latch; this is the only thread allowed to touch the handle.
  free(impl->workers);

  DEBUG("nbw had %d slow handles\n", impl->available_slow_handle_count);
  DEBUG("fast handles = %d, used buffer = %lld\n", impl->fast_handle_count,
        impl->used_buffer_size);
  if(impl->requested_bytes_written < impl->total_bytes_written) {
    printf("nbw: Problem with write coalescing detected.\n"
           "Client wrote %lld bytes, handle wrote %lld.\n",
           impl->requested_bytes_written, impl->total_bytes_written);
  }

  assert(impl->fast_handle_count == 0);
  assert(impl->used_buffer_size == 0);

  rbdestroy(impl->fast_handles);
  pthread_mutex_destroy(&impl->mut);
  stasis_handle_t * slow;
  while(-1 != (long)(slow = (stasis_handle_t*)popMaxVal(&impl->available_slow_handles))) {
    if(!impl->slow_factory_close) slow->close(slow);
    impl->available_slow_handle_count--;
  }
  destroyList(&impl->available_slow_handles);
  free(impl->all_slow_handles);
  assert(impl->available_slow_handle_count == 0);
  int ret = 0;
  if(impl->slow_factory_close) {
    ret = impl->slow_factory_close(impl->slow_factory_arg);
  }

  free(h->impl);
  free(h);
  return ret;
}
static stasis_handle_t * nbw_dup(stasis_handle_t *h) {
  nbw_impl * impl = h->impl;
  (impl->refcount)++;
  return h;
}
static void nbw_enable_sequential_optimizations(stasis_handle_t *h) {
  // TODO non blocking should pass sequential optimizations down to underlying handles.
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
    impl->requested_bytes_written += len;
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
  impl->requested_bytes_written += len;
  pthread_mutex_unlock(&impl->mut);

  return nbw_write_buffer(h, off, len);
}
static int nbw_release_write_buffer(stasis_write_buffer_t * w) {
  nbw_impl * impl = w->h->impl;
  write_buffer_impl * w_impl = w->impl;
  const tree_node * n = w_impl->n;
  w_impl->w->h->release_write_buffer(w_impl->w);
  releaseFastHandle(impl, n, DIRTY);
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

  stasis_read_buffer_t * ret = malloc(sizeof(stasis_read_buffer_t));
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
  // XXX shouldn't need to check for this here; getFastHandle does
  // something similar...
  if(n) {
    releaseFastHandle(impl, n, CLEAN);
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
  releaseFastHandle(impl, n, DIRTY);
  if(!ret) {
    pthread_mutex_lock(&impl->mut);
    assert(impl->start_pos <= impl->end_pos);
    if(off < impl->start_pos) {
      ret = EDOM;
    } else if(off + len > impl->end_pos) {
      impl->end_pos = off+len;
    }
    impl->requested_bytes_written += len;
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
  impl->requested_bytes_written += len;
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
    releaseFastHandle(impl, n, CLEAN);
  } else {
    stasis_handle_t * slow = getSlowHandle(impl);
    ret = slow->read(slow, off, buf, len);
    releaseSlowHandle(impl, slow);
  }
  return ret;
}
static int nbw_force_range_impl(stasis_handle_t * h, lsn_t start, lsn_t stop) {
  nbw_impl * impl = h->impl;
  //  pthread_mutex_lock(&impl->mut);
  tree_node scratch;
  scratch.start_pos = start;
  scratch.end_pos = start+1;
  if(!stop) stop =  impl->end_pos;
  const tree_node * n = rblookup(RB_LUGTEQ,&scratch,impl->fast_handles);   // min)(impl->fast_handles);
  int blocked = 0;
  while(n) {
    if(n->start_pos >= stop) { break; }
    if(n->dirty) {
      // cast strips const
      ((tree_node*)n)->dirty = NEEDS_FORCE;
      blocked = 1;
    }
    n = rblookup(RB_LUNEXT,n,impl->fast_handles);
  }
  pthread_cond_broadcast(&impl->pending_writes_cond);
  while(blocked) {
    pthread_cond_wait(&impl->force_completed_cond,&impl->mut);
    blocked = 0;
    n = rbmin(impl->fast_handles);
    while(n) {
      if(n->dirty == NEEDS_FORCE) {
        blocked = 1;
      }
      n = rblookup(RB_LUNEXT,n,impl->fast_handles);
    }
  }
  int ret = 0;
  if(impl->slow_force_once) {
    if(impl->all_slow_handle_count) {
      stasis_handle_t * h = impl->all_slow_handles[0];
      ret = h->force_range(h, start, stop);
    }
  } else {
    for(int i = 0; i < impl->all_slow_handle_count; i++) {
      stasis_handle_t * h = impl->all_slow_handles[i];
      int tmpret = h->force_range(h, start, stop);
      if(tmpret) { ret = tmpret; }
    }
  }
  //  pthread_mutex_unlock(&impl->mut);
  return ret;
}
static int nbw_force(stasis_handle_t * h) {
  nbw_impl * impl = h->impl;
  pthread_mutex_lock(&impl->mut);
  int ret = nbw_force_range_impl(h, impl->start_pos, impl->end_pos);
  pthread_mutex_unlock(&impl->mut);
  return ret;
}
static int nbw_force_range(stasis_handle_t * h,
                           lsn_t start,
                           lsn_t stop) {
  nbw_impl * impl = h->impl;
  pthread_mutex_lock(&impl->mut);
  int ret = nbw_force_range_impl(h, start, stop);
  pthread_mutex_unlock(&impl->mut);
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
    // @todo close all slow handles; truncate them. (ie: implement truncate)
  }
  return error;
}

struct stasis_handle_t nbw_func = {
  .num_copies = nbw_num_copies,
  .num_copies_buffer = nbw_num_copies_buffer,
  .close = nbw_close,
  .dup = nbw_dup,
  .enable_sequential_optimizations = nbw_enable_sequential_optimizations,
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
  .force = nbw_force,
  .force_range = nbw_force_range,
  .truncate_start = nbw_truncate_start,
  .error = 0
};

/**
   This worker thread simulates asynchrnous I/O by handling writeback
   on behalf of the application.  Multiple workers may be spawned for
   a non-blocking handle.

   This function walks the list of fast handles, writing back dirty
   ones, and freeing clean ones.  It (almost) never performs a write
   while holding the mutex.

   @todo Non-blocking handle should not memcpy() buffers while holding
         the mutex.

*/
static void * nbw_worker(void * handle) {
  stasis_handle_t * h = handle;
  nbw_impl * impl = h->impl;

  stasis_handle_t * slow = getSlowHandle(impl);

  pthread_mutex_lock(&impl->mut);
  while(1) {
    // cast strips const.
    tree_node * node = (tree_node*)rbmin(impl->fast_handles);
    int writes = 0;
    int contributed_to_force = 0;
    while(node) {
      tree_node next_node = *node;
      if(node->dirty && !node->pin_count) {
        if(node->dirty == NEEDS_FORCE) {
          contributed_to_force = 1;
        }
        assert(node->dirty != INVALID_NODE);
        node->dirty = CLEAN;
        node->pin_count++;
        writes++;

        stasis_handle_t * fast = node->h;
        lsn_t off = fast->start_position(fast);
        lsn_t len = fast->end_position(fast) - off;
        stasis_read_buffer_t * r = fast->read_buffer(fast, off, len);

#ifdef MERGE_WRITES
        // cast strips const
        byte *buf = (byte*)r->buf;

        int first = 1;
        off_t buf_off = 0;
        tree_node dummy;
        dummy.start_pos = node->end_pos;
        dummy.end_pos   = node->end_pos+1;
        tree_node * np;
        tree_node * dummies = 0;
        int dummy_count = 0;
        while((np = (tree_node*)rbfind(&dummy, impl->fast_handles))
              && np->dirty && !np->pin_count) {
          lsn_t np_len = np->end_pos - np->start_pos;
          len += np_len;

          if(first) {
            buf = malloc(r->len + len);
            memcpy(buf, r->buf, r->len);
            buf_off += r->len;

            dummies = malloc(sizeof(tree_node));
            dummies[0] = dummy;
            dummy_count = 1;
            first = 0;
          } else {
            buf = realloc(buf, len);

            dummies = realloc(dummies, sizeof(tree_node) * (dummy_count+1));
            dummies[dummy_count] = dummy;
            dummy_count++;
          }

          stasis_handle_t * fast2 = np->h;
          stasis_read_buffer_t * r2 = fast2->read_buffer(fast2,np->start_pos,
                                                         np_len);
          memcpy(buf + buf_off, r2->buf, np_len);
          buf_off += np_len;
          r2->h->release_read_buffer(r2);
          if(np->dirty == NEEDS_FORCE) contributed_to_force = 1;
          assert(np->dirty != INVALID_NODE);
          np->dirty = CLEAN;
          np->pin_count++;
          dummy.start_pos = np->end_pos;
          dummy.end_pos   = np->end_pos+1;
        }

        impl->total_bytes_written += len;

        pthread_mutex_unlock(&impl->mut);

        if(len != PAGE_SIZE) {
          DEBUG("merged %lld pages at %lld into single write\n",
                len/PAGE_SIZE, off/PAGE_SIZE);
        }
        slow->write(slow, off, buf, len);

        if(!first) {
          free(buf);
        }
#else
        pthread_mutex_unlock(&impl->mut);
        slow->write(slow, off, r->buf, len);
#endif
	r->h->release_read_buffer(r);
        pthread_mutex_lock(&impl->mut);

#ifdef MERGE_WRITES
        for(int i = 0; i < dummy_count; i++) {
          np = (tree_node*)rbfind(&dummies[i], impl->fast_handles);
          assert(np);
          assert(np->pin_count);
          assert(np->dirty != INVALID_NODE);

          np->pin_count--;
          if(!np->dirty && !np->pin_count) {
            impl->used_buffer_size -= (np->end_pos - np->start_pos);
            freeFastHandle(impl, np);
          }
        }
        if(dummies) {
          next_node = dummies[dummy_count-1];
          free(dummies);
        }
#endif
        assert(node->pin_count);
        node->pin_count--;
      }

      if(impl->used_buffer_size < (1 * impl->max_buffer_size) / 5) {
        pthread_mutex_unlock(&impl->mut);
        struct timespec ts = {0, 1000000};
        nanosleep(&ts, 0);
        pthread_mutex_lock(&impl->mut);
      }

      tree_node *new_node = (tree_node*)rblookup(RB_LUGREAT, &next_node,
                                                         impl->fast_handles);
      if(!node->dirty && !node->pin_count) {
        impl->used_buffer_size -= (node->end_pos - node->start_pos);
        freeFastHandle(impl, node);
      }
      node = new_node;
    }
    if(contributed_to_force) {
      pthread_cond_broadcast(&impl->force_completed_cond);
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

  releaseSlowHandle(impl, slow);

  return 0;
}

stasis_handle_t * stasis_handle(open_non_blocking)
     (stasis_handle_t * (*slow_factory)(void * arg),
     int (*slow_factory_close)(void * arg),
     void * slow_factory_arg,
     int slow_force_once,
     stasis_handle_t * (*fast_factory)(lsn_t, lsn_t, void *),
     void * fast_factory_arg, int worker_thread_count, lsn_t buffer_size,
     int max_fast_handles) {
  nbw_impl * impl = malloc(sizeof(nbw_impl));
  pthread_mutex_init(&impl->mut, 0);

  impl->start_pos = 0;
  impl->end_pos = 0;

  impl->slow_factory = slow_factory;
  impl->slow_factory_close = slow_factory_close;
  impl->slow_factory_arg = slow_factory_arg;

  impl->slow_force_once = slow_force_once;

  impl->available_slow_handles = 0;
  impl->available_slow_handle_count = 0;
  impl->all_slow_handles = malloc(sizeof(stasis_handle_t*));
  impl->all_slow_handle_count = 0;

  impl->requested_bytes_written = 0;
  impl->total_bytes_written = 0;

  impl->fast_factory = fast_factory;
  impl->fast_factory_arg = fast_factory_arg;

  impl->fast_handles = rbinit(cmp_handle, 0);
  impl->fast_handle_count = 0;
  impl->max_fast_handles = max_fast_handles;
  impl->min_fast_handles = max_fast_handles / 2;
  impl->max_buffer_size = buffer_size;
  impl->used_buffer_size = 0;

  impl->workers = malloc(worker_thread_count * sizeof(pthread_t));
  impl->worker_count = worker_thread_count;

  pthread_cond_init(&impl->pending_writes_cond, 0);
  pthread_cond_init(&impl->force_completed_cond, 0);

  impl->still_open = 1;
  impl->refcount = 1;

  stasis_handle_t *h = malloc(sizeof(stasis_handle_t));
  *h = nbw_func;
  h->impl = impl;

  for(int i = 0; i < impl->worker_count; i++) {
    int err = pthread_create(&(impl->workers[i]), 0, nbw_worker, h);
    if(err) {
      perror("Coudln't spawn worker thread for non_blocking io");
    }
  }

  DEBUG("Opened non blocking I/O handle; buffer size = %lldmb max outstanding writes = %d\n",
        impl->max_buffer_size / (1024 * 1024), impl->max_fast_handles);

  return h;
}


#if 0 // XXX need to move this into a factory for slow handle
// @todo this factory stuff doesn't really belong here...
static stasis_handle_t * fast_factory(lsn_t off, lsn_t len, void * ignored) {
  stasis_handle_t * h = stasis_handle(open_memory)(off);
  //h = stasis_handle(open_debug)(h);
  stasis_write_buffer_t * w = h->append_buffer(h, len);
  w->h->release_write_buffer(w);
  return h;
}
typedef struct sf_args {
  const char * filename;
  int    openMode;
  int    filePerm;
} sf_args;
static stasis_handle_t * slow_file_factory(void * argsP) {
  sf_args * args = (sf_args*) argsP;
  stasis_handle_t * h =  stasis_handle(open_file)(0, args->filename, args->openMode, args->filePerm);
  //h = stasis_handle(open_debug)(h);
  return h;
}
static stasis_handle_t * slow_pfile_factory(void * argsP) {
  stasis_handle_t * h = argsP;
  return h;
}
static int nop_close(stasis_handle_t*h) { return 0; }
    struct sf_args * slow_arg = malloc(sizeof(sf_args));
        slow_arg->filename = path;

        slow_arg->openMode = openMode;

        slow_arg->filePerm = FILE_PERM;
        // Allow 4MB of outstanding writes.
        // @todo Where / how should we open storefile?
        int worker_thread_count = 1;
        if(bufferManagerNonBlockingSlowHandleType == IO_HANDLE_PFILE) {
          //              printf("\nusing pread()/pwrite()\n");
          stasis_handle_t * slow_pfile = stasis_handle_open_pfile(0, slow_arg->filename, slow_arg->openMode, slow_arg->filePerm);
          int (*slow_close)(struct stasis_handle_t *) = slow_pfile->close;
          slow_pfile->close = nop_close;
          ret =
              stasis_handle(open_non_blocking)(slow_pfile_factory, (int(*)(void*))slow_close, slow_pfile, 1, fast_factory,
                         NULL, worker_thread_count, PAGE_SIZE * 1024 , 1024);

        } else if(bufferManagerNonBlockingSlowHandleType == IO_HANDLE_FILE) {
          ret =
              stasis_handle(open_non_blocking)(slow_file_factory, 0, slow_arg, 0, fast_factory,
                         NULL, worker_thread_count, PAGE_SIZE * 1024, 1024);
        } else {
          printf("Unknown value for config option bufferManagerNonBlockingSlowHandleType\n");
          abort();
        }
      } break;
#endif

