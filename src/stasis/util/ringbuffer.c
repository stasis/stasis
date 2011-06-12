#include <stasis/util/ringbuffer.h>
#include <stasis/util/min.h>

#include <assert.h>
#include <sys/mman.h>
#include <stdio.h>
/**
 *  A ring buffer implementation.
 *
 *  This file implements a ring buffer.  Reads and writes are to
 *  contiguous regions, and are zero-copy.  Regions may be populated
 *  in parallel, and data can be randomly accessed by its offset in
 *  the buffer's logical address space (which is returned by various API calls).
 *  Any number of readers and writers may safely read and write data from the
 *  ringbuffer without further coordination.
 *
 *  The size of the ring buffer must be a power of two (this avoids
 *  expensive mod operations).
 *
 *  TODO: Currently, in order to support multiple readers and writers, this file
 *        relies upon red-black trees, which adds a lot of overhead.
 */

struct stasis_ringbuffer_t {
  byte * mem;
  lsn_t mask;
  // Track four regions:  write_frontier (wf), write_tail (wt), read_frontier (rf), read_tail (rt):

  // Logical buffer layout:
  // byte zero
  // ...

  lsn_t rt; // First byte that some thread might be reading.  Earlier bytes can be reclaimed.
  lsn_t rf; // First byte that will be returned by "read next".
  lsn_t wt; // First byte that some thread might be writing.  Earlier bytes are stable for readers.
  lsn_t wf; // First available byte

  // ...
  // byte 2^64

  stasis_aggregate_min_t * min_writer;
  stasis_aggregate_min_t * min_reader;

  // Synchronization stuff
  pthread_mutex_t mut;
  pthread_cond_t read_done;
  pthread_cond_t write_done;

  int fd;

  // If non-zero, all read requests act as though size is
  // RING_NEXT until the read frontier is greater than flush.
  lsn_t flush;
  // Once this is non-zero, no read will ever block.  Attempts to
  // write data after shutdown is set will have undefined semantics.
  int shutdown;
};

// Does not need synchronization (only called from nb function).
static inline lsn_t freespace(stasis_ringbuffer_t * ring) {
  lsn_t ret =  ((ring->rt - ring->wf) - 1) & ring->mask;
//  printf("freespace is %lld\n", (long long)ret);
  return ret;
}

// Does not need any synchronization (all fields are read only)
static inline void* ptr_off(stasis_ringbuffer_t * ring, lsn_t off) {
  return ring->mem + (off & ring->mask);
}

// Not threadsafe.
lsn_t stasis_ringbuffer_nb_reserve_space(stasis_ringbuffer_t * ring, lsn_t sz) {
  if(freespace(ring) < sz) { return RING_FULL; }
  lsn_t ret = ring->wf;
  ring->wf += sz;
  return ret;
}
// Threadsafe (explicit synchronization).  Blocks.
lsn_t stasis_ringbuffer_reserve_space(stasis_ringbuffer_t * ring, lsn_t sz, lsn_t * handle) {
  pthread_mutex_lock(&ring->mut);
  lsn_t ret;
  while(RING_FULL == (ret = stasis_ringbuffer_nb_reserve_space(ring, sz))) {
    pthread_cond_wait(&ring->read_done, &ring->mut);
  }
  if(handle) {
    *handle = ret;
    stasis_aggregate_min_add(ring->min_writer, handle);
  }
  pthread_mutex_unlock(&ring->mut);
  return ret;
}
lsn_t stasis_ringbuffer_nb_consume_bytes(stasis_ringbuffer_t * ring, lsn_t off, lsn_t* sz) {
  if(off == RING_NEXT) { off = ring->rf; }
  if(*sz == RING_NEXT) {
    *sz  = ring->wt - off;
    if(*sz == 0) { return RING_VOLATILE; }
  }

  // has the entire byte range been consumed?  (This is "normal".)
  if(off + *sz < ring->rt) { return RING_TRUNCATED; }

  // check to see if only some part of the range has been consumed.
  // (Probably bad news for the caller, but not our problem)

  if(off      < ring->rt) { return RING_TORN; }

  // part of the byte range is still being written.  Recovering from
  // this at the caller is probably easy (just wait a bit), but
  // something fugly is going on.
  if(off + *sz > ring->wt) { return RING_VOLATILE; }

  if(ring->rf < off + *sz) { ring->rf = off + *sz; }

  return off;
}
lsn_t stasis_ringbuffer_consume_bytes(stasis_ringbuffer_t * ring, lsn_t* sz, lsn_t * handle) {

  pthread_mutex_lock(&ring->mut);
  lsn_t ret;
  lsn_t orig_sz = *sz;

  if(ring->flush > ring->rf) {
    if(ring->flush > ring->rf) { *sz = RING_NEXT; }
  }
  if(ring->shutdown) {
    if(ring->rt == ring->wf) {
      pthread_cond_signal(&ring->read_done);
      pthread_mutex_unlock(&ring->mut);
      return RING_CLOSED;
    } else {
      *sz = RING_NEXT;
    }
  }

  while(RING_VOLATILE == (ret = stasis_ringbuffer_nb_consume_bytes(ring, RING_NEXT, sz))) {
    pthread_cond_wait(&ring->write_done, &ring->mut);
    *sz = (ring->flush > ring->rf) ? RING_NEXT : orig_sz;
    if(ring->shutdown) {
        if(ring->rt == ring->wf) {
          fprintf(stderr, "Shutting down, and there are no more bytes.  Signaling shutdown thread.\n");
          pthread_cond_signal(&ring->read_done);
          pthread_mutex_unlock(&ring->mut);
          return RING_CLOSED;
        } else {
          *sz = RING_NEXT;
        }
    }
  }
  if(handle) {
    *handle = ret;
    stasis_aggregate_min_add(ring->min_reader, handle);
  }
  pthread_mutex_unlock(&ring->mut);
  return ret;
}
// Not threadsafe.
const void * stasis_ringbuffer_nb_get_rd_buf(stasis_ringbuffer_t * ring, lsn_t off, lsn_t sz) {
  lsn_t off2 = stasis_ringbuffer_nb_consume_bytes(ring, off, &sz);
  if(off2 != off) { if(off != RING_NEXT || (off2 < 0 && off2 > RING_MINERR)) { return (const void*) (intptr_t)off2; } }
  assert(! (off2 < 0 && off2 >= RING_MINERR));
  return ptr_off(ring, off2);
}
// Explicit synchronization (blocks).
const void * stasis_ringbuffer_get_rd_buf(stasis_ringbuffer_t * ring, lsn_t off, lsn_t sz) {
  pthread_mutex_lock(&ring->mut);
  const void * ret;
  assert(sz != RING_NEXT);
  while(((const void*)RING_VOLATILE) == (ret = stasis_ringbuffer_nb_get_rd_buf(ring, off, sz))) {
    pthread_cond_wait(&ring->write_done, &ring->mut);
  }
  pthread_mutex_unlock(&ring->mut);
  return ret;
}
// No need for synchronization (only touches read-only-fields)
void * stasis_ringbuffer_get_wr_buf(stasis_ringbuffer_t * ring, lsn_t off, lsn_t sz) {
  return ptr_off(ring, off);
}
void   stasis_ringbuffer_nb_advance_write_tail(stasis_ringbuffer_t * ring, lsn_t off) {
  assert(off >= ring->wt);
  ring->wt = off;
  assert(ring->wt <= ring->wf);
}
lsn_t stasis_ringbuffer_current_write_tail(stasis_ringbuffer_t * ring) {
  pthread_mutex_lock(&ring->mut);
  lsn_t ret = ring->wt;
  pthread_mutex_unlock(&ring->mut);
  return ret;
}
void stasis_ringbuffer_advance_write_tail(stasis_ringbuffer_t * ring, lsn_t off) {
  pthread_mutex_lock(&ring->mut);
  stasis_ringbuffer_nb_advance_write_tail(ring, off);
  pthread_cond_broadcast(&ring->write_done);
  pthread_mutex_unlock(&ring->mut);
}
void stasis_ringbuffer_write_done(stasis_ringbuffer_t * ring, lsn_t * off) {
  pthread_mutex_lock(&ring->mut);
  stasis_aggregate_min_remove(ring->min_writer, off);
  lsn_t * new_wtp = (lsn_t*)stasis_aggregate_min_compute(ring->min_writer);
  lsn_t new_wt = new_wtp ? *new_wtp : ring->wf;
  if(new_wt != ring->wt) {
    stasis_ringbuffer_nb_advance_write_tail(ring, new_wt);
    pthread_cond_broadcast(&ring->write_done);
  }
  pthread_mutex_unlock(&ring->mut);

}
lsn_t stasis_ringbuffer_get_read_tail(stasis_ringbuffer_t * ring) {
  pthread_mutex_lock(&ring->mut);
  lsn_t ret = ring->rt;
  pthread_mutex_unlock(&ring->mut);
  return ret;
}
lsn_t stasis_ringbuffer_get_write_tail(stasis_ringbuffer_t * ring) {
  pthread_mutex_lock(&ring->mut);
  lsn_t ret = ring->wt;
  pthread_mutex_unlock(&ring->mut);
  return ret;
}
lsn_t stasis_ringbuffer_get_write_frontier(stasis_ringbuffer_t * ring) {
  pthread_mutex_lock(&ring->mut);
  lsn_t ret = ring->wf;
  pthread_mutex_unlock(&ring->mut);
  return ret;
}
void   stasis_ringbuffer_nb_advance_read_tail(stasis_ringbuffer_t * ring, lsn_t off) {
  assert(off >= ring->rt);
  assert(off <= ring->rf);
  ring->rt = off;
}
void stasis_ringbuffer_advance_read_tail(stasis_ringbuffer_t * ring, lsn_t off) {
  pthread_mutex_lock(&ring->mut);
  stasis_ringbuffer_nb_advance_read_tail(ring, off);
  pthread_cond_broadcast(&ring->read_done);
  pthread_mutex_unlock(&ring->mut);
}
void stasis_ringbuffer_read_done(stasis_ringbuffer_t * ring, lsn_t * off) {
  pthread_mutex_lock(&ring->mut);
  stasis_aggregate_min_remove(ring->min_reader, off);
  lsn_t * new_rtp = (lsn_t*)stasis_aggregate_min_compute(ring->min_reader);
  lsn_t new_rt = new_rtp ? *new_rtp : ring->rf;
  if(new_rt != ring->rt) {
    stasis_ringbuffer_nb_advance_read_tail(ring, new_rt);
    pthread_cond_broadcast(&ring->read_done);
  }
  pthread_mutex_unlock(&ring->mut);
}

stasis_ringbuffer_t * stasis_ringbuffer_init(intptr_t base, lsn_t initial_offset) {

  if(base < 12) {
    fprintf(stderr, "can't allocate ringbuffer that is less than 4096 bytes.\n");
    return 0;
  }

  stasis_ringbuffer_t * ring = malloc(sizeof(*ring));

  // Allocate the memory region using mmap black magic.

  char* name = strdup("/tmp/stasis-ringbuffer-XXXXXX");
  ring->fd = mkstemp(name);
  if(ring->fd == -1) { perror("Couldn't mkstemp\n"); abort(); }

  int err;

  err = unlink(name);

  if(err == -1) { perror("Couldn't unlink mkstemp file\n"); }

  free(name);

  ring->mask = (1 << base) - 1;
  lsn_t size = ring->mask+1;
  err = ftruncate(ring->fd, size);

  if(err == -1) { perror("Couldn't ftruncate file"); }

  ring->mem = mmap(0, size*2, PROT_NONE, MAP_ANONYMOUS|MAP_PRIVATE, -1, 0);

  if(ring->mem == MAP_FAILED) { perror("Couldn't mmap anonymous region"); abort(); }

  void * errp = mmap(ring->mem, size, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_FIXED, ring->fd, 0);

  if(errp == MAP_FAILED) { perror("Couldn't mmap temp region"); abort(); }

  errp = mmap(((char*)ring->mem)+size, size, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_FIXED, ring->fd, 0);

  if(errp == MAP_FAILED) { perror("Couldn't mmap temp region the second time."); abort(); }

  // Done with the black magic.

  ring->rt = ring->rf = ring->wt = ring->wf = initial_offset;

  ring->min_reader = stasis_aggregate_min_init(0);
  ring->min_writer = stasis_aggregate_min_init(0);
  ring->flush = 0;
  ring->shutdown = 0;

  pthread_mutex_init(&ring->mut,0);
  pthread_cond_init(&ring->read_done,0);
  pthread_cond_init(&ring->write_done,0);

  return ring;
}
static int stasis_ringbuffer_flush_impl(stasis_ringbuffer_t * ring, lsn_t off, int past_end) {
  pthread_mutex_lock(&ring->mut);
  if(ring->wt < off && !past_end) {
    pthread_mutex_unlock(&ring->mut);
    return RING_VOLATILE;
  }
  if(ring->flush < off) { ring->flush = off; }
  while(ring->rt < off) {
    pthread_cond_signal(&ring->write_done);
    DEBUG("sleeping for flush rt = %lld off = %lld\n", ring->rt, off);
    pthread_cond_wait(&ring->read_done, &ring->mut);
  }
  DEBUG("flushed rt = %lld off = %lld\n", ring->rt, off);
  pthread_mutex_unlock(&ring->mut);
  return 0;
}
int stasis_ringbuffer_tryflush(stasis_ringbuffer_t * ring, lsn_t off) {
  return stasis_ringbuffer_flush_impl(ring, off, 0);
}
void stasis_ringbuffer_flush(stasis_ringbuffer_t * ring, lsn_t off) {
  stasis_ringbuffer_flush_impl(ring, off, 1);
}
void stasis_ringbuffer_shutdown(stasis_ringbuffer_t * ring) {
  pthread_mutex_lock(&ring->mut);
  ring->shutdown = 1;
  do {
//    fprintf(stderr, "%lld < %lld signaling readers for shutdown and sleeping\n", ring->rt, ring->wf);
    pthread_cond_signal(&ring->write_done);
    pthread_cond_wait(&ring->read_done,&ring->mut);
//    fprintf(stderr, "readers done\n");
  } while (ring->rt < ring->wf);

  pthread_mutex_unlock(&ring->mut);
}
void stasis_ringbuffer_free(stasis_ringbuffer_t * ring) {
  lsn_t size = ring->mask+1;
  int err = munmap(((char*)ring->mem), size * 2);
  if(err == -1) { perror("could not munmap first half of ringbuffer"); }
  munmap(((char*)ring->mem)+size, size);
  if(err == -1) { perror("could not munmap second half of ringbuffer"); }
  munmap(((char*)ring->mem), size);
  if(err == -1) { perror("could not munmap hidden backing region of ringbuffer"); }

  stasis_aggregate_min_deinit(ring->min_reader);
  stasis_aggregate_min_deinit(ring->min_writer);
  pthread_mutex_destroy(&ring->mut);
  pthread_cond_destroy(&ring->read_done);
  pthread_cond_destroy(&ring->write_done);
  close(ring->fd);
  free(ring);
}
