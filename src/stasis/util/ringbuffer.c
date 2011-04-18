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
  int64_t mask;
  // Track four regions:  write_frontier (wf), write_tail (wt), read_frontier (rf), read_tail (rt):

  // Logical buffer layout:
  // byte zero
  // ...

  int64_t rt; // First byte that some thread might be reading.  Earlier bytes can be reclaimed.
  int64_t rf; // First byte that will be returned by "read next".
  int64_t wt; // First byte that some thread might be writing.  Earlier bytes are stable for readers.
  int64_t wf; // First available byte

  // ...
  // byte 2^64

  stasis_aggregate_min_t * min_writer;
  stasis_aggregate_min_t * min_reader;

  // Synchronization stuff
  pthread_mutex_t mut;
  pthread_cond_t read_done;
  pthread_cond_t write_done;
};

// Does not need synchronization (only called from nb function).
static inline int64_t freespace(stasis_ringbuffer_t * ring) {
  int64_t ret =  ((ring->rt - ring->wf) - 1) & ring->mask;
//  printf("freespace is %lld\n", (long long)ret);
  return ret;
}

// Does not need any synchronization (all fields are read only)
static inline void* ptr_off(stasis_ringbuffer_t * ring, int64_t off) {
  return ring->mem + (off & ring->mask);
}

// Not threadsafe.
int64_t stasis_ringbuffer_nb_reserve_space(stasis_ringbuffer_t * ring, int64_t sz) {
  if(freespace(ring) < sz) { return RING_FULL; }
  int64_t ret = ring->wf;
  ring->wf += sz;
  return ret;
}
// Threadsafe (explicit synchronization).  Blocks.
int64_t stasis_ringbuffer_reserve_space(stasis_ringbuffer_t * ring, int64_t sz, int64_t * handle) {
  pthread_mutex_lock(&ring->mut);
  int64_t ret;
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
int64_t stasis_ringbuffer_nb_consume_bytes(stasis_ringbuffer_t * ring, int64_t off, int64_t* sz) {
  if(off == RING_NEXT) { off = ring->rf; }
  if(*sz == RING_NEXT)  { *sz  = ring->wt - off; }

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
int64_t stasis_ringbuffer_consume_bytes(stasis_ringbuffer_t * ring, int64_t* sz, int64_t * handle) {
  pthread_mutex_lock(&ring->mut);
  int64_t ret;
  while(RING_VOLATILE == (ret = stasis_ringbuffer_nb_consume_bytes(ring, RING_NEXT, sz))) {
    pthread_cond_wait(&ring->write_done, &ring->mut);
  }
  if(handle) {
    *handle = ret;
    stasis_aggregate_min_add(ring->min_reader, handle);
  }
  pthread_mutex_unlock(&ring->mut);
  return ret;
}
// Not threadsafe.
const void * stasis_ringbuffer_nb_get_rd_buf(stasis_ringbuffer_t * ring, int64_t off, int64_t sz) {
  int64_t off2 = stasis_ringbuffer_nb_consume_bytes(ring, off, &sz);
  if(off2 != off) { if(off != RING_NEXT || (off2 < 0 && off2 > RING_MINERR)) { return (const void*) off2; } }
  assert(! (off2 < 0 && off2 >= RING_MINERR));
  return ptr_off(ring, off2);
}
// Explicit synchronization (blocks).
const void * stasis_ringbuffer_get_rd_buf(stasis_ringbuffer_t * ring, int64_t off, int64_t* sz) {
  pthread_mutex_lock(&ring->mut);
  const void * ret;
  while(((const void*)RING_VOLATILE) == (ret = stasis_ringbuffer_nb_get_rd_buf(ring, off, *sz))) {
    pthread_cond_wait(&ring->write_done, &ring->mut);
  }
  pthread_mutex_unlock(&ring->mut);
  return ret;
}
// No need for synchronization (only touches read-only-fields)
void * stasis_ringbuffer_get_wr_buf(stasis_ringbuffer_t * ring, int64_t off, int64_t sz) {
  return ptr_off(ring, off);
}
void   stasis_ringbuffer_nb_advance_write_tail(stasis_ringbuffer_t * ring, int64_t off) {
  assert(off >= ring->wt);
  ring->wt = off;
  assert(ring->wt <= ring->wf);
}

void stasis_ringbuffer_advance_write_tail(stasis_ringbuffer_t * ring, int64_t off) {
  pthread_mutex_lock(&ring->mut);
  stasis_ringbuffer_nb_advance_write_tail(ring, off);
  pthread_cond_broadcast(&ring->write_done);
  pthread_mutex_unlock(&ring->mut);
}
void stasis_ringbuffer_write_done(stasis_ringbuffer_t * ring, int64_t * off) {
  pthread_mutex_lock(&ring->mut);
  stasis_aggregate_min_remove(ring->min_writer, off);
  int64_t * new_wtp = (int64_t*)stasis_aggregate_min_compute(ring->min_writer);
  int64_t new_wt = new_wtp ? *new_wtp : ring->wf;
  if(new_wt != ring->wt) {
    stasis_ringbuffer_nb_advance_write_tail(ring, new_wt);
    pthread_cond_broadcast(&ring->write_done);
  }
  pthread_mutex_unlock(&ring->mut);

}
void   stasis_ringbuffer_nb_advance_read_tail(stasis_ringbuffer_t * ring, int64_t off) {
  assert(off >= ring->rt);
  assert(off <= ring->rf);
  ring->rt = off;
}
void stasis_ringbuffer_advance_read_tail(stasis_ringbuffer_t * ring, int64_t off) {
  pthread_mutex_lock(&ring->mut);
  stasis_ringbuffer_nb_advance_read_tail(ring, off);
  pthread_cond_broadcast(&ring->read_done);
  pthread_mutex_unlock(&ring->mut);
}
void stasis_ringbuffer_read_done(stasis_ringbuffer_t * ring, int64_t * off) {
  pthread_mutex_lock(&ring->mut);
  stasis_aggregate_min_remove(ring->min_reader, off);
  int64_t * new_rtp = (int64_t*)stasis_aggregate_min_compute(ring->min_reader);
  int64_t new_rt = new_rtp ? *new_rtp : ring->rf;
  if(new_rt != ring->rt) {
    stasis_ringbuffer_nb_advance_read_tail(ring, new_rt);
    pthread_cond_broadcast(&ring->read_done);
  }
  pthread_mutex_unlock(&ring->mut);
}

stasis_ringbuffer_t * stasis_ringbuffer_init(intptr_t base, int64_t initial_offset) {

  if(base < 12) {
    fprintf(stderr, "can't allocate ringbuffer that is less than 4096 bytes.\n");
    return 0;
  }

  stasis_ringbuffer_t * ring = malloc(sizeof(*ring));

  // Allocate the memory region using mmap black magic.

  char* name = strdup("/dev/shm/stasis-ringbuffer-XXXXXX");
  int fd = mkstemp(name);
  if(fd == -1) { perror("Couldn't mkstemp\n"); abort(); }

  int err;

  err = unlink(name);

  if(err == -1) { perror("Couldn't unlink mkstemp file\n"); }

  free(name);

  ring->mask = (1 << base) - 1;
  int64_t size = ring->mask+1;
  err = ftruncate(fd, size);

  if(err == -1) { perror("Couldn't ftruncate file"); }

  ring->mem = mmap(0, size*2, PROT_NONE, MAP_ANONYMOUS|MAP_PRIVATE, -1, 0);

  if(ring->mem == MAP_FAILED) { perror("Couldn't mmap anonymous region"); abort(); }

  void * errp = mmap(ring->mem, size, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_FIXED, fd, 0);

  if(errp == MAP_FAILED) { perror("Couldn't mmap temp region"); abort(); }

  errp = mmap(((char*)ring->mem)+size, size, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_FIXED, fd, 0);

  if(errp == MAP_FAILED) { perror("Couldn't mmap temp region the second time."); abort(); }

  // Done with the black magic.

  ring->rt = ring->rf = ring->wt = ring->wf = 0;

  ring->min_reader = stasis_aggregate_min_init(0);
  ring->min_writer = stasis_aggregate_min_init(0);

  pthread_mutex_init(&ring->mut,0);
  pthread_cond_init(&ring->read_done,0);
  pthread_cond_init(&ring->write_done,0);

  return ring;

}
