#include <stasis/flags.h>
#include <stasis/pageHandle.h>

#include <assert.h>
#include <errno.h>
#include <stdio.h>
/**
    @todo Make sure this doesn't need to be atomic.  (It isn't!) Can
    we get in trouble by setting the page clean after it's written
    out, or forcing the log too early?
*/
static void phWrite(stasis_page_handle_t * ph, Page * ret) {
  stasis_handle_t* impl = (stasis_handle_t*) (ph->impl);
  DEBUG("\nPAGEWRITE %lld\n", ret->id);
  // The caller guarantees that we have exclusive access to the page, so
  // no further latching is necessary.
  if(!ret->dirty) { return; }
  stasis_page_flushed(ret);
  if(ph->log) { stasis_log_force(ph->log, ret->LSN, LOG_FORCE_WAL); }
  int err = impl->write(impl, PAGE_SIZE * ret->id, ret->memAddr, PAGE_SIZE);
  if(err) {
    printf("Couldn't write to page file: %s\n", strerror(err));
    fflush(stdout);
    abort();
  }
  stasis_dirty_page_table_set_clean(ph->dirtyPages, ret);
}
static void phRead(stasis_page_handle_t * ph, Page * ret, pagetype_t type) {
  stasis_handle_t* impl = (stasis_handle_t*) (ph->impl);
  // The caller guarantees that we have exclusive access to the page, so
  // no further latching is necessary.
  int err = impl->read(impl, PAGE_SIZE * ret->id, ret->memAddr, PAGE_SIZE);
  if(err) {
    if(err == EDOM) {
      // tried to read off end of file...
      memset(ret->memAddr, 0, PAGE_SIZE);
    } else {
      printf("Couldn't read from page file: %s\n", strerror(err));
      fflush(stdout);
      abort();
    }
  }
  assert(!ret->dirty);
  stasis_page_loaded(ret, type);
}
static void phPrefetchRange(stasis_page_handle_t *ph, pageid_t pageid, pageid_t count) {
  stasis_handle_t* impl = (stasis_handle_t*) (ph->impl);
  // TODO RTFM and see if Linux provides a decent API for prefetch hints.
  lsn_t off = pageid * PAGE_SIZE;
  lsn_t len = count * PAGE_SIZE;

  byte * buf = stasis_malloc(len, byte);

  impl->read(impl, off, buf, len);

  free(buf);
}
static int phPreallocateRange(stasis_page_handle_t * ph, pageid_t pageid, pageid_t count) {
  stasis_handle_t* impl = (stasis_handle_t*) (ph->impl);
  lsn_t off = pageid * PAGE_SIZE;
  lsn_t len = count * PAGE_SIZE;

  return impl->fallocate(impl, off, len);
}
static void phForce(stasis_page_handle_t * ph) {
  stasis_handle_t* impl = (stasis_handle_t*) (ph->impl);
  int err = impl->force(impl);
  assert(!err);
}
static void phAsyncForce(stasis_page_handle_t * ph) {
  stasis_handle_t* impl = (stasis_handle_t*) (ph->impl);
  int err = impl->async_force(impl);
  assert(!err);
}
static void phForceRange(stasis_page_handle_t * ph, lsn_t start, lsn_t stop) {
  stasis_handle_t* impl = (stasis_handle_t*) (ph->impl);
  int err = impl->force_range(impl,start*PAGE_SIZE,stop*PAGE_SIZE);
  assert(!err);
}
static void phClose(stasis_page_handle_t * ph) {
  stasis_handle_t* impl = (stasis_handle_t*) (ph->impl);
  int err = impl->close(impl);
  DEBUG("Closing pageHandle\n");
  if(err) {
    printf("Couldn't close page file: %s\n", strerror(err));
    fflush(stdout);
    abort();
  }
  free(ph);
}
static stasis_page_handle_t * phDup(stasis_page_handle_t * ph, int is_sequential) {
  stasis_page_handle_t * ret = stasis_alloc(stasis_page_handle_t);
  stasis_handle_t* impl = (stasis_handle_t*) (ph->impl);
  memcpy(ret, ph, sizeof(*ret));
  ret->impl = impl->dup(impl);
  stasis_handle_t* retimpl = (stasis_handle_t*)ret->impl;
  if(retimpl->error != 0) {
    fprintf(stderr, "Could not dup file handle: %s\n", strerror(retimpl->error));
    ret->close(ret);
    return 0;
  }
  if(is_sequential) {
    retimpl->enable_sequential_optimizations(retimpl);
  }
  return ret;
}
stasis_page_handle_t * stasis_page_handle_open(stasis_handle_t * handle,
                                               stasis_log_t * log, stasis_dirty_page_table_t * dpt) {
  DEBUG("Using pageHandle implementation\n");
  stasis_page_handle_t * ret = stasis_alloc(stasis_page_handle_t);
  ret->write = phWrite;
  ret->read  = phRead;
  ret->prefetch_range = phPrefetchRange;
  ret->preallocate_range = phPreallocateRange;
  ret->force_file = phForce;
  ret->async_force_file = phAsyncForce;
  ret->force_range = phForceRange;
  ret->close = phClose;
  ret->dup = phDup;
  ret->log = log;
  ret->dirtyPages = dpt;
  ret->impl = handle;
  return ret;
}
stasis_page_handle_t* stasis_page_handle_default_factory(stasis_log_t *log, stasis_dirty_page_table_t *dpt) {
  return stasis_page_handle_open(stasis_handle_factory(), log, dpt);
}
