#include <stasis/pageHandle.h>

#include <assert.h>
#include <stdio.h>

/**
    @todo Make sure this doesn't need to be atomic.  (It isn't!) Can
    we get in trouble by setting the page clean after it's written
    out, or forcing the log too early?
*/
static void phWrite(stasis_page_handle_t * ph, Page * ret) {
  DEBUG("\nPAGEWRITE %lld\n", ret->id);
  // This lock is only held to make the page implementation happy.  We should
  // implicitly have exclusive access to the page before this function is called,
  // or we'll deadlock.
  int locked = trywritelock(ret->rwlatch,0);
  assert(locked);
  if(!ret->dirty) { unlock(ret->rwlatch); return; }
  stasis_page_flushed(ret);
  if(ph->log) { stasis_log_force(ph->log, ret->LSN, LOG_FORCE_WAL); }
  int err = ((stasis_handle_t*)ph->impl)->write(ph->impl, PAGE_SIZE * ret->id, ret->memAddr, PAGE_SIZE);
  if(err) {
    printf("Couldn't write to page file: %s\n", strerror(err));
    fflush(stdout);
    abort();
  }
  stasis_dirty_page_table_set_clean(ph->dirtyPages, ret);
  unlock(ret->rwlatch);
}
static void phRead(stasis_page_handle_t * ph, Page * ret, pagetype_t type) {
  int locked = trywritelock(ret->rwlatch,0);
  assert(locked);
  int err = ((stasis_handle_t*)ph->impl)->read(ph->impl, PAGE_SIZE * ret->id, ret->memAddr, PAGE_SIZE);
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
  unlock(ret->rwlatch);
}
static void phPrefetchRange(stasis_page_handle_t *ph, pageid_t pageid, pageid_t count) {
  // TODO RTFM and see if Linux provides a decent API for prefetch hints.
  lsn_t off = pageid * PAGE_SIZE;
  lsn_t len = count * PAGE_SIZE;

  byte * buf = malloc(len);

  ((stasis_handle_t*)ph->impl)->read(ph->impl, off, buf, len);

  free(buf);
}
static void phForce(stasis_page_handle_t * ph) {
  int err = ((stasis_handle_t*)ph->impl)->force(ph->impl);
  assert(!err);
}
static void phForceRange(stasis_page_handle_t * ph, lsn_t start, lsn_t stop) {
  int err = ((stasis_handle_t*)ph->impl)->force_range(ph->impl,start*PAGE_SIZE,stop*PAGE_SIZE);
  assert(!err);
}
static void phClose(stasis_page_handle_t * ph) {
  int err = ((stasis_handle_t*)ph->impl)->close(ph->impl);
  DEBUG("Closing pageHandle\n");
  if(err) {
    printf("Couldn't close page file: %s\n", strerror(err));
    fflush(stdout);
    abort();
  }
  free(ph);
}
stasis_page_handle_t * stasis_page_handle_open(stasis_handle_t * handle,
                                               stasis_log_t * log, stasis_dirty_page_table_t * dpt) {
  DEBUG("Using pageHandle implementation\n");
  stasis_page_handle_t * ret = malloc(sizeof(*ret));
  ret->write = phWrite;
  ret->read  = phRead;
  ret->prefetch_range = phPrefetchRange;
  ret->force_file = phForce;
  ret->force_range = phForceRange;
  ret->close = phClose;
  ret->log = log;
  ret->dirtyPages = dpt;
  ret->impl = handle;
  return ret;
}
