#include <assert.h>
#include <string.h>
#include <errno.h>
#include <stasis/io/handle.h>
#include <stasis/pageHandle.h>
#include <stasis/bufferPool.h>
#include <stasis/logger/logger2.h>
#include <stasis/truncation.h>
void (*pageWrite)(Page * dat);
void (*pageRead)(Page * ret);
void (*forcePageFile)();
void (*forceRangePageFile)();
void (*closePageFile)();

int printedForceWarning = 0;

static stasis_handle_t * h;
/**
    @todo Make sure this doesn't need to be atomic.  (It isn't!) Can
    we get in trouble by setting the page clean after it's written
    out, or forcing the log too early?
*/
static void phWrite(Page * ret) {
  if(!ret->dirty) { return; }
  // This lock is only held to make the page implementation happy.  We should
  // implicitly have exclusive access to the page before this function is called,
  // or we'll deadlock.
  writelock(ret->rwlatch,0);
  stasis_page_flushed(ret);
  LogForce(ret->LSN);
  int err = h->write(h, PAGE_SIZE * ret->id, ret->memAddr, PAGE_SIZE);
  if(err) {
    printf("Couldn't write to page file: %s\n", strerror(err));
    fflush(stdout);
    abort();
  }
  dirtyPages_remove(ret);
  unlock(ret->rwlatch);
}
static void phRead(Page * ret) {
  writelock(ret->rwlatch,0);
  int err = h->read(h, PAGE_SIZE * ret->id, ret->memAddr, PAGE_SIZE);
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
  ret->dirty = 0;
  stasis_page_loaded(ret);
  unlock(ret->rwlatch);
}
static void phForce() { 
  int err = h->force(h);
  assert(!err);
}
static void phForceRange(lsn_t start, lsn_t stop) {
  int err = h->force_range(h,start,stop);
  assert(!err);
}
static void phClose() { 
  int err = h->close(h);
  DEBUG("Closing pageHandle\n");
  if(err) {
    printf("Couldn't close page file: %s\n", strerror(err));
    fflush(stdout);
    abort();
  }  
}

void pageHandleOpen(stasis_handle_t * handle) { 
  DEBUG("Using pageHandle implementation\n");
  pageWrite = phWrite;
  pageRead  = phRead;
  forcePageFile = phForce;
  forceRangePageFile = phForceRange;
  closePageFile = phClose;
  h = handle;
}
