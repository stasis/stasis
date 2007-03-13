#include <assert.h>
#include <string.h>
#include <errno.h>
#include <lladd/io/handle.h>
#include <lladd/pageHandle.h>
#include <lladd/bufferPool.h>
#include <lladd/logger/logger2.h>
#include <lladd/truncation.h>
void (*pageWrite)(Page * dat); 
void (*pageRead)(Page * ret);
void (*forcePageFile)();
void (*closePageFile)();

int printedForceWarning = 0;

static stasis_handle_t * h;
/** 
    @todo Make sure this doesn't need to be atomic.  (It isn't!) Can
    we get in trouble by setting the page clean after it's written
    out, or forcing the log too early? 
*/
static void phWrite(Page * ret) { 
  assert(ret->LSN == pageReadLSN(ret));
  if(!ret->dirty) { return; }
  LogForce(ret->LSN);
  int err = h->write(h, PAGE_SIZE * ret->id, ret->memAddr, PAGE_SIZE);
  if(err) {
    printf("Couldn't write to page file: %s\n", strerror(err));
    fflush(stdout);
    abort();
  }
  dirtyPages_remove(ret);
}

static void phRead(Page * ret) { 
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
  ret->LSN = *lsn_ptr(ret);
}
static void phForce() { 
  if(!printedForceWarning) { 
    printf("Warning!  pageHandle can't force the page file yet!\n");
    fflush(stdout);
  }
}
static void phClose() { 
  int err = h->close(h);
  if(err) {
    printf("Couldn't close page file: %s\n", strerror(err));
    fflush(stdout);
    abort();
  }  
}

void pageHandleOpen(stasis_handle_t * handle) { 
  pageWrite = phWrite;
  pageRead  = phRead;
  forcePageFile = phForce;
  closePageFile = phClose;
  h = handle;
}
