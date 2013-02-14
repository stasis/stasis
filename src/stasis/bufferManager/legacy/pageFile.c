/**
   @file
    This file handles all of the file I/O for pages.

*/
#ifndef _GNU_SOURCE
#define _GNU_SOURCE  // for sync file range constants.
#endif
#include "config.h"

#include <stasis/page.h>
#include <stasis/bufferManager.h>
#include <stasis/pageHandle.h>
#include <stasis/truncation.h>

#include <stasis/bufferManager/legacy/pageFile.h>
#include <stasis/logger/logger2.h>

#include <assert.h>
#include <sys/stat.h>

#include <stdio.h>

#include <fcntl.h>
#include <unistd.h>

/** Allows bootstrapping of the header page. */
#include <stasis/operations/pageOperations.h>

static int stable = -1;
static pthread_mutex_t stable_mutex;
static void pfForcePageFile(stasis_page_handle_t* h);
static void pfClosePageFile(stasis_page_handle_t* h);
static void pfForceRangePageFile(stasis_page_handle_t* h, lsn_t start, lsn_t stop) ;
inline static pageid_t myLseekNoLock(int f, pageid_t offset, int whence);

static int oldOffset = -1;

int pageFile_isDurable = 1;

static void pfPageRead(stasis_page_handle_t * h, Page *ret, pagetype_t type) {

  pageid_t pageoffset;
  pageid_t offset;

  pageoffset = ret->id * PAGE_SIZE;
  pthread_mutex_lock(&stable_mutex);


  if(oldOffset != pageoffset) {
    offset = myLseekNoLock(stable, pageoffset, SEEK_SET);
    assert(offset == pageoffset);
  } else {
    offset = oldOffset;
  }
  oldOffset = offset + PAGE_SIZE;

  assert(offset == pageoffset);
  int read_size;
  read_size = read(stable, ret->memAddr, PAGE_SIZE);
  if(read_size != PAGE_SIZE) {
    if (!read_size) {  /* Past EOF... */
      memset(ret->memAddr, 0, PAGE_SIZE); // The file will be extended when we write to the new page.
    } else if(read_size == -1) {
      perror("pageFile.c couldn't read");
      fflush(NULL);
      abort();
    } else {
      printf("pageFile.c readfile: read_size = %d, errno = %d\n", read_size, errno);
      abort();
    }
  }
  assert(ret->dirty == 0);
  stasis_page_loaded(ret, type);

  pthread_mutex_unlock(&stable_mutex);

}
/** @todo need to sync the page file to disk occasionally, so that the
    dirty page table can be kept up to date. */
static void pfPageWrite(stasis_page_handle_t * h, Page * ret) {
  /** If the page is clean, there's no reason to write it out. */
  assertlocked(ret->rwlatch);
  if(!stasis_dirty_page_table_is_dirty(h->dirtyPages, ret)) {
    DEBUG(" =^)~ ");
    return;
  }
  pageid_t pageoffset = ret->id * PAGE_SIZE;
  pageid_t offset ;

  stasis_page_flushed(ret);

  // If necessary, force the log to disk so that ret's LSN will be stable.

  assert(ret->LSN == stasis_page_lsn_read(ret));
  if(h->log) { stasis_log_force(h->log, ret->LSN, LOG_FORCE_WAL); }

  pthread_mutex_lock(&stable_mutex);

  if(oldOffset != pageoffset) {
    offset = myLseekNoLock(stable, pageoffset, SEEK_SET);
    assert(offset == pageoffset);
  } else {
    offset = oldOffset;
  }
  oldOffset = offset + PAGE_SIZE;
  assert(ret->memAddr);

  /*  DEBUG("Writing page %d\n", ret->id); */
  int write_ret = write(stable, ret->memAddr, PAGE_SIZE);
  if(write_ret != PAGE_SIZE) {
    if(-1 == write_ret) {
      perror("pageFile.c couldn't write");
      fflush(NULL);
      abort();
    } else if(0 == write_ret) {
      /* now what? */
      printf("write_ret is zero\n");
      fflush(NULL);
      abort();
    } else {
      printf("write_ret is %d\n", write_ret);
      fflush(NULL);
      abort();
    }
  }

  stasis_dirty_page_table_set_clean(h->dirtyPages, ret);

  pthread_mutex_unlock(&stable_mutex);
}
//#define PAGE_FILE_O_DIRECT

/** @todo O_DIRECT is broken in older linuxes (eg 2.4).  The build script should disable it on such platforms. */
stasis_page_handle_t*  openPageFile(stasis_log_t * log, stasis_dirty_page_table_t * dpt) {
  stasis_page_handle_t * ret = stasis_alloc(stasis_page_handle_t);
  ret->read = pfPageRead;
  ret->write = pfPageWrite;
  ret->force_file = pfForcePageFile;
  ret->force_range = pfForceRangePageFile;
  ret->close = pfClosePageFile;
  ret->log = log;
  ret->dirtyPages = dpt;
  DEBUG("Opening storefile.\n");

#ifdef PAGE_FILE_O_DIRECT
  stable = open (stasis_store_file_name,
                 O_CREAT | O_RDWR | O_DIRECT, FILE_PERM);
#else
  stable = open (stasis_store_file_name,
                 O_CREAT | O_RDWR, FILE_PERM);
#endif
  if(!pageFile_isDurable) {
    fprintf(stderr, "\n**********\n");
    fprintf  (stderr, "pageFile.c: pageFile_isDurable==0; the page file will not force writes to disk.\n");
    fprintf  (stderr, "            Transactions will not be durable if the system crashes.\n**********\n");
  }
  if(stable == -1) {
    perror("couldn't open storefile");
    fflush(NULL);
    abort();
  }

  pthread_mutex_init(&stable_mutex, NULL);
  return ret;
}

static void pfForcePageFile(stasis_page_handle_t * h) {
  if(pageFile_isDurable) {
#ifndef PAGE_FILE_O_DIRECT
#ifdef HAVE_FDATASYNC
  fdatasync(stable);
#else
  fsync(stable);
#endif // HAVE_FDATASYNC
#endif // PAGE_FILE_O_DIRECT
  }
}

static void pfForceRangePageFile(stasis_page_handle_t * h, lsn_t start, lsn_t stop) {
  if(pageFile_isDurable) {
#ifdef HAVE_SYNC_FILE_RANGE
  int ret = sync_file_range(stable, start, stop,
			      SYNC_FILE_RANGE_WAIT_BEFORE |
			      SYNC_FILE_RANGE_WRITE |
			      SYNC_FILE_RANGE_WAIT_AFTER);
  assert(!ret);
#else
#ifdef HAVE_FDATASYNC
  fdatasync(stable);
#else
  fsync(stable);
#endif
#endif
  }
}
static void pfClosePageFile(stasis_page_handle_t * h) {
  assert(stable != -1);
  h->force_file(h);
  DEBUG("Closing storefile\n");

  int ret = close(stable);


  if(-1 == ret) {
    perror("Couldn't close storefile.");
    fflush(NULL);
    abort();
  }
  stable = -1;
}

static pageid_t myLseekNoLock(int f, pageid_t offset, int whence) {
  assert(! ( offset % 4096 ));
  pageid_t ret = lseek(f, offset, whence);
  if(ret == -1) {
    perror("Couldn't seek.");
    fflush(NULL);
    abort();
  }
  return ret;
}

stasis_page_handle_t* stasis_page_handle_deprecated_factory(stasis_log_t *log, stasis_dirty_page_table_t *dpt) {
  printf("\nWarning: Using old I/O routines (with known bugs).\n");
  return openPageFile(log, dpt);
}
