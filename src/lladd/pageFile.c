/**
   @file 
    This file handles all of the file I/O for pages.

*/
#include "page.h"
#include <lladd/bufferManager.h>

#include "pageFile.h"
#include <assert.h>
#include <lladd/logger/logger2.h>
#include <lladd/truncation.h>
#include <sys/types.h>
#include <sys/stat.h>

/** For O_DIRECT.  It's unclear that this is the correct thing to \#define, but it works under linux. */
#define __USE_GNU

#include <fcntl.h>
#include <unistd.h>

/** Allows boostrapping of the header page. */
#include <lladd/operations/pageOperations.h>

static int stable = -1;
static pthread_mutex_t stable_mutex;
static void pfForcePageFile();
static void pfClosePageFile();
inline static pageid_t myLseekNoLock(int f, pageid_t offset, int whence);

static int oldOffset = -1;

int pageFile_isDurable = 1;

static void pfPageRead(Page *ret) {

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

  ret->dirty = 0;
  ret->LSN = *lsn_ptr(ret);

  pthread_mutex_unlock(&stable_mutex);

}
/** @todo need to sync the page file to disk occasionally, so that the
    dirty page table can be kept up to date. */
static void pfPageWrite(Page * ret) {
  /** If the page is clean, there's no reason to write it out. */
  assert(ret->dirty == dirtyPages_isDirty(ret));
  if(!ret->dirty) {
    //  if(!dirtyPages_isDirty(ret)) { 
    DEBUG(" =^)~ "); 
    return; 
  }
  pageid_t pageoffset = ret->id * PAGE_SIZE;
  pageid_t offset ;

  /*  assert(ret->pending == 0); */
  
  // If necessary, force the log to disk so that ret's LSN will be stable.

  assert(ret->LSN == pageReadLSN(ret));
  LogForce(ret->LSN);

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

  dirtyPages_remove(ret);

  pthread_mutex_unlock(&stable_mutex);
}
//#define PAGE_FILE_O_DIRECT

/** @todo O_DIRECT is broken in older linuxes (eg 2.4).  The build script should disable it on such platforms. */
void openPageFile() {
  pageRead = pfPageRead;
  pageWrite = pfPageWrite;
  forcePageFile = pfForcePageFile;
  closePageFile = pfClosePageFile;

  DEBUG("Opening storefile.\n");

#ifdef PAGE_FILE_O_DIRECT
  stable = open (STORE_FILE, O_CREAT | O_RDWR | O_DIRECT, FILE_PERM); //S_IRWXU | S_IRWXG | S_IRWXO);
#else
  stable = open (STORE_FILE, O_CREAT | O_RDWR, FILE_PERM);//S_IRWXU | S_IRWXG | S_IRWXO);
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

}

static void pfForcePageFile() { 
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

static void pfClosePageFile() {
  forcePageFile();
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

