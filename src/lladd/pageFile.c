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

/** For O_DIRECT.  It's unclear that this is the correct thing to #define, but it works under linux. */
#define __USE_GNU

#include <fcntl.h>
#include <unistd.h>

/** Allows boostrapping of the header page. */
#include <lladd/operations/pageOperations.h>

static int stable = -1;
static pthread_mutex_t stable_mutex;

/* static long myLseek(int f, long offset, int whence); */
static long myLseekNoLock(int f, long offset, int whence);

static int oldOffset = -1;


void pageRead(Page *ret) {

  long pageoffset;
  long offset;

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
      /*      long fileSize = myLseekNoLock(stable, 0, SEEK_END);
      offset = myLseekNoLock(stable, pageoffset, SEEK_SET);
      assert(offset == pageoffset); */
      /*      if(fileSize <= pageoffset) { */
	memset(ret->memAddr, 0, PAGE_SIZE);
	/*	write(stable, ret->memAddr, PAGE_SIZE); */ /* all this does is extend the file..why would we bother doing that? :) 
							      } */
    } else if(read_size == -1) { 
      perror("pageFile.c couldn't read");
      fflush(NULL);
      abort();
    } else {
      printf("pageFile.c readfile: read_size = %d, errno = %d\n", read_size, errno);
      abort();
    }
  }
  pthread_mutex_unlock(&stable_mutex);

}
/** @todo need to sync the page file to disk occasionally, so that the
    dirty page table can be kept up to date. */
void pageWrite(Page * ret) {
  /** If the page is clean, there's no reason to write it out. */
  if(!dirtyPages_isDirty(ret)) { 
    DEBUG(" =^)~ "); 
    return; 
  }
  long pageoffset = ret->id * PAGE_SIZE;
  long offset ;

  /*  assert(ret->pending == 0); */
  
  // If necessary, force the log to disk so that ret's LSN will be stable.
  LogForce(pageReadLSN(ret));

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
/** @todo O_DIRECT is broken in older linuxes (eg 2.4).  The build script should disable it on such platforms. */
void openPageFile() {

  DEBUG("Opening storefile.\n");

  stable = open (STORE_FILE, O_CREAT | O_RDWR | O_DIRECT, S_IRWXU | S_IRWXG | S_IRWXO);

  if(stable == -1) {
    perror("couldn't open storefile");
    fflush(NULL);
    abort();
  }
  
  pthread_mutex_init(&stable_mutex, NULL);

}
void closePageFile() {

  int ret = close(stable);

  if(-1 == ret) { 
    perror("Couldn't close storefile.");
    fflush(NULL);
    abort();
  }
  stable = -1;
}

static long myLseek(int f, long offset, int whence) {
  long ret;
  pthread_mutex_lock(&stable_mutex);
  ret = myLseekNoLock(f, offset, whence);
  pthread_mutex_unlock(&stable_mutex);
  return ret;
}

static long myLseekNoLock(int f, long offset, int whence) {
  assert(! ( offset % 4096 ));
  long ret = lseek(f, offset, whence);
  if(ret == -1) {
    perror("Couldn't seek.");
    fflush(NULL);
    abort();
  }
  return ret;
}

/*void myFwrite(const void * dat, long size, FILE * f) {
  int nmemb = fwrite(dat, size, 1, f); 
  / * test * /
  if(nmemb != 1) {
    perror("myFwrite");
    abort();
    / *    return FILE_WRITE_OPEN_ERROR; * /
  }
  
}*/

long pageCount() {
  pthread_mutex_lock(&stable_mutex);
  printf(".");
  long fileSize = myLseek(stable, 0, SEEK_END);

  oldOffset = -1;

  pthread_mutex_unlock(&stable_mutex);
  assert(! (fileSize % PAGE_SIZE));
  return fileSize / PAGE_SIZE;
}
