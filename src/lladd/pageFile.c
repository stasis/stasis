/**
   
    This file handles all of the file I/O for pages.

*/

#include <lladd/bufferManager.h>

#include "page.h"
#include "pageFile.h"
#include <assert.h>
#include "logger/logWriter.h"

static FILE * stable = NULL;
/** Defined in bufferManager.c */
extern pthread_mutex_t add_pending_mutex;



/** 
    This function blocks until there are no events pending for this page.

    @see addPendingEvent(), removePendingEvent()
*/


void finalize(Page * p) {
  pthread_mutex_lock(&(add_pending_mutex));
  p->waiting++;

  while(p->pending) {
    DEBUG("A");
    pthread_cond_wait(&(p->noMorePending), &(add_pending_mutex));
  }

  pthread_mutex_unlock(&(add_pending_mutex)); 
  pthread_cond_broadcast(&addPendingOK);

  return;
}



/* This function is declared in page.h */
void pageRead(Page *ret) {
  long fileSize;

  long pageoffset;
  long offset;


  /** @todo pageRead() is using fseek to calculate the file size on each read, which is inefficient. */
  pageoffset = ret->id * PAGE_SIZE;
  flockfile(stable);

  fileSize = myFseekNoLock(stable, 0, SEEK_END);


  /*  DEBUG("Reading page %d\n", ret->id); */

  /*  if(!ret->memAddr) {
    ret->memAddr = malloc(PAGE_SIZE);
  }
  if(!ret->memAddr) {
    perror("pageFile.c");
    fflush(NULL);
  }
  assert(ret->memAddr); */
  
  if ((ret->id)*PAGE_SIZE >= fileSize) {
    myFseekNoLock(stable, (1+ ret->id) * PAGE_SIZE -1, SEEK_SET);
    if(1 != fwrite("", 1, 1, stable)) {
      if(feof(stable)) { printf("Unexpected eof extending storefile!\n"); fflush(NULL); abort(); }
      if(ferror(stable)) { printf("Error extending storefile! %d", ferror(stable)); fflush(NULL); abort(); }
    }

  }
  offset = myFseekNoLock(stable, pageoffset, SEEK_SET);
  assert(offset == pageoffset);

  if(1 != fread(ret->memAddr, PAGE_SIZE, 1, stable)) {
                                                                                                                 
    if(feof(stable)) { printf("Unexpected eof reading!\n"); fflush(NULL); abort(); }
    if(ferror(stable)) { printf("Error reading stream! %d", ferror(stable)); fflush(NULL); abort(); }
                                                                                                                 
  }

  funlockfile(stable);

}

/* This function is declared in page.h */
void pageWrite(Page * ret) {

  long pageoffset = ret->id * PAGE_SIZE;
  long offset ;

  assert(ret->pending == 0);
  
  if(flushedLSN() < pageReadLSN(ret)) {
    DEBUG("pageWrite is calling syncLog()!\n"); 
    syncLog();
  }

  flockfile(stable);
  offset = myFseekNoLock(stable, pageoffset, SEEK_SET);
  assert(offset == pageoffset);
  assert(ret->memAddr);

  /*  DEBUG("Writing page %d\n", ret->id); */

  if(1 != fwrite(ret->memAddr, PAGE_SIZE, 1, stable)) {
                                                                                                                 
    if(feof(stable)) { printf("Unexpected eof writing!\n"); fflush(NULL); abort(); }
    if(ferror(stable)) { printf("Error writing stream! %d", ferror(stable)); fflush(NULL); abort(); }
                                                                                                                 
  }

  funlockfile(stable);
}

void openPageFile() {

  DEBUG("Opening storefile.\n");
  if( ! (stable = fopen(STORE_FILE, "r+"))) { /* file may not exist */
    byte* zero = calloc(1, PAGE_SIZE);

    if(!(stable = fopen(STORE_FILE, "w+"))) { perror("Couldn't open or create store file"); abort(); }

    /* Write out one page worth of zeros to get started. */
    
    if(1 != fwrite(zero, PAGE_SIZE, 1, stable)) { assert (0); }

    free(zero);
  }
  
  DEBUG("storefile opened.\n");

}
void closePageFile() {

  int ret = fclose(stable);
  assert(!ret);
  stable = NULL;
}

long myFseek(FILE * f, long offset, int whence) {
  long ret;
  flockfile(f);
  ret = myFseekNoLock(f, offset, whence);
  funlockfile(f);
  return ret;
}

long myFseekNoLock(FILE * f, long offset, int whence) {
  long ret;
  if(0 != fseek(f, offset, whence)) { perror ("fseek"); fflush(NULL); abort(); }
  if(-1 == (ret = ftell(f))) { perror("ftell"); fflush(NULL); abort(); }
  return ret;
}

void myFwrite(const void * dat, long size, FILE * f) {
  int nmemb = fwrite(dat, size, 1, f);
  /* test */
  if(nmemb != 1) {
    perror("myFwrite");
    abort();
    /*    return FILE_WRITE_OPEN_ERROR; */
  }
  
}


