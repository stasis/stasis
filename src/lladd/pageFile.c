/**
   
    This file handles all of the file I/O for pages.

*/

#include <lladd/bufferManager.h>

#include "page.h"
#include "pageFile.h"
#include <assert.h>
#include "logger/logWriter.h"

static FILE * stable = NULL;

/** 
    This function blocks until there are no events pending for this page.

    @see addPendingEvent(), removePendingEvent()
*/


static void finalize(Page * p) {
  pthread_mutex_lock(&(p->pending_mutex));
  p->waiting++;

  while(p->pending) {
    
    pthread_cond_wait(&(p->noMorePending), &(p->pending_mutex));
  }

  pthread_mutex_unlock(&(p->pending_mutex)); 

  return;
}



/* This function is declared in page.h */
void pageRead(Page *ret) {
  long fileSize = myFseek(stable, 0, SEEK_END);
  long pageoffset = ret->id * PAGE_SIZE;
  long offset;

  DEBUG("Reading page %d\n", ret->id);

  if(!ret->memAddr) {
    ret->memAddr = malloc(PAGE_SIZE);
  }
  assert(ret->memAddr);
  
  if ((ret->id)*PAGE_SIZE >= fileSize) {
    myFseek(stable, (1+ ret->id) * PAGE_SIZE -1, SEEK_SET);
    if(1 != fwrite("", 1, 1, stable)) {
      if(feof(stable)) { printf("Unexpected eof extending storefile!\n"); fflush(NULL); abort(); }
      if(ferror(stable)) { printf("Error extending storefile! %d", ferror(stable)); fflush(NULL); abort(); }
    }

  }
  offset = myFseek(stable, pageoffset, SEEK_SET);
  assert(offset == pageoffset);

  if(1 != fread(ret->memAddr, PAGE_SIZE, 1, stable)) {
                                                                                                                 
    if(feof(stable)) { printf("Unexpected eof reading!\n"); fflush(NULL); abort(); }
    if(ferror(stable)) { printf("Error reading stream! %d", ferror(stable)); fflush(NULL); abort(); }
                                                                                                                 
  }

}

/* This function is declared in page.h */
void pageWrite(Page * ret) {

  long pageoffset = ret->id * PAGE_SIZE;
  long offset = myFseek(stable, pageoffset, SEEK_SET);
  assert(offset == pageoffset);
  assert(ret->memAddr);

  DEBUG("Writing page %d\n", ret->id);
  /* Need to call finalize before checking the LSN.  Once finalize
     returns, we have exclusive access to this page, and can safely
     write it to disk. */
  finalize(ret);
  
  if(flushedLSN() < pageReadLSN(ret)) {
    DEBUG("pageWrite is calling syncLog()!\n");
    syncLog();
  }


  if(1 != fwrite(ret->memAddr, PAGE_SIZE, 1, stable)) {
                                                                                                                 
    if(feof(stable)) { printf("Unexpected eof writing!\n"); fflush(NULL); abort(); }
    if(ferror(stable)) { printf("Error writing stream! %d", ferror(stable)); fflush(NULL); abort(); }
                                                                                                                 
  }
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
  if(0 != fseek(f, offset, whence)) { perror ("fseek"); fflush(NULL); abort(); }
  if(-1 == (ret = ftell(f))) { perror("ftell"); fflush(NULL); abort(); }
  funlockfile(f);
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


