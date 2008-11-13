#include <stdio.h>
#include <stasis/logger/logWriterUtils.h>
#include <stdlib.h>
/** @file 

    This file contains old-ish utility methods that wrap fseek, read, etc...

*/
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
