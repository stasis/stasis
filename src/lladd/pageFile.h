
#ifndef __PAGE_FILE_H
#define __PAGE_FILE_H
#include "page.h"

/**
 * Write page to disk, including correct LSN.  Doing so may require a
 * call to logSync().  There is not much that can be done to avoid
 * this call right now.  In the future, it might make sense to check
 * to see if some other page can be kicked, in order to avoid the log
 * flush.  
 *
 * This funciton is automatically called immediately before a page is
 * evicted from cache.  Operation implementors, and normal users
 * should never have to call this routine.
 *
 * @see bufferManager.c for the implementation of pageWrite
 *
 * @param dat  The page to be flushed to disk.
 */
void pageWrite(Page * dat); 

extern int pageFile_isDurable;

/**
   Read a page from disk. This bypassess the cache, and should only be
   called by bufferManager and blobManager.  To retrieve a page under
   normal circumstances, use loadPage() instead.

   Operation implementors and normal users should never need to call
   this routine.

   @param ret A page struct, with id set correctly.  The rest of this
   struct will be overwritten by pageMap.
   
   @see bufferManager.c for the implementation of pageRead.

   @todo pageRead and pageWrite should be static, but pageCache needs
   to call them.
*/
void pageRead(Page * ret);

void forcePageFile();

void openPageFile();
void closePageFile();

void finalize(Page * p);

#endif /* __PAGE_FILE_H */
