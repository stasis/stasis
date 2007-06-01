#include <lladd/bufferPool.h>
#include <lladd/io/handle.h>
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
 * @param dat  The page to be flushed to disk.  No concurrent calls
 * may have the same value of dat.
 */
extern void (*pageWrite)(Page * dat); 

extern int pageFile_isDurable;

/**
   Read a page from disk. This bypassess the cache, and should only be
   called by bufferManager and blobManager.  To retrieve a page under
   normal circumstances, use loadPage() instead.

   Operation implementors and normal users should never need to call
   this routine.

   @param ret A page struct, with id set correctly.  The rest of this
   struct will be overwritten by pageMap.  This method assumes that no
   concurrent calls will be passed the same value of ret.
   
   @see bufferManager.c for the implementation of pageRead.

   @todo pageRead and pageWrite should be stored in a struct returned by
   an initailizer, not in global function pointers.
*/
extern void (*pageRead)(Page * ret);
/**
   Force the page file to disk.  Pages that have had pageWrite()
   called on them are guaranteed to be on disk after this returns.

   (Note that bufferManager implementations typically call pageWrite()
   automatically, so in general, other pages could be written back 
   as well...)
*/
extern void (*forcePageFile)();
/**
   Force the page file to disk, then close it.
*/
extern void (*closePageFile)();

void pageHandleOpen(stasis_handle_t * handle);
