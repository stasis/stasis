#ifndef STASIS_PAGEHANDLE_H
#define STASIS_PAGEHANDLE_H
#include <stasis/page.h>
#include <stasis/io/handle.h>

typedef struct stasis_page_handle_t {
  /**
   * Write page to disk, including correct LSN.  Doing so may require a
   * call to logSync().  There is not much that can be done to avoid
   * this call right now.  In the future, it might make sense to check
   * to see if some other page can be kicked, in order to avoid the log
   * flush.
   *
   * This function is automatically called immediately before a page is
   * evicted from cache.  Operation implementors, and normal users
   * should never have to call this routine.
   *
   * @see bufferManager.c for the implementation of pageWrite
   *
   * @param dat  The page to be flushed to disk.  No concurrent calls
   * may have the same value of dat.
   *
   */
  void (*write)(struct stasis_page_handle_t* ph, Page * dat);

  /**
     Read a page from disk. This bypasses the cache, and should only be
     called by bufferManager and blobManager.  To retrieve a page under
     normal circumstances, use loadPage() instead.

     Operation implementors and normal users should never need to call
     this routine.

     @param ret A page struct, with id set correctly.  The rest of this
     struct will be overwritten by pageMap.  This method assumes that no
     concurrent calls will be passed the same value of ret.

     @see bufferManager.c for the implementation of read_page.

     @todo read_page and pageWrite should be stored in a struct returned by
     an initializer, not in global function pointers.
  */
  void (*read)(struct stasis_page_handle_t* ph, Page * ret);
  /**
     Force the page file to disk.  Pages that have had pageWrite()
     called on them are guaranteed to be on disk after this returns.

     (Note that bufferManager implementations typically call pageWrite()
     automatically, so in general, other pages could be written back
     as well...)
  */
  void (*force_file)(struct stasis_page_handle_t* ph);
  void (*force_range)(struct stasis_page_handle_t* ph, lsn_t start, lsn_t stop);
  /**
     Force the page file to disk, then close it.
  */
  void (*close)(struct stasis_page_handle_t* ph);
  /**
   * Pointer to implementation-specific state.
   */
  void * impl;
} stasis_page_handle_t;

stasis_page_handle_t * stasis_page_handle_open(struct stasis_handle_t * handle);

#endif //STASIS_PAGEHANDLE_H
