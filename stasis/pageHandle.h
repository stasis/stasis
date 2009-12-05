#ifndef STASIS_PAGEHANDLE_H
#define STASIS_PAGEHANDLE_H

typedef struct stasis_page_handle_t stasis_page_handle_t;

#include <stasis/page.h>
#include <stasis/io/handle.h>
#include <stasis/logger/logger2.h>

/**
 * Provides page-based, write-ahead access to the page file.
 *
 * The operations provided by page handles maintain the write-ahead invariant,
 * and callers to write pages to file-handle buffers, and to force write the
 * buffers to disk.
 */
struct stasis_page_handle_t {
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
  */
  void (*read)(struct stasis_page_handle_t* ph, Page * ret, pagetype_t type);
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
     The write ahead log associated with this page handle.

     If this is non-null, stasis_page_handle implementations will call
     stasis_log_force on this to maintain the write-ahead invariant.
   */
  stasis_log_t * log;
  /**
     The dirty page table associated with this page handle.

     If this is non-null, stasis_page_handle will keep the dirty page table up-to-date.
   */
  stasis_dirty_page_table_t * dirtyPages;
  /**
   * Pointer to implementation-specific state.
   */
  void * impl;
};
/**
  Open a Stasis page handle.

  @param handle A stasis_handle_t that will perform I/O to the page file.
  @param log A stasis_log_t that will be used to maintain the write ahead invariant.
         If null, then write ahead will not be maintained.
  @param dirtyPages A stasis_dirty_page_table that will be updated as pages are written back.
  @return a handle that performs high-level (page based, write-ahead) page file I/O.
 */
stasis_page_handle_t * stasis_page_handle_open(struct stasis_handle_t * handle,
                                               stasis_log_t * log, stasis_dirty_page_table_t * dirtyPages);

#endif //STASIS_PAGEHANDLE_H
