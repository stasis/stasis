/*---
This software is copyrighted by the Regents of the University of
California, and other parties. The following terms apply to all files
associated with the software unless explicitly disclaimed in
individual files.

The authors hereby grant permission to use, copy, modify, distribute,
and license this software and its documentation for any purpose,
provided that existing copyright notices are retained in all copies
and that this notice is included verbatim in any distributions. No
written agreement, license, or royalty fee is required for any of the
authorized uses. Modifications to this software may be copyrighted by
their authors and need not follow the licensing terms described here,
provided that the new terms are clearly indicated on the first page of
each file where they apply.

IN NO EVENT SHALL THE AUTHORS OR DISTRIBUTORS BE LIABLE TO ANY PARTY
FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES
ARISING OUT OF THE USE OF THIS SOFTWARE, ITS DOCUMENTATION, OR ANY
DERIVATIVES THEREOF, EVEN IF THE AUTHORS HAVE BEEN ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

THE AUTHORS AND DISTRIBUTORS SPECIFICALLY DISCLAIM ANY WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
NON-INFRINGEMENT. THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, AND
THE AUTHORS AND DISTRIBUTORS HAVE NO OBLIGATION TO PROVIDE
MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.

GOVERNMENT USE: If you are acquiring this software on behalf of the
U.S. government, the Government shall have only "Restricted Rights" in
the software and related documentation as defined in the Federal
Acquisition Regulations (FARs) in Clause 52.227.19 (c) (2). If you are
acquiring the software on behalf of the Department of Defense, the
software shall be classified as "Commercial Computer Software" and the
Government shall have only "Restricted Rights" as defined in Clause
252.227-7013 (c) (1) of DFARs. Notwithstanding the foregoing, the
authors grant the U.S. Government and others acting in its behalf
permission to use and distribute the software in accordance with the
terms specified in this license.
---*/
/**
 * @file
 * Manages the page buffer

    bufferManager - Provides cached page handling, delegates to blob
    manager when necessary.  Doesn't implement an eviction policy.
    That is left to a cacheManager.  (Multiple cacheManagers could be
    used with a single bufferManager.)

  @todo Allow error checking!

  @todo Refactoring for lock manager

  Possible interface for lockManager:

       Define three classes of objects that the lock manager is interested in:

         Transactions,
	 Operations,
	 Predicates.

       Stasis already has operations and transactions, and these can be
       relatively unchanged.  Predicates are read only operations that
       return a set of tuples.  Tread() is the simplest predicate.
       Index scans provide a motivating example.

       See http://research.microsoft.com/%7Eadya/pubs/icde00.pdf
       (Generalized Isolation Level Definitions, Adya, Liskov, O'Neil,
       2000) for a theoretical discussion of general locking schemes..

       Locking functions can return errors such as DEADLOCK, etc.
       When such a value is returned, the transaction aborts, and an
       error is passed up to the application.

       @ingroup BUFFER_MANAGER
 * $Id: bufferManager.h 1560 2011-10-08 22:01:04Z sears.russell@gmail.com $
 */
#ifndef __BUFFERMANAGER_H__
#define __BUFFERMANAGER_H__
#include <stasis/common.h>
BEGIN_C_DECLS
/**
 * Obtain a pointer to a page from the buffer manager.  The page will
 * be pinned, and the pointer valid until releasePage is called.
 *
 * @param xid The transaction that is pinning the page (used by page-level locking implementations.)
 *
 * @param pageid ID of the page you want to load
 *
 * @return fully formed Page type
 */
Page * loadPage(int xid, pageid_t pageid);

Page * loadPageOfType(int xid, pageid_t pageid, pagetype_t type);

Page * loadUninitializedPage(int xid, pageid_t pageid);

Page * loadPageForOperation(int xid, pageid_t pageid, int op);

void   prefetchPages(pageid_t pageid, pageid_t count);
int    preallocatePages(pageid_t pageid, pageid_t count);
/**
    Get a page from cache.  This function should never block on I/O.

    @return a pointer to the page, or NULL if the page is not in cache, or is being read from disk.
 */
Page * getCachedPage(int xid, const pageid_t pageid);

/**
   loadPage acquires a lock when it is called, effectively pinning it
   in memory.  releasePage releases this lock.
*/
void releasePage(Page *p);
/**
 * Switch the buffer manager into / out of redo mode.  Redo mode forces loadUnintializedPage() to behave like loadPage().
 */
void stasis_buffer_manager_set_redo_mode(int in_redo);

typedef struct stasis_buffer_manager_t stasis_buffer_manager_t;
typedef void* stasis_buffer_manager_handle_t;
struct stasis_buffer_manager_t {
  /** Open a 'handle' to this buffer manager.  Returns NULL on failure, non-zero otherwise */
  stasis_buffer_manager_handle_t* (*openHandleImpl)(stasis_buffer_manager_t*, int is_sequential);
  int    (*closeHandleImpl)(stasis_buffer_manager_t*, stasis_buffer_manager_handle_t*);
  Page * (*loadPageImpl)(stasis_buffer_manager_t*, stasis_buffer_manager_handle_t* h, int xid, pageid_t pageid, pagetype_t type);
  Page * (*loadUninitPageImpl)(stasis_buffer_manager_t*, int xid, pageid_t pageid);
  void   (*prefetchPages)(stasis_buffer_manager_t*, pageid_t pageid, pageid_t count);
  int    (*preallocatePages)(stasis_buffer_manager_t*, pageid_t pageid, pageid_t count);
  Page * (*getCachedPageImpl)(stasis_buffer_manager_t*, int xid, const pageid_t pageid);
  void   (*releasePageImpl)(stasis_buffer_manager_t*, Page * p);
  /**
      This is used by truncation to move dirty pages from Stasis cache
      into the operating system cache.  Once writeBackPage(p) returns,
      calling forcePages() will synchronously force page number p to
      disk.

      (Not all buffer managers support synchronous writes to stable
       storage.  For compatibility, such buffer managers should ignore
       this call.)

      This function may block instead of returning EBUSY.  Therefore, if the page
      may be pinned by some other (or this thread), call tryToWriteBackPage instead.

       @return 0 on success, ENOENT if the page is not in cache, and EBUSY if the page is pinned.
  */
  int    (*writeBackPage)(stasis_buffer_manager_t*, pageid_t p);
  /**
   *  This function is like writeBackPage, except that it will never block due
   *  because the page is pinned.  However, it may sometimes fail to write the page,
   *  and instead return EBUSY, even if the page is not pinned. Therefore, this
   *  method is appropriate for performance hints and log truncation, but not
   *  FORCE mode transactions.
   */
  int    (*tryToWriteBackPage)(stasis_buffer_manager_t*, pageid_t p);
  /**
      Force any written back pages to disk.

      @see writeBackPage for more information.

      If the buffer manager doesn't support stable storage, this call is
      a no-op.
  */
  void   (*forcePages)(struct stasis_buffer_manager_t*, stasis_buffer_manager_handle_t *h);
  /**
      Asynchronously force pages to disk.

      More concretely, this call blocks until the last call to asyncForcePages
      has finished writing blocks to disk, schedules pages for writeback, and
      (usually) immediately returns.

      For various reasons, this is not useful for data integrity, but is
      instead useful as a performance hint.

      This function is currently implemented using sync_file_range(2).  See its
      manpage for a discussion of the limitations of this primitive.
   */
  void   (*asyncForcePages)(struct stasis_buffer_manager_t*, stasis_buffer_manager_handle_t *h);
  /**
      Force written back pages that fall within a particular range to disk.

      This does not force page that have not been written to with pageWrite().

      @param start the first pageid to be forced to disk
      @param stop the page after the last page to be forced to disk.
  */
  void   (*forcePageRange)(struct stasis_buffer_manager_t*, stasis_buffer_manager_handle_t *h, pageid_t start, pageid_t stop);
  void   (*stasis_buffer_manager_simulate_crash)(struct stasis_buffer_manager_t*);
  /**
   * Write out any dirty pages.  Assumes that there are no running transactions
   */
  void   (*stasis_buffer_manager_close)(struct stasis_buffer_manager_t*);

  /**
   *  If this is true, then the underlying implementation should read from disk
   *  even if an uninitialized page was requested by the caller.  This is needed
   *  because loadUninitializedPage sometimes has to set the LSN of the page in
   *  question to the current tail of the log, which would prevent future redo
   *  entries from being replayed.
   */
  int in_redo;
  void * impl;
};

#ifdef PROFILE_LATCHES_WRITE_ONLY
#define loadPage(x,y) __profile_loadPage((x), (y), __FILE__, __LINE__)
#define releasePage(x) __profile_releasePage((x))
compensated_function void  __profile_releasePage(Page * p);
compensated_function Page * __profile_loadPage(int xid, pageid_t pageid, char * file, int line);
#endif

END_C_DECLS

#endif
