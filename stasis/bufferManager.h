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
 * $Id$
 */

#include <stasis/transactional.h>

#ifndef __BUFFERMANAGER_H__
#define __BUFFERMANAGER_H__

BEGIN_C_DECLS

typedef struct Page_s Page_s;
/**
   Page is defined in bufferManager.h as an incomplete type to enforce
   an abstraction barrier between page.h and the rest of the system.
*/
typedef struct Page_s Page;

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

Page * loadUninitializedPage(int xid, pageid_t pageid);

/** 
    This is the function pointer that bufInit sets in order to
    override loadPage.
*/
extern Page * (*loadPageImpl)(int xid, pageid_t pageid);
extern Page * (*loadUninitPageImpl)(int xid, pageid_t pageid);
/**
   loadPage aquires a lock when it is called, effectively pinning it
   in memory.  releasePage releases this lock.
*/
void releasePage(Page *p);

/** 
    This is the function pointer that bufInit sets in order to
    override releasePage.
*/
extern void   (*releasePageImpl)(Page * p);
/**
 * initialize buffer manager
 * @return 0 on success
 * @return error code on failure
 */
/** 
    This is used by truncation to move dirty pages from Stasis cache
    into the operating system cache.  Once writeBackPage(p) returns,
    calling forcePages() will synchronously force page number p to
    disk.

    (Not all buffer managers support synchronous writes to stable
     storage.  For compatibility, such buffer managers should ignore
     this call.)
*/
extern void (*writeBackPage)(Page * p);
/**
    Force any written back pages to disk.
   
    @see writeBackPage for more information.

    If the buffer manager doesn't support stable storage, this call is
    a no-op.
*/
extern void (*forcePages)();
/**
    Force written back pages that fall within a particular range to disk.

    This does not force page that have not been written to with pageWrite().
*/
extern void (*forcePageRange)(pageid_t start, pageid_t stop);
extern void (*simulateBufferManagerCrash)();

int bufInit(int type);
/**
 * will write out any dirty pages, assumes that there are no running
 * transactions
 */
extern void   (*bufDeinit)();

#ifdef PROFILE_LATCHES_WRITE_ONLY
#define loadPage(x,y) __profile_loadPage((x), (y), __FILE__, __LINE__)
#define releasePage(x) __profile_releasePage((x))
compensated_function void  __profile_releasePage(Page * p);
compensated_function Page * __profile_loadPage(int xid, pageid_t pageid, char * file, int line);
#endif


/*compensated_function Page * bufManLoadPage(int xid, int pageid);

void bufManReleasePage(Page * p);

int bufManBufInit();

void bufManBufDeinit();

void setBufferManager(int i); */


END_C_DECLS

#endif
