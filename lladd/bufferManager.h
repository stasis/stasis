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
 *
 * @todo Allow error checking!  
 *
 * @todo CORRECTNESS PROBLEM: Recovery assumes that each page is
 * written atomically to disk, and that the LSN field of the page will
 * be in sync with the contents of each record on the page.  Since we
 * use mmap() to read and write the pages, and a page fault could
 * interrupt LLADD between a writeRecord and writeLSN call, it is
 * possible for the operating system to steal dirty pages from the
 * buffer manager.  (We support steal in the normal case, but still
 * need to be sure that the stolen pages are internally consistent!)
 *
 * It looks as though we will need to re-write the buffer manager so
 * that it only uses read or write calls.  Alternatively, we could
 * lock pages as we update them, but that requires root privliges.
 *
 * @todo Refactoring for lock manager / correctness
 *
 * Possible interfaces:
 * 
 


    pageManager - Provides cached page handling, delegates to blob
    manager when necessary.  Doesn't implement an eviction policy.
    That is left to a cacheManager.  (Multiple cacheManagers can be
    used with a single page manager.)

    typedef struct {
       Page * page;
       / * If this page is pinned, what's the maximum lsn that's dirtied it? * /
       lsn_t * max_dirty_lsn = 0;
       / * How many times has this page been pinned using readWriteMap()? * /
       int pin_count;
    } page_metadata_t;


     Calls for user managed memory:

     Read only access to record:

      Cost with cache hit:  memcpy();

      - int readRecord(rid, void *);

     Write record directly:
      Cost with cache hit:  memcpy(rid.size), eventual disk flush;
     
      - int writeRecord(rid, lsn, void *);

     @todo need alloc + free...

//     Calls for LLADD managed memory (returns pointers to LLADD's cache.)

//     Read only access to record (requires an un-pinning)

//     Cost with cache hit:  pointer arithmetic.      

//     - map_t readMapRecord(rid, &((const void *));
      
//     Map a page read / write.  Pins the page, sets its lsn, and provides a pointer to the record: 
//      Cost with cache hit:  pointer arithmetic, eventual disk flush.

//      - map_t readWriteMapRecord(rid, &(void *));
 
//     Unmap a mapped page so that it can be kicked.

//    @param lsn can be 0 if this is a read-only mapping.  Otherwise,
//     it should be the LSN of the operation that calls unmapRecord.
//     @todo What connection between the lock manager and this function
//     is there?  Presumably, unmap should be called when the locks are
//     released...
//     
//      - void unmapRecord(map_t, lsn);

    cachePolicy

       page_id kickPage();  // Choose a page to kick.  May call logFlush() if necessary.
       readPage(page);      // Inform the cache that a page was read.
       writePage(page);     // Inform the cache that a page was written.
       cacheHint(void *);   // Optional method needed to implement dbmin.

    lockManager 

     - These functions provide a locking implementation based on logical operations:

       lock_t lock(Operation o);
       unlock(Operation o);

     - These functions provide a locking implementation based on physical operations:

       (Insert bufferManager API here, but make each call take a xid and a lock_t* parameter)

     Locking functions can return errors such as DEADLOCK, etc.

 *
 * @ingroup LLADD_CORE
 * $Id$
 */

#ifndef __BUFFERMANAGER_H__
#define __BUFFERMANAGER_H__

#include <lladd/page.h>
#include <lladd/constants.h>

/**
 * initialize buffer manager
 * @return 0 on success
 * @return error code on failure
 */
int bufInit();

/**
 * @param pageid ID of the page you want to load
 * @return fully formed Page type
 * @return page with -1 ID if page not found
 */
Page loadPage(int pageid);

/**
 * allocate a record
 * @param xid The active transaction.
 * @param size The size of the new record
 * @return allocated record
 */
recordid ralloc(int xid, size_t size);

/**
 * Find a page with some free space.
 *
 */
 

/**
 * This function updates the LSN of a page.  This is needed by the
 * recovery process to make sure that each action is undone or redone
 * exactly once.
 *
 * @param LSN The new LSN of the page.
 * @param pageid ID of the page you want to write
 *
 * @todo This needs to be handled by ralloc and writeRecord for
 * correctness.  Right now, there is no way to atomically update a
 * page(!)  To fix this, we need to change bufferManager's
 * implementation to use read/write (to prevent the OS from stealing
 * pages in the middle of updates), and alter kickPage to see what the
 * last LSN synced to disk was.  If the log is too far behind, it will
 * need to either choose a different page, or call flushLog().  We may
 * need to implement a special version of fwrite() to do this
 * atomically.  (write does not have to write all of the data that was
 * passed to it...)
 */
void writeLSN(long LSN, int pageid);

/**
 * @param pageid ID of page you want to read
 * @return LSN found on disk
 */
long readLSN(int pageid);

/**
 * @param xid transaction id
 * @param rid recordid where you want to write
 * @param dat data you wish to write
 */
void writeRecord(int xid, recordid rid, const void *dat);

/**
 * @param xid transaction ID
 * @param rid
 * @param dat buffer for data
 */
void readRecord(int xid, recordid rid, void *dat);

/**
 * @param page write page to disk, including correct LSN
 * @return 0 on success
 * @return error code on failure 
 */
int flushPage(Page page);

/*
 * this function does NOT write to disk, just drops the page from the active
 * pages
 * @param page to take out of buffer manager
 * @return 0 on success
 * @return error code on failure
int dropPage(Page page);
 */

/**
 * all actions necessary when committing a transaction. Can assume that the log
 * has been written as well as any other actions that do not depend on the
 * buffer manager
 *
 * Basicly, this call is here because we used to do copy on write, and
 * it might be useful when locking is implemented.
 *
 * @param xid transaction ID
 * @return 0 on success
 * @return error code on failure
 */
int bufTransCommit(int xid);

/**
 * 
 * Currently identical to bufTransCommit.
 * 
 * @param xid transaction ID
 * @return 0 on success
 * @return error code on failure
 */
int bufTransAbort(int xid);

/**
 * will write out any dirty pages, assumes that there are no running
 * transactions
 */
void bufDeinit();

/** @todo Global file descriptors are nasty. */

extern int blobfd0;
extern int blobfd1;


#endif
