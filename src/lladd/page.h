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
 *
 * interface for dealing with slotted pages
 *
 * This file provides a re-entrant interface for pages that contain
 * variable-size records.
 *
 * @ingroup LLADD_CORE
 * $Id$
 * 
 * @todo The slotted pages implementation, and the rest of the page
 * structure should be seperated, and each page should have a 'type'
 * slot so that we can implement multiple page types on top of LLADD.

 STRUCTURE OF A GENERIC PAGE
<pre>
 +----------------------------------------------------------------------+
 |                                                                      |
 |  USABLE SPACE                                                        |
 |                                                                      |
 |                                                                      |
 |                                                                      |
 |                                                                      |
 |                                                                      |
 |                                                                      |
 |                                                                      |
 |                                                                      |
 |                                                                      |
 |                                                                      |
 |                                                                      |
 |                                                                      |
 |                                                    +-----------+-----+
 |                                                    | page type | LSN |
 +----------------------------------------------------+-----------+-----+
</pre>
  */

#ifndef __PAGE_H__
#define __PAGE_H__

#include <config.h>
#include <lladd/common.h>
#include "latches.h"
/** @todo page.h includes things that it shouldn't, and page.h should eventually be an installed header. */

#include <lladd/transactional.h>
#include <lladd/bufferManager.h>
#include "../../src/lladd/page.h"


BEGIN_C_DECLS

#define UNINITIALIZED_PAGE  0
#define SLOTTED_PAGE        1
#define INDIRECT_PAGE       2
#define LLADD_HEADER_PAGE   3
#define LLADD_FREE_PAGE     4
#define FIXED_PAGE          5
#define ARRAY_LIST_PAGE     6
#define lsn_ptr(page)                   (((lsn_t *)(&((page)->memAddr[PAGE_SIZE])))-1)
#define page_type_ptr(page)             (((int*)lsn_ptr((page)))-1)
#define end_of_usable_space_ptr(page)   page_type_ptr((page))

#define shorts_from_end(page, count)  (((short*)end_of_usable_space_ptr((page)))-(count))
#define bytes_from_start(page, count) (((byte*)((page)->memAddr))+(count))
#define ints_from_start(page, count)  (((int*)((page)->memAddr))+(count))
#define ints_from_end(page, count)    (((int*)end_of_usable_space_ptr((page)))-(count))

#define USABLE_SIZE_OF_PAGE (PAGE_SIZE - sizeof(lsn_t) - sizeof(int))

#define UNINITIALIZED_RECORD 0
#define BLOB_RECORD          1
#define SLOTTED_RECORD       2
#define FIXED_RECORD         3



/*#define invalidateSlot(page, n) (*slot_ptr((page), (n)) =  INVALID_SLOT)*/

/** 
    The page type contains in-memory information about pages.  This
    information is used by LLADD to track the page while it is in
    memory, and is never written to disk.

    In particular, our current page replacement policy requires two doubly
    linked lists, 

    @todo The Page struct should be tuned for better memory utilization.
*/
struct Page_s {
  /** @todo Shouldn't Page.id be a long? */
  int id;
  /** @todo The Page.LSN field seems extraneous.  Why do we need it? */
  long LSN;
  byte *memAddr;
  byte dirty;
  /** The next item in the replacement policy's queue */
  struct Page_s *next;
  /** The previous item in the replacement policy's queue. */
  struct Page_s *prev; 
  /** Which queue is the page in? */
  int queue; 
  /** Is the page in the cache at all? */
  int inCache;

  /** Used for page-level latching.
      
      Each page has an associated read/write lock.  This lock only
      protects the internal layout of the page, and the members of the
      page struct.  Here is how rwlatch is held in various circumstances:
      
      Record allocation:  Write lock
      Record read:        Read lock
      Read LSN            Read lock
      Record write       *READ LOCK*
      Write LSN           Write lock
      Write page to disk  No lock
      Read page from disk No lock

      Any circumstance where one these locks are held during an I/O
      operation is a bug.  
      
      For the 'no lock' cases, @see loadlatch

  */
  
  rwl * rwlatch;

  /**
      Since the bufferManager re-uses page structs, this lock is used
      to ensure that the page is in one of two consistent states,
      depending on whether a read lock or a write lock is being held.
      If a read lock is held, then the page is managed by the rwlatch
      also defined in this struct.  Therefore, it cannot be read from
      or written to disk.  Furthermore, since we do not impose an
      order on operations, the holder of a readlock may not use the
      lsn field to determine whether a particular operation has
      completed on the page.

      The write lock is used to block all writers (other than the one
      holding the page), and to ensure that all updates with lsn less
      than or equal to the page's lsn have been applied.  Therefore,
      threads that write the page to disk must hold this lock.  Since
      it precludes access by all other threads, a write lock also
      allows the holder to evict the current page, and replace it.

      Examples:
      
      Write page to disk    Write lock
      Read page from disk   Write lock
      Allocate a new record Read lock
      Write to a record     Read lock
      Read from a record    Read lock


      @see rwlatch, getPage(), pageRalloc(), pageRead()
      
  */
  rwl * loadlatch;

};

/**
 * initializes all the global variables needed by the functions
 * dealing with pages.
 */
void pageInit();
/**
 * releases all resources held by the page sub-system.
 */
void pageDeInit();

/**
 * assumes that the page is already loaded in memory.  It takes
 * as a parameter a Page.  The Page struct contains the new LSN and the page
 * number to which the new LSN must be written to.
 *
 * @param page You must have a writelock on page before calling this function.
 * @param lsn The new lsn of the page.  If the new lsn is less than the page's current lsn, then the page's lsn will not be changed.
 */
void pageWriteLSN(int xid, Page * page, lsn_t lsn);

/**
 * assumes that the page is already loaded in memory.  It takes
 * as a parameter a Page and returns the LSN that is currently written on that
 * page in memory.
 */
lsn_t pageReadLSN(const Page * page);

/**
   Sets the record type, if applicable.  Right now, this is only
   really meaningful in the case of slotted pages that store
   information about blobs, but the intention of this function is to
   allow a level of indirection so that the blob implementation and
   slotted page implementation are independent of each other.

   The record type is meant to be a hint to the page implementation,
   so no getRecordType function is provided.  (If the type of record
   does not matter to the page implementation, then it is free to
   ignore this call.)

   @param page A pointer to the page containing the record of interest.
   @param rid The record's id.
   @param slot_type The new type of the record.  (Must be > PAGE_SIZE).
*/
/*void setRecordType(Page * page, recordid rid, int slot_type); */

/**
 * @param xid transaction id @param lsn the lsn that the updated
 * record will reflect.  This is needed by recovery, and undo.  (The
 * lsn of a page must always increase.  Undos are handled by passing
 * in the LSN of the CLR that records the undo.)
 *
 * @param page a pointer to an in-memory copy of the page as it
 * currently exists.  This copy will be updated by writeRecord.
 *
 * @param rid recordid where you want to write 
 *
 * @param dat the new value of the record.
 *
 */
void writeRecord(int xid, Page * page, lsn_t lsn, recordid rid, const void *dat); 
/**
 *  The same as writeRecord, but does not obtain a latch on the page.
 */
void writeRecordUnlocked(int xid, Page * page, lsn_t lsn, recordid rid, const void *dat); 
/**
 * @param xid transaction ID
 * @param rid the record to be written
 * @param dat buffer for data
 */
void readRecord(int xid, Page * page, recordid rid, void *dat);
/**
 *  The same as readRecord, but does not obtain a latch.
 */
void readRecordUnlocked(int xid, Page * p, recordid rid, void *buf);
/**
   Should be called when transaction xid commits.
*/
void pageCommit(int xid);
/**
   Should be called when transaction xid aborts.
*/
void pageAbort(int xid);

Page* pageMalloc();
void  pageRealloc(Page * p, int id);

/** 
    Allocates a single page on disk.  Has nothing to do with pageMalloc.

    @return the pageid of the newly allocated page, which is the
    offset of the page in the file, divided by the page size.
*/
/*int pageAlloc() ;*/
/**
   obtains the type of the record pointed to by rid.  

   @return UNINITIALIZED_RECORD, BLOB_RECORD, SLOTTED_RECORD, or FIXED_RECORD.
*/


int getRecordType(int xid, Page * p, recordid rid);

int getRecordSize(int xid, Page * p, recordid rid);
/**
   same as getRecordType(), but does not obtain a lock.
*/
int getRecordTypeUnlocked(int xid, Page * p, recordid rid);
/**
   return the length of the record rid.  (the rid parameter's size field will be ignored)

   @todo implement getRecordLength for blobs and fixed length pages.

   @return -1 if the field does not exist, the size of the field otherwise.
 */
int getRecordLength(int xid, Page * p, recordid rid);

END_C_DECLS

#endif
