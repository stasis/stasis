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
 * interface for dealing with generic, lsn based pages
 *
 * This file provides a re-entrant interface for pages that are labeled
 * with an LSN and a page type.
 *
 * @ingroup LLADD_CORE
 * $Id$
 * 

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

#include <lladd/common.h>
#include "latches.h"
#include <lladd/transactional.h>

BEGIN_C_DECLS

/** 
    The page type contains in-memory information about pages.  This
    information is used by LLADD to track the page while it is in
    memory, and is never written to disk.

    In particular, our current page replacement policy requires two doubly
    linked lists, 

    @todo The Page struct should be tuned for better memory utilization.

    @todo Remove next and prev from Page_s
*/
struct Page_s {
  pageid_t id;
  lsn_t LSN;
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

#define lsn_ptr(page)                   (((lsn_t*)(&((page)->memAddr[PAGE_SIZE])))-1)
#define page_type_ptr(page)             (((int*)lsn_ptr((page)))-1)
#define end_of_usable_space_ptr(page)   page_type_ptr((page))

#define shorts_from_end(page, count)  (((short*)end_of_usable_space_ptr((page)))-(count))
#define bytes_from_start(page, count) (((byte*)((page)->memAddr))+(count))
#define ints_from_start(page, count)  (((int*)((page)->memAddr))+(count))
#define ints_from_end(page, count)    (((int*)end_of_usable_space_ptr((page)))-(count))

#define decode_size(size) (((size) >= SLOT_TYPE_BASE) ? SLOT_TYPE_LENGTHS[(size)-SLOT_TYPE_BASE] : (size))

#define USABLE_SIZE_OF_PAGE (PAGE_SIZE - sizeof(lsn_t) - sizeof(int))

#define physical_slot_length(size) ((size) >= 0 ? (size) : SLOT_TYPE_LENGTHS[-1*size])

/**
 * initializes all the global variables needed by the functions
 * dealing with pages.
 */
void pageInit();
/**
 * releases all resources held by the page sub-system.
 */
void pageDeinit();

/**
 * assumes that the page is already loaded in memory.  It takes as a
 * parameter a Page.  The Page struct contains the new LSN and the
 * page number to which the new LSN must be written to.  Furthermore,
 * this function updates the dirtyPages table, if necessary.  The
 * dirtyPages table is needed for log truncation.  (If the page->id is
 * null, this function assumes the page is not in the buffer pool, and
 * does not update dirtyPages.  Similarly, if the page is already
 * dirty, there is no need to udpate dirtyPages.
 *
 * @param xid The transaction that is writing to the page, or -1 if
 * outside of a transaction.
 *
 * @param page You must have a writelock on page before calling this
 * function.
 *
 * @param lsn The new lsn of the page.  If the new lsn is less than
 * the page's current lsn, then the page's lsn will not be changed.
 * If the page is clean, the new lsn must be greater than the old lsn.
 */
void pageWriteLSN(int xid, Page * page, lsn_t lsn);

/**
 * assumes that the page is already loaded in memory.  It takes
 * as a parameter a Page and returns the LSN that is currently written on that
 * page in memory.
 */
lsn_t pageReadLSN(const Page * page);

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
 * @return 0 on success, lladd error code on failure
 *
 * @deprecated Unnecessary memcpy()'s
 */
void recordWrite(int xid, Page * page, lsn_t lsn, recordid rid, const byte *dat); 
/**
 * @param xid transaction ID
 * @param page a pointer to the pinned page that contains the record.
 * @param rid the record to be written
 * @param dat buffer for data
 * @return 0 on success, lladd error code on failure
 * @deprecated Unnecessary memcpy()'s
 */
int recordRead(int xid, Page * page, recordid rid, byte *dat);

const byte * recordReadNew(int xid, Page * p, recordid rid);
byte * recordWriteNew(int xid, Page * p, recordid rid);
int recordGetTypeNew(int xid, Page * p, recordid rid);
void recordSetTypeNew(int xid, Page * p, recordid rid, int type);
int recordGetLength(int xid, Page *p, recordid rid);
recordid recordFirst(int xid, Page * p);
recordid recordNext(int xid, Page * p, recordid prev);
recordid recordPreAlloc(int xid, Page * p, int size);
void recordPostAlloc(int xid, Page * p, recordid rid);
void recordFree(int xid, Page * p, recordid rid);
int pageIsBlockSupported(int xid, Page * p);
int pageFreespace(int xid, Page * p);
void pageCompact(Page * p);
void pageLoaded(Page * p);
void pageFlushed(Page * p);

/**
   @return -1 if the field does not exist, the size of the field otherwise (the rid parameter's size field will be ignored).
 */
int recordSize(int xid, Page * p, recordid rid);
/**
   @todo recordDereference doesn't dispatch to pages.  Should it? 
*/
recordid recordDereference(int xid, Page *p, recordid rid);

/**
   @param a block returned by blockFirst() or blockNext().

   is*() methods return zero for false, non-zero for true.

   recordFixedLen() returns the (single) length of every record in the
   block, or BLOCK_VARIABLE_LENGTH

   methods that take int * size as an argument return a record size by 
   setting the pointer (if it's not null).

   @see Abadi, et. al, Intergrating Compression and Execution in Column-Oriented Database Systems, VLDB 2006.

*/
typedef struct block_t {
  int    (*isValid)         (struct block_t *b);
  int    (*isOneValue)      (struct block_t *b);
  int    (*isValueSorted)   (struct block_t *b);
  int    (*isPosContig)     (struct block_t *b);
  byte * (*recordFirst)     (struct block_t *b, int *size);
  byte * (*recordNext)      (struct block_t *b, int *size);
  int    (*recordCount)     (struct block_t *b);
  byte * (*recordPtrArray)  (struct block_t *b);
  int  * (*recordSizeArray) (struct block_t *b);
  // These two are not in paper
  int    (*recordFixedLen)  (struct block_t *b);
  /**
     This returns a pointer to an array that contains the records in
     the page.  It only makes sense for pages that many values of the
     same length, and that can implement this more efficiently than
     repeated calls to recordNext.

     @param block the block
     @param count will be set to the number of slots in the page
     @param stride will be set to the size of each value in the page

     @return a pointer to an array of record contents.  The array is
     ordered according to slot id.  The page implementation manages
     the memory; hopefully, this points into the buffer manager, and
     this function call is O(1).  If it would be expensive to return a
     packed array of every record in the page, then only some of the
     records might be returned.  Calling recordNext on { page->id,
     off+ count } will tell the caller if it's received the entire
     page's contents.

     @todo Should recordArray be forced to return records in slot order?
  */
  byte * (*recordPackedArray)     (struct block_t *b, int * count, int * stride);
  /**
     This must be called when you're done with the block (before
     releasePage) */
  void (*blockRelease)(struct block_t *b);
  void * impl;
} block_t;

block_t pageBlockFirst(int xid, Page * p);
block_t pageBlockNext(int xid, Page * p, block_t prev);

/**
   None of these functions obtain latches.  Calling them without
   holding rwlatch is an error. (Exception: dereferenceRid grabs the
   latch for you...)

   The function pointer should be null if your page implementaion does
   not support the method in question.

   @todo Figure out what to do about readlock vs writelock...
   @todo Get rid of .size in recordid?

   New allocation sequence (normal operation)

   pin
   latch
   recordPreAlloc
   recordPostAlloc
   unlatch

   (There is a race here; other transactions can see that the page
   contains a new slot, but that the LSN hasn't been updated.  This
   seems to be benign.  writeLSN refuses to decrement LSN's...  If the
   lock manager is using LSNs for versioning, it might get into a
   situation where the page has changed (the slot was allocated), but
   the version wasn't bumped.  I can't imagine this causing trouble,
   unless the application is using the presense or absence of an
   uninitialized slot as some sort of side channel....)

   lsn = Tupdate(...)
   latch
   writeLSN
   unlatch
   unpin

   New allocation sequence (recovery)

   pin
   latch
   recordPostAlloc
   writeLSN
   unlatch
   unpin
*/
typedef struct page_impl {
  int page_type;

  // ---------- Record access

  /**

      @param size If non-null, will be set to the size of the record
      that has been read.

      @return pointer to read region.  The pointer will be guaranteed
      valid while the page is read latched by this caller, or while
      the page is write latched, and no other method has been called on
      this page.
  */
  const byte* (*recordRead)(int xid,  Page *p, recordid rid);
  /**
      Like recordRead, but marks the page dirty, and provides a
      non-const pointer.

      @return a pointer to the buffer manager's copy of the record.
  */
  byte* (*recordWrite)(int xid, Page *p, recordid rid);
  /**
      Check to see if a slot is a normal slot, or something else, such
      as a blob.  This is stored in the size field in the slotted page
      structure.

      @param p the page of interest
      @param slot the slot in p that we're checking.
      @return The type of this slot.
  */
  int (*recordGetType)(int xid, Page *p, recordid rid);
  /**
     Change the type of a slot.  Doing so must not change the record's
     type.
  */
  void (*recordSetType)(int xid, Page *p, recordid rid, int type);
  /**
     @return the length of a record in bytes.  This value can be used
     to safely access the pointers returned by page_impl.recordRead()
     and page_impl.recordWrite()
   */
  int (*recordGetLength)(int xid, Page *p, recordid rid);
  /**
     @param p the page that will be iterated over
     @return the first slot on the page (in slot order), or NULLRID if the page is empty.
  */
  recordid (*recordFirst)(int xid, Page *p);
  /**
      This returns the next slot in the page (in slot order).

      @param rid If NULLRID, start at the beginning of the page.
      @return The next record in the sequence, or NULLRID if done.
  */
  recordid (*recordNext)(int xid, Page *p, recordid rid);

  // -------- "Exotic" (optional) access methods.

  /*
    @return non-zero if this page implementation supports the block API
  */
  int        (*isBlockSupported)(int xid, Page *p);
  /** Returns a block from this page.  For some page implementations,
      this is essentially the same as recordRead.  Others can produce
      more sophisticated blocks.

  */
  block_t (*blockFirst)(int xid, Page *p);
  block_t (*blockNext)(int xid, Page * p, block_t prev);


  // -------- Allocation methods.


  /**
      This returns a lower bound on the amount of freespace in the
      page

      @param p the page whose freespace will be estimated.
      @return The number of bytes of free space on the page, or (for
              efficiency's sake) an underestimate.

  */
  int (*pageFreespace)(int xid, Page * p);
  /**
      Compact the page in place.  This operation should not change the
      slot id's allocated to the records on the page.  Instead, it
      should update the extimate returned by page_impl.freespace().

      Depending on the page implementation, this function may have
      other side effects.
  */
  void(*pageCompact)(Page *p);
  /**
      Generate a new, appropriately sized recordid.  This is the first
      of two allocation phases, and does not actually modify the page.
      The caller of this function must call recordPostAlloc() before
      unlatching the page.

      @see page_impl.recordPostAlloc()
      @see Talloc() for an overview of allocation

      @param xid The active transaction.
      @param size The size of the new record

      @return A new recordid, or NULLRID if there is not enough space
  */
  recordid (*recordPreAlloc)(int xid, Page *p, int size);
  /** Allocate a new record with the supplied rid.  This is the second
      of two allocation phases.  The supplied record must use an
      unoccupied slot, and there must be enough freespace on the page.

      @param xid The active transaction
      @param p The page that will be allocated from
      @param rid A new recordid that is (usually) from recordPreAlloc()

      @see Talloc(), page_impl.recordPreAlloc()
  */
  void (*recordPostAlloc)(int xid, Page *p, recordid rid);
  /** Free a record.  The page implementation doesn't need to worry
      about uncommitted deallocations; that is handled by a higher
      level.

      @param xid The active transaction
      @param page The page that contains the record that will be freed.
      @param rid the recordid to be freed.

      @see Tdealloc() for example usage
   */
  void (*recordFree)(int xid, Page *p, recordid rid);
  /**
      @todo Is recordDereference a page-implementation specific
      operation, or should it be implemented once and for all in
      page.c?

      indirect.c suggets this should be specfic to the page type;
      blobs suggest this should be specific to record type.  Perhaps
      two levels of dereferencing are in order...  (First: page
      specific; second record specific...)
  */
  recordid (*recordDereference)(int xid, Page *p, recordid rid);

  // -------- Page maintenance

  /** This is called (exactly once) right after the page is read from
      disk.

      This function should set p->LSN to an appropriate value.

      @todo Arrange to call page_impl.loaded() and page_impl.flushed().

  */
  void (*pageLoaded)(Page * p);
  /** This is called (exactly once) right before the page is written
      back to disk.

      This function should record p->LSN somewhere appropriate
  */
  void (*pageFlushed)(Page * p);
} page_impl;

/**
   Register a new page type with Stasis.  The pageType field of impl
   will be checked for uniqueness.  If the type is not unique, this
   function will return non-zero.

   (Since error handling isn't really working yet, it aborts if the
    page type is not unique.)

*/
int registerPageType(page_impl impl);

// --------------------  Page specific, general purpose methods

/**
    Initialize a new page

    @param p The page that will be turned into a new slotted page.
             Its contents will be overwitten.  It was probably
             returned by loadPage()
 */
void slottedPageInitialize(Page * p);
void fixedPageInitialize(Page * page, size_t size, int count);

int fixedRecordsPerPage(size_t size);

void indirectInitialize(Page * p, int height);
compensated_function recordid dereferenceRID(int xid, recordid rid);

END_C_DECLS

#endif
