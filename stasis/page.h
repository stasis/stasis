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
 * interface for dealing with generic, LSN based pages
 *
 * This file provides a re-entrant interface for pages that are labeled
 * with an LSN and a page type.
 *
 * @ingroup PAGE_FORMATS
 *
 * $Id$
 */

/**
   @defgroup PAGE_FORMATS Page layouts

   Stasis allows developers to define their own on-disk page formats.
   Currently, each page format must end with a hard-coded header
   containing an LSN and a page type.  (This restriction will be
   removed in the future.)

   This section explains how new page formats can be implemented in
   Stasis, and documents the currently available page types.

   A number of callbacks are invoked on existing pages as they are read
   from disk, flushed back, and ultimately, evicted from cache:

    -# stasis_page_loaded() is invoked when the page is read from disk.  It
       should set the Page::LSN field appropriately, and
       perhaps allocate any data structures that will be stored in the
       Page::impl field.
    -# stasis_page_flushed() is invoked when a dirty page is written back to
       disk.  It should make sure that all updates are applied to the
       physical representation of the page.  (Implementations of this
       callback usually copy the Page::LSN field into the page header.)
    -# stasis_page_cleanup() is invoked before a page is evicted from cache.
       It should free any memory associated with the page, such as
       that allocated by stasis_page_loaded(), or pointed to by Page::impl.

   When an uninitialized page is read from disk, Stasis has no way of
   knowing which stasis_page_loaded() callback should be invoked.  Therefore,
   when a new page is initialized the page initialization code should
   perform the work that would normally be performed by stasis_page_loaded().
   Similarly, before a page is freed (and therefore, will be treated as
   uninitialized data) stasis_page_cleanup() should be called.

   Page implementations are free to define their own access methods
   and APIs.  However, Stasis's record oriented page interface
   provides a default set of methods for page access. 
   
   @see PAGE_RECORD_INTERFACE

   @todo Page deallocators should call stasis_page_cleanup()
   @todo Create variant of loadPage() that takes a page type
   @todo Add support for LSN free pages.

 */
/*@{*/
#ifndef __PAGE_H__
#define __PAGE_H__

#include <stasis/common.h>
#include <stasis/latches.h>
#include <stasis/transactional.h>

BEGIN_C_DECLS

/**
    The page type contains in-memory information about pages.  This
    information is used by Stasis to track the page while it is in
    memory, and is never written to disk.

    @todo Page_s shouldn't hardcode doubly linked lists.

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
      LSN field to determine whether a particular operation has
      completed on the page.

      The write lock is used to block all writers (other than the one
      holding the page), and to ensure that all updates with LSN less
      than or equal to the page's LSN have been applied.  Therefore,
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
  /**
      Page type implementations may store whatever they'd like in this
      pointer.  It persists from each call to stasis_page_loaded() to the
      subsequent call to stasis_page_flushed().
  */
  void * impl;
};
/*@}*/

/**
   @defgroup PAGE_HEADER Default page header

   Most Stasis pages contain an LSN and a page type.  These are used
   by recovery to determine whether or not to perform redo.  At
   run time, the page type is used to decide which page implementation
   should manipulate the page.

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
/*@{*/
static inline lsn_t* stasis_page_lsn_ptr(Page *p) {
  return ((lsn_t*)(&(p->memAddr[PAGE_SIZE])))-1;
}
static inline const lsn_t* stasis_page_lsn_cptr(const Page *p) {
  return ((const lsn_t*)(&(p->memAddr[PAGE_SIZE])))-1;
}

/**
   Returns a pointer to the page's type.  This information is stored with the LSN.
   Stasis uses it to determine which page implementation should handle each
   page.

   @param p Any page that contains an LSN header.
   @see stasis_page_impl_register
   @todo Need to typedef page_type_t
 */
static inline int* stasis_page_type_ptr(Page *p) {
  return ((int*)stasis_page_lsn_ptr(p))-1;
}
static inline const int* stasis_page_type_cptr(const Page *p) {
  return ((const int*)stasis_page_lsn_cptr(p))-1;
}

/**
 * assumes that the page is already loaded in memory.  It takes as a
 * parameter a Page.  The Page struct contains the new LSN and the
 * page number to which the new LSN must be written to.  Furthermore,
 * this function updates the dirtyPages table, if necessary.  The
 * dirtyPages table is needed for log truncation.  (If the page->id is
 * null, this function assumes the page is not in the buffer pool, and
 * does not update dirtyPages.  Similarly, if the page is already
 * dirty, there is no need to update dirtyPages.
 *
 * @param xid The transaction that is writing to the page, or -1 if
 * outside of a transaction.
 *
 * @param page You must have a writelock on page before calling this
 * function.
 *
 * @param lsn The new lsn of the page.  If the new lsn is less than
 * the page's current LSN, then the page's LSN will not be changed.
 * If the page is clean, the new LSN must be greater than the old LSN.
 */
void stasis_page_lsn_write(int xid, Page * page, lsn_t lsn);

/**
 * assumes that the page is already loaded in memory.  It takes
 * as a parameter a Page and returns the LSN that is currently written on that
 * page in memory.
 */
lsn_t stasis_page_lsn_read(const Page * page);

/*@}*/

/**
   @defgroup PAGE_UTIL Byte-level page manipulation

   These methods make it easy to manipulate pages that use a standard
   Stasis header (one with an LSN and page type).

   Each one counts bytes from the beginning or end of the page's
   usable space.  Methods with "_cptr_" in their names return const
   pointers (and can accept const Page pointers as arguments).
   Methods with "_ptr_" in their names take non-const pages, and
   return non-const pointers.

   @par Implementing new pointer arithmetic macros

   Stasis page type implementations typically do little more than
   pointer arithmetic.  However, implementing page types cleanly and
   portably is a bit tricky.  Stasis has settled upon a compromise in
   this matter.  Its page file formats are compatible within a single
   architecture, but not across systems with varying lengths of
   primitive types, or that vary in endianness.

   Over time, types that vary in length such as "int", "long", etc
   will be removed from Stasis, but their usage still exists in a few
   places.  Once they have been removed, file compatibility problems
   should be limited to endianness (though application code will still
   be free to serialize objects in a non-portable manner).

   Most page implementations leverage C's pointer manipulation
   semantics to lay out pages.  Rather than casting pointers to
   char*'s and then manually calculating byte offsets using sizeof(),
   the existing page types prefer to cast pointers to appropriate
   types, and then add or subtract the appropriate number of values.

   For example, instead of doing this:

   @code
   // p points to an int, followed by a two bars, then the foo whose address
   // we want to calculate

   int * p;
   foo* f = (foo*)( ((char*)p) + sizeof(int) + 2 * sizeof(bar))
   @endcode

   the implementations would do this:

   @code
   int * p;
   foo * f = (foo*)( ((bar*)(p+1)) + 2 )
   @endcode

   The main disadvantage of this approach is the large number of ()'s
   involved.  However, it lets the compiler deal with the underlying
   multiplications, and often reduces the number of casts, leading to
   slightly more readable code.  Take this implementation of
   stasis_page_type_ptr(), for example:

   @code
   int * stasis_page_type_ptr(Page *p) { 
      return ( (int*)stasis_page_lsn_ptr(Page *p) ) - 1; 
   }
   @endcode

   Here, the page type is stored as an integer immediately before the
   LSN pointer.  Using arithmetic over char*'s would require an extra
   cast to char*, and a multiplication by sizeof(int).

*/
/*@{*/
static inline byte*
stasis_page_byte_ptr_from_start(Page *p, int count) {
  return ((byte*)(p->memAddr))+count;
}
static inline byte*
stasis_page_byte_ptr_from_end(Page *p, int count) {
  return ((byte*)stasis_page_type_ptr(p))-count;
}

static inline int16_t*
stasis_page_int16_ptr_from_start(Page *p, int count) {
  return ((int16_t*)(p->memAddr))+count;
}

static inline int16_t*
stasis_page_int16_ptr_from_end(Page *p, int count) {
  return ((int16_t*)stasis_page_type_ptr(p))-count;
}
static inline int32_t*
stasis_page_int32_ptr_from_start(Page *p, int count) {
  return ((int32_t*)(p->memAddr))+count;
}

static inline int32_t*
stasis_page_int32_ptr_from_end(Page *p, int count) {
  return ((int32_t*)stasis_page_type_ptr(p))-count;
}
// Const methods
static inline const byte*
stasis_page_byte_cptr_from_start(const Page *p, int count) {
  return (const byte*)stasis_page_byte_ptr_from_start((Page*)p, count);
}
static inline const byte*
stasis_page_byte_cptr_from_end(const Page *p, int count) {
  return (const byte*)stasis_page_byte_ptr_from_end((Page*)p, count);
}

static inline const int16_t*
stasis_page_int16_cptr_from_start(const Page *p, int count) {
  return (const int16_t*)stasis_page_int16_ptr_from_start((Page*)p,count);
}

static inline const int16_t*
stasis_page_int16_cptr_from_end(const Page *p, int count) {
  return ((int16_t*)stasis_page_type_cptr(p))-count;
}
static inline const int32_t*
stasis_page_int32_cptr_from_start(const Page *p, int count) {
  return ((const int32_t*)(p->memAddr))+count;
}

static inline const int32_t*
stasis_page_int32_cptr_from_end(const Page *p, int count) {
  return (const int32_t*)stasis_page_int32_ptr_from_end((Page*)p,count);
}


/*@}*/

/**
 * initializes all the global variables needed by the functions
 * dealing with pages.
 *
 * @todo documentation group for page init and deinit?
 */
void stasis_page_init();
/**
 * releases all resources held by the page sub-system.
 */
void stasis_page_deinit();

/**
    @defgroup PAGE_RECORD_INTERFACE Record interface
    @ingroup PAGE_FORMATS

   Page formats define the layout of data on pages.  Currently, all
   pages contain a header with an LSN and a page type in it.  This
   information is used by recovery and the buffer manager to invoke
   callbacks at appropriate times.  (LSN-free pages are currently not
   supported.)

   Stasis' record-oriented page interface uses the page type to determine
   which page implementation should be used to access or modify
   records.  This API's functions begin with "stasis_record".  Two
   commonly used examples are stasis_read_record() and
   stasis_write_record().

   This interface is not re-entrant.  Rather, page implementations
   assume that their callers will latch pages using
   readLock(p->rwlatch) and writeLock(p-rwlatch) before attempting to
   access the page.  A second latch, p->loadlatch, should not be
   acquired by page manipulation code.  Instead, it is used by the
   buffer manager to protect against races during page eviction.

   @par Registering new page type implementations

   Page implementations are registered with Stasis by passing a
   page_impl struct into registerPageType().  page_impl.page_type
   should contain an integer that is unique across all page types,
   while the rest of the fields contain function pointers to the page
   type's implementation.


*/
/*@{*/
static const size_t USABLE_SIZE_OF_PAGE = (PAGE_SIZE - sizeof(lsn_t) - sizeof(int));

/**
   Stasis records carry type information with them.  The type either
   encodes the physical size of opaque data (the common case), or
   indicates that the record should be handled specially (as the
   fixed-length header of a data structure, for example).

   This function maps from record type to record size.

   @param type If positive, a record type is equal to the record size,
   in bytes.  If negative, the type has special significance.  Its
   length is looked up in the SLOT_TYPE_LENGTHS array.

   @return The length of the record in bytes.
 */
static inline size_t 
stasis_record_type_to_size(ssize_t type) {
  return type >= 0 ? type : SLOT_TYPE_LENGTHS[0 - type];
}

/**
 *
 * Write a record.  This call will be dispatched to the proper page implementation.
 *
 * @param xid transaction id
 *
 * @param page a pointer to an in-memory copy of the page as it
 * currently exists.  This copy will be updated by writeRecord.
 *
 * @param lsn the LSN that the updated record will reflect.  This is
 * needed by recovery, and undo.  (The LSN of a page must always
 * increase.  Undos are handled by passing in the LSN of the CLR that
 * records the undo.)
 *
 * @param rid recordid where you want to write
 * @param dat the new value of the record.
 *
 * @todo this updates the LSN of the page that points to blob, even if
 *       the page is otherwise untouched!!  This is slow and breaks
 *       recovery.
 *
 */
void stasis_record_write(int xid, Page * page, lsn_t lsn, recordid rid, const byte *dat);
/**
 * Read a record.  This call will be dispatched to the proper page implementation.
 *
 * @param xid transaction ID
 * @param page a pointer to the pinned page that contains the record.
 * @param rid the record to be written
 * @param dat buffer for data
 * @return 0 on success, Stasis error code on failure
 */
int stasis_record_read(int xid, Page * page, recordid rid, byte *dat);

const byte * stasis_record_read_begin(int xid, Page * p, recordid rid);
byte * stasis_record_write_begin(int xid, Page * p, recordid rid);
void stasis_record_read_done(int xid, Page *p, recordid rid, const byte* buf);
void stasis_record_write_done(int xid, Page *p, recordid rid, byte *buf);
int stasis_record_type_read(int xid, Page * p, recordid rid);
void stasis_record_type_write(int xid, Page * p, recordid rid, int type);
int stasis_record_length_read(int xid, Page *p, recordid rid);
recordid stasis_record_first(int xid, Page * p);
recordid stasis_record_next(int xid, Page * p, recordid prev);
recordid stasis_record_alloc_begin(int xid, Page * p, int size);
void stasis_record_alloc_done(int xid, Page * p, recordid rid);
void stasis_record_free(int xid, Page * p, recordid rid);
int stasis_block_supported(int xid, Page * p);
int stasis_record_freespace(int xid, Page * p);
void stasis_record_compact(Page * p);
void stasis_page_loaded(Page * p);
void stasis_page_flushed(Page * p);
void stasis_page_cleanup(Page * p);
/**
   @todo XXX stasis_record_dereference should be dispatched via page_impl[]
*/
recordid stasis_record_dereference(int xid, Page *p, recordid rid);
/*@}*/
/**
   @param a block returned by stasis_block_first() or stasis_block_next().

   is*() methods return zero for false, non-zero for true.

   recordFixedLen() returns the (single) length of every record in the
   block, or BLOCK_VARIABLE_LENGTH

   methods that take int * size as an argument return a record size by
   setting the pointer (if it's not null).

   @see Abadi, et. al, Integrating Compression and Execution in Column-Oriented Database Systems, VLDB 2006.

*/
typedef struct block_t {
  int    (*isValid)         (struct block_t *b);
  int    (*isOneValue)      (struct block_t *b);
  int    (*isValueSorted)   (struct block_t *b);
  int    (*isPosContig)     (struct block_t *b);
  const byte * (*recordFirst)     (struct block_t *b);
  const byte * (*recordNext)      (struct block_t *b);
  int    (*recordSize)      (struct block_t *b);
  int    (*recordCount)     (struct block_t *b);
  const byte **(*recordPtrArray)  (struct block_t *b);
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
     records might be returned.  Calling stasis_record_next() on { page->id,
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

/**
   This function should work with any valid page implementation, but
   it might be less efficient than a custom implementation.

   This is a convenience function for page implementers.  Other code
   should call stasis_block_first() instead.
*/
block_t *stasis_block_first_default_impl(int xid, Page *p);
/**
   This function should work with any valid page implementation, but
   it might be less efficient than a custom implementation.

   This is a convenience function for page implementers.  Other code
   should call stasis_block_next() instead.
*/
block_t * stasis_block_next_default_impl(int xid, Page *p, block_t *prev);
/**
   This function should work with any valid page implementation, but
   it might be less efficient than a custom implementation.

   This is a convenience function for page implementers.  Other code
   should call stasis_block_done() instead.
*/
void      stasis_block_done_default_impl(int xid, Page *p, block_t *b);

block_t * stasis_block_first(int xid, Page * p);
block_t * stasis_block_next(int xid, Page * p, block_t * prev);
void stasis_block_done(int xid, Page * p, block_t * done);
/**
   None of these functions obtain latches.  Calling them without
   holding rwlatch is an error. (Exception: recordDereference grabs the
   latch for you... XXX does it?)

   The function pointer should be null if your page implementation does
   not support the method in question.

   @todo Figure out what to do about readlock vs writelock...
   @todo Get rid of .size in recordid?

   New allocation sequence (normal operation)

   pin
   latch
   stasis_record_alloc_begin
   stasis_record_alloc_done
   unlatch

   (There is a race here; other transactions can see that the page
   contains a new slot, but that the LSN hasn't been updated.  This
   seems to be benign.  stasis_page_lsn_write() refuses to decrement
   LSN's...  If the lock manager is using LSNs for versioning, it
   might get into a situation where the page has changed (the slot was
   allocated), but the version wasn't bumped.  I can't imagine this
   causing trouble, unless the application is using the presence or
   absence of an uninitialized slot as some sort of side channel....)

   lsn = Tupdate(...)
   latch
   stasis_page_lsn_write
   unlatch
   unpin

   New allocation sequence (recovery)

   pin
   latch
   stasis_record_alloc_done
   stasis_page_lsn_write
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
      this page.  Return null on error.  (XXX current implementations
      abort/crash)
  */
  const byte* (*recordRead)(int xid,  Page *p, recordid rid);
  /**
      Like recordRead, but marks the page dirty, and provides a
      non-const pointer.

      @return a pointer to the buffer manager's copy of the record.
  */
  byte* (*recordWrite)(int xid, Page *p, recordid rid);
  void (*recordReadDone)(int xid, Page *p, recordid rid, const byte *b);
  void (*recordWriteDone)(int xid, Page *p, recordid rid, byte *b);
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
  block_t* (*blockFirst)(int xid, Page *p);
  block_t* (*blockNext)(int xid, Page *p, block_t *prev);
  void (*blockDone)(int xid, Page *p, block_t *done);

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
      should update the estimate returned by page_impl.freespace().

      Depending on the page implementation, this function may have
      other side effects.
  */
  void(*pageCompact)(Page *p);
  /**
      Generate a new, appropriately sized recordid.  This is the first
      of two allocation phases, and does not actually modify the page.
      The caller of this function must call stasis_record_alloc_done() before
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
      @param rid A new recordid that is (usually) from stasis_record_alloc_begin()

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

      indirect.c suggests this should be specific to the page type;
      blobs suggest this should be specific to record type.  Perhaps
      two levels of dereferencing are in order...  (First: page
      specific; second record specific...)
  */
  recordid (*recordDereference)(int xid, Page *p, recordid rid);

  // -------- Page maintenance

  /** This is called when the page is read from disk.

      This function should set p->LSN to an appropriate value.

      @todo In order to support "raw" pages, we need a new page read
      method that lets the caller decide which page type should handle
      the call to stasis_page_loaded().

      @todo stasis_page_loaded() should set p->pageType.

      @todo set *stasis_page_type_ptr() to UNINITIALIZED_PAGE when appropriate.

  */
  void (*pageLoaded)(Page * p);
  /** This is called before the page is written back to disk.

      This function should record p->LSN somewhere appropriate
      (perhaps via stasis_page_lsn_ptr()), and should prepare the page
      to be written back to disk.
  */
  void (*pageFlushed)(Page * p);
  /** This is called before the page is evicted from memory.

      At this point the page has already been written back to disk
      (if necessary).  Any resources held on behalf of this page
      should be released.
   */
  void (*pageCleanup)(Page * p);
} page_impl;

/**
   Register a new page type with Stasis.  The pageType field of impl
   will be checked for uniqueness.  If the type is not unique, this
   function will return non-zero.

   (Since error handling isn't really working yet, it aborts if the
    page type is not unique.)

*/
int stasis_page_impl_register(page_impl impl);

// --------------------  Page specific, general purpose methods

/**
    Initialize a new page

    @param p The page that will be turned into a new slotted page.
	     Its contents will be overwritten.  It was probably
	     returned by loadPage()
 */
void stasis_slotted_initialize_page(Page * p);
void stasis_fixed_initialize_page(Page * page, size_t size, int count);
void stasis_indirect_initialize_page(Page * p, int height);
int stasis_fixed_records_per_page(size_t size);
void stasis_blob_initialize_page(Page * p);
END_C_DECLS

#endif
