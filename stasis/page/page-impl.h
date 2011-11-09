/*
 * page-impl.h
 *
 *  Created on: Nov 7, 2011
 *      Author: sears
 */

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
 * $Id: page.h 1526 2011-06-13 11:26:25Z sears.russell@gmail.com $
 */

#ifndef stasis_page
#define stasis_page_memaddr(p) (p->memAddr)
#define stasis_page(x) stasis_page_##x
#define PAGE Page
#define STASIS_PAGE_IMPL_NEED_UNDEF
#endif

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

/*@{*/
static inline lsn_t* stasis_page(lsn_ptr)(PAGE *p) {
  return ((lsn_t*)(&(stasis_page(memaddr)(p)[PAGE_SIZE])))-1;
}
static inline const lsn_t* stasis_page(lsn_cptr)(const PAGE *p) {
  return ((const lsn_t*)(&(stasis_page(memaddr)(p)[PAGE_SIZE])))-1;
}

/**
   Returns a pointer to the page's type.  This information is stored with the LSN.
   Stasis uses it to determine which page implementation should handle each
   page.

   @param p Any page that contains an LSN header.
   @see stasis_page_impl_register
   @todo Need to typedef page_type_t
 */
static inline int* stasis_page(type_ptr)(PAGE *p) {
  return ((int*)stasis_page(lsn_ptr)(p))-1;
}
static inline const int* stasis_page(type_cptr)(const PAGE *p) {
  return ((const int*)stasis_page(lsn_cptr)(p))-1;
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
void stasis_page(lsn_write)(int xid, PAGE * page, lsn_t lsn);

/**
 * assumes that the page is already loaded in memory.  It takes
 * as a parameter a Page and returns the LSN that is currently written on that
 * page in memory.
 */
lsn_t stasis_page(lsn_read)(const PAGE * page);

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
stasis_page(byte_ptr_from_start)(PAGE *p, int count) {
  return ((byte*)(stasis_page(memaddr)(p)))+count;
}
static inline byte*
stasis_page(byte_ptr_from_end)(PAGE *p, int count) {
  return ((byte*)stasis_page(type_ptr)(p))-count;
}

static inline int16_t*
stasis_page(int16_ptr_from_start)(PAGE *p, int count) {
  return ((int16_t*)(stasis_page(memaddr)(p)))+count;
}

static inline int16_t*
stasis_page(int16_ptr_from_end)(PAGE *p, int count) {
  return ((int16_t*)stasis_page(type_ptr)(p))-count;
}
static inline int32_t*
stasis_page(int32_ptr_from_start)(PAGE *p, int count) {
  return ((int32_t*)(stasis_page(memaddr)(p)))+count;
}

static inline int32_t*
stasis_page(int32_ptr_from_end)(PAGE *p, int count) {
  return ((int32_t*)stasis_page(type_ptr)(p))-count;
}
static inline pageid_t*
stasis_page(pageid_t_ptr_from_start)(PAGE *p, int count) {
  return ((pageid_t*)(stasis_page(memaddr)(p)))+count;
}

static inline pageid_t*
stasis_page(pageid_t_ptr_from_end)(PAGE *p, int count) {
  return ((pageid_t*)stasis_page(type_ptr)(p))-count;
}
// Const methods
static inline const byte*
stasis_page(byte_cptr_from_start)(const PAGE *p, int count) {
  return (const byte*)stasis_page(byte_ptr_from_start)((PAGE*)p, count);
}
static inline const byte*
stasis_page(byte_cptr_from_end)(const PAGE *p, int count) {
  return (const byte*)stasis_page(byte_ptr_from_end)((PAGE*)p, count);
}

static inline const int16_t*
stasis_page(int16_cptr_from_start)(const PAGE *p, int count) {
  return (const int16_t*)stasis_page(int16_ptr_from_start)((PAGE*)p,count);
}

static inline const int16_t*
stasis_page(int16_cptr_from_end)(const PAGE *p, int count) {
  return ((int16_t*)stasis_page(type_cptr)(p))-count;
}
static inline const int32_t*
stasis_page(int32_cptr_from_start)(const PAGE *p, int count) {
  return ((const int32_t*)(stasis_page(memaddr)(p)))+count;
}

static inline const int32_t*
stasis_page(int32_cptr_from_end)(const PAGE *p, int count) {
  return (const int32_t*)stasis_page(int32_ptr_from_end)((PAGE*)p,count);
}
static inline const pageid_t*
stasis_page(pageid_t_cptr_from_start)(const PAGE *p, int count) {
  return ((const pageid_t*)(stasis_page(memaddr)(p)))+count;
}

static inline const pageid_t*
stasis_page(pageid_t_cptr_from_end)(const PAGE *p, int count) {
  return (const pageid_t*)stasis_page(pageid_t_cptr_from_end)(p,count);
}
/*@}*/

// --------------------  Page specific, general purpose methods


/**
    Initialize a new page

    @param p The page that will be turned into a new slotted page.
         Its contents will be overwritten.  It was probably
         returned by loadPage()
 */
void stasis_page(slotted_initialize_page)(PAGE * p);
void stasis_page(slotted_latch_free_initialize_page)(PAGE * page);
void stasis_page(slotted_lsn_free_initialize_page)(Page * p);
void stasis_page(indirect_initialize_page)(Page * p, int height);
void stasis_page(blob_initialize_page)(PAGE * p);

#include "fixed-impl.h"
#include "slotted-impl.h"

#ifdef STASIS_PAGE_IMPL_NEED_UNDEF
#undef stasis_page_memaddr
#undef stasis_page
#undef PAGE
#undef STASIS_PAGE_IMPL_NEED_UNDEF
#endif
