/************************************************************************
 * @file implementation of variable-sized slotted pages

 STRUCTURE OF A PAGE
<pre>
 +-----------------------------------------+-----------------------+----+
 | DATA SECTION                 +--------->| RID: (PAGE, 0)        |    |
 |          +-----------------+ |          +-----------------------+    |
 |      +-->| RID: (PAGE, 1)  | |                                       |
 |      |   +-----------------+ |                                       |
 |      |                       |                                       |
 |      ----------------+       |        +------------------------------+
 |                      |       |   +--->| RID: (PAGE, n)               |
 |                      |       |   |    +------------------------------+
 |======================================================================|
 |  ^ FREE SPACE        |       |   |                                   |
 |  |                   |       |   |                                   |
 |  +-------------------|-------|---|--------------------+              |
 |                      |       |   |                    |              |
 |        +-------------|-------|---+                    |              |
 |        |             |       |                        |              |
 |    +---|---+-----+---|---+---|---+--------------+-----|------+-------+
 |    | slotn | ... | slot1 | slot0 | num of slots | free space |  ***  |
 +----+-------+-----+-------+-------+--------------+------------+-------+
</pre>

 *** = @see page.h for information on this field.

 NOTE:
   - slots are zero indexed.
   - slots are of implemented as (offset, length)

Slotted page layout:

 END:
	 lsn (4 bytes)
	 type (2 bytes)
	 free space (2 bytes)
	 num of slots (2 bytes)
	 freelist head(2 bytes)
	 slot 0 (4 bytes)
	 slot 1 (4 bytes)
	 ...
	 slot n (4 bytes)
	 ...
	 unused
	 ...
	 record n (x bytes)
	 ...
	 record 0 (y bytes)
	 record 1 (z bytes)

 START

 $Id$

@todo slotted.c Should know that specific record types (like blobs) exist,
		(but should not hardcode information about these types) This
		has been handled, except in slottedPostRalloc...

************************************************************************/

#define SLOTTED_PAGE_OVERHEAD_PER_RECORD (2 * sizeof(short))
#define SLOTTED_PAGE_HEADER_OVERHEAD (3 * sizeof(short))

//#define SLOTTED_PAGE_CHECK_FOR_OVERLAP 1
//#define SLOTTED_PAGE_SKIP_SANITY_CHECKS 1

#ifdef LONG_TEST
#define SLOTTED_PAGE_CHECK_FOR_OVERLAP 1
#endif

/**
 @todo rename and rewrite slotted.h macros as static inline functions.
 */
#define freespace_ptr(page)      stasis_page_int16_ptr_from_end((page), 1)
#define numslots_ptr(page)       stasis_page_int16_ptr_from_end((page), 2)
#define freelist_ptr(page)       stasis_page_int16_ptr_from_end((page), 3)
#define slot_ptr(page, n)        stasis_page_int16_ptr_from_end((page), (2*(n))+4)
#define slot_length_ptr(page, n) stasis_page_int16_ptr_from_end((page), (2*(n))+5)
#define record_ptr(page, n)      stasis_page_byte_ptr_from_start((page), \
								 *slot_ptr((page), (n)))

#define freespace_cptr(page)      stasis_page_int16_cptr_from_end((page), 1)
#define numslots_cptr(page)       stasis_page_int16_cptr_from_end((page), 2)
#define freelist_cptr(page)       stasis_page_int16_cptr_from_end((page), 3)
#define slot_cptr(page, n)        stasis_page_int16_cptr_from_end((page), (2*(n))+4)
#define slot_length_cptr(page, n) stasis_page_int16_cptr_from_end((page), (2*(n))+5)
#define record_cptr(page, n)      stasis_page_byte_cptr_from_start((page), \
								   *slot_cptr((page), (n)))

void slottedPageInit();
void slottedPageDeinit();
page_impl slottedImpl();
page_impl boundaryTagImpl();
