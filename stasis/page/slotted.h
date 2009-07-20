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

static inline int16_t* stasis_page_slotted_freespace_ptr(Page * p) { return stasis_page_int16_ptr_from_end((p), 1); }
static inline int16_t* stasis_page_slotted_numslots_ptr(Page * p) { return stasis_page_int16_ptr_from_end(p, 2); }
static inline int16_t* stasis_page_slotted_freelist_ptr(Page * p) { return stasis_page_int16_ptr_from_end(p, 3); }
static inline int16_t* stasis_page_slotted_slot_ptr(Page * p, slotid_t n) { return stasis_page_int16_ptr_from_end(p, (2*(n))+4); }
static inline int16_t* stasis_page_slotted_slot_length_ptr(Page * p, slotid_t n) { return stasis_page_int16_ptr_from_end((p), (2*(n))+5); }
static inline byte*    stasis_page_slotted_record_ptr(Page * p, slotid_t n) { return stasis_page_byte_ptr_from_start((p), *stasis_page_slotted_slot_ptr((p), (n))); }

static inline const int16_t* stasis_page_slotted_freespace_cptr(const Page * p) { return stasis_page_slotted_freespace_ptr((Page*)p); }
static inline const int16_t* stasis_page_slotted_numslots_cptr(const Page * p) { return stasis_page_slotted_numslots_ptr((Page*)p); }
static inline const int16_t* stasis_page_slotted_freelist_cptr(const Page * p) { return stasis_page_slotted_freelist_ptr((Page*)p); }
static inline const int16_t* stasis_page_slotted_slot_cptr(const Page * p, slotid_t n) { return stasis_page_slotted_slot_ptr((Page*)p, n); }
static inline const int16_t* stasis_page_slotted_slot_length_cptr(const Page * p, slotid_t n) { return stasis_page_slotted_slot_length_ptr((Page*)p, n); }
static inline const byte* stasis_page_slotted_record_cptr(const Page * p, slotid_t n) { return stasis_page_slotted_record_ptr((Page*)p, n); }

void stasis_page_slotted_init();
void stasis_page_slotted_deinit();
page_impl stasis_page_slotted_impl();
page_impl stasis_page_boundary_tag_impl();
