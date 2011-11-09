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
#ifndef STASIS_PAGE_SLOTTED_H
#define STASIS_PAGE_SLOTTED_H

void stasis_page_slotted_init();
void stasis_page_slotted_deinit();
page_impl stasis_page_slotted_impl();
page_impl stasis_page_boundary_tag_impl();
#endif //STASIS_PAGE_SLOTTED_H
