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

************************************************************************/

#define SLOTTED_PAGE_OVERHEAD_PER_RECORD 4
#define SLOTTED_PAGE_HEADER_OVERHEAD 6

void pageWriteRecord(int xid, Page * page, lsn_t lsn, recordid rid, const byte *data);
void pageReadRecord(int xid, Page * page, recordid rid, byte *buff);

/**
 * assumes that the page is already loaded in memory.  It takes as a
 * parameter a Page, and returns an estimate of the amount of free space on this
 * page.  This is either exact, or an underestimate.
 * @todo how should this be handled? */
int freespace(Page * p);

void pageInitialize(Page * p);

#define freespace_ptr(page)      shorts_from_end((page), 1)
#define numslots_ptr(page)       shorts_from_end((page), 2)
#define freelist_ptr(page)       shorts_from_end((page), 3)
#define slot_ptr(page, n)        shorts_from_end((page), (2*(n))+4)
#define slot_length_ptr(page, n) shorts_from_end((page), (2*(n))+5)
#define record_ptr(page, n)      bytes_from_start((page), *slot_ptr((page), (n)))
#define isValidSlot(page, n)   ((*slot_ptr((page), (n)) == INVALID_SLOT) ? 0 : 1)

