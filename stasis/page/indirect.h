/** 
    @file

    Indirect block implementation.

    On disk layout of indirect blocks:
END
    lsn      (2 bytes)
    type = 2 (2 bytes)
    level    (2 bytes)
    ...

    block1 = {pageid, maxslot} (8 bytes)
    block0 = {pageid, maxslot} (8 bytes)
START

If blockN has pageid = INVALID_SLOT, then block(N-1) is the last
indirect block that has been allocated.

maxslot is the first slot number that would not fit on this page.  (If the slot exists, then it must be on the next page).

The 'level' field indicates how many levels of indirect blocks lie
below this block.  level = 1 means that the pageid's point to 'normal'
pages.  (They may be slotted (type = 1), or provided by some other
implementation).

   @todo Does anything actually use indirect.h?  ArrayList doesn't use it because accesing it is O(log n).

*/

#include <stasis/common.h>
#include "../page.h"

#ifndef  __LLADD_PAGE_INDIRECT_H
#define  __LLADD_PAGE_INDIRECT_H

BEGIN_C_DECLS

#define level_ptr(page)             stasis_page_int16_ptr_from_end((page), 3)

/** @todo indirect.h cannot handle 64 bit file offsets! */
#define page_ptr(page, offset)      stasis_page_pageid_t_ptr_from_start((page), 2*(offset))
#define maxslot_ptr(page, offset)   stasis_page_pageid_t_ptr_from_start((page), 2*(offset)+1)

#define INDIRECT_POINTERS_PER_PAGE (USABLE_SIZE_OF_PAGE / 16)

/** 
    Translates a recordid that points to an indirect block into the
    physical location of the record.
*/
compensated_function recordid dereferenceIndirectRID(int xid, recordid rid);
void indirectInitialize(Page * p, int height);

compensated_function recordid rallocMany(int xid,  int recordSize, int recordCount);
compensated_function int indirectPageRecordCount(int xid, recordid rid);

page_impl indirectImpl();

END_C_DECLS

#endif /*__LLADD_PAGE_INDIRECT_H*/
