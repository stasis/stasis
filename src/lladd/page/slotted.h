
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

