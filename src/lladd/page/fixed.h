#include "../page.h"

#ifndef __FIXED_H 
#define __FIXED_H

#define recordsize_ptr(page)  shorts_from_end((page), 1)
#define recordcount_ptr(page) shorts_from_end((page), 2)
#define fixed_record_ptr(page, n)   bytes_from_start((page), *recordsize_ptr((page)) * (n))
int recordsPerPage(size_t size);
void fixedPageInitialize(Page * page, size_t size, int count);
/** Return the number of records in a fixed length page */
short fixedPageCount(Page * page);
short fixedPageRecordSize(Page * page);
recordid fixedRawRallocMany(Page * page, int count);
recordid fixedRawRalloc(Page *page);
void fixedRead(Page * page, recordid rid, byte * buf);
void fixedWrite(Page * page, recordid rid, const byte* dat);
void fixedReadUnlocked(Page * page, recordid rid, byte * buf);
void fixedWriteUnlocked(Page * page, recordid rid, const byte* dat);

#endif
