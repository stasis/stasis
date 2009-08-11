/*
 * dirtyPageTable.h
 *
 *  Created on: May 18, 2009
 *      Author: sears
 */

#ifndef DIRTYPAGETABLE_H_
#define DIRTYPAGETABLE_H_

BEGIN_C_DECLS

typedef struct stasis_dirty_page_table_t stasis_dirty_page_table_t;

stasis_dirty_page_table_t * stasis_dirty_page_table_init();
void stasis_dirty_page_table_deinit(stasis_dirty_page_table_t * dirtyPages);

void stasis_dirty_page_table_set_dirty(stasis_dirty_page_table_t * dirtyPages, Page * p);
void stasis_dirty_page_table_set_clean(stasis_dirty_page_table_t * dirtyPages, Page * p);
int  stasis_dirty_page_table_is_dirty(stasis_dirty_page_table_t * dirtyPages, Page * p);

pageid_t stasis_dirty_page_table_dirty_count(stasis_dirty_page_table_t * dirtyPages);

void stasis_dirty_page_table_flush(stasis_dirty_page_table_t * dirtyPages);
lsn_t stasis_dirty_page_table_minRecLSN(stasis_dirty_page_table_t* dirtyPages);

/**
   @todo flushRange's API sucks.  It should be two functions, "startRangeFlush" and "waitRangeFlushes" or something.
 */
void stasis_dirty_page_table_flush_range(stasis_dirty_page_table_t * dirtyPages, pageid_t start, pageid_t stop);

END_C_DECLS

#endif /* DIRTYPAGETABLE_H_ */
