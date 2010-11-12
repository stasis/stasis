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

#include <stasis/bufferManager.h>
stasis_dirty_page_table_t * stasis_dirty_page_table_init(void);
// XXX circular dependency
void stasis_dirty_page_table_set_buffer_manager(stasis_dirty_page_table_t* dpt, stasis_buffer_manager_t* bm);
void stasis_dirty_page_table_deinit(stasis_dirty_page_table_t * dirtyPages);

void stasis_dirty_page_table_set_dirty(stasis_dirty_page_table_t * dirtyPages, Page * p);
void stasis_dirty_page_table_set_clean(stasis_dirty_page_table_t * dirtyPages, Page * p);
int  stasis_dirty_page_table_is_dirty(stasis_dirty_page_table_t * dirtyPages, Page * p);

pageid_t stasis_dirty_page_table_dirty_count(stasis_dirty_page_table_t * dirtyPages);

int  stasis_dirty_page_table_flush(stasis_dirty_page_table_t * dirtyPages);
int  stasis_dirty_page_table_flush_with_target(stasis_dirty_page_table_t * dirtyPages, lsn_t targetLsn);
lsn_t stasis_dirty_page_table_minRecLSN(stasis_dirty_page_table_t* dirtyPages);

/**
  This method returns a (mostly) contiguous range of the dirty page table for writeback.

  Usage (to non-atomically flush all pages in a range, except ones that were dirtied while we were running)

  int n = 100;
  pageid_t range_starts[n], pageid_t range_ends[n];

  pageid_t next = start;

  while((blocks = stasis_dirty_page_table_get_flush_candidates(dpt, next, stop, n, range_starts, range_ends) {
    for(int i = 0; i < blocks; i++) {
      for(pageid_t p = range_starts[i]; p < range_ends[i]; p++) {
        // flush p
      }
    }
    next = range_ends[blocks-1];;
  }

  @param dirtyPages The dirty page table; this method will not change it.
  @param start The first page to be considered for writeback
  @param stop The page after the last page to be considered for writeback
  @param count The maximum number of pages to be returned for writeback
  @param range_starts An array of pageids of length count.  Some number of these will be populated with the first page in a range to be written back.
  @param range_ends An array of the same length as range_starts.  This will be populated with the page after the last page in each range.

  @return The number of entries in range_starts and range_ends that were populated.  This can be less than count even if there are more dirty pages in the range.  If this is zero, then the entire range is clean.
*/
int stasis_dirty_page_table_get_flush_candidates(stasis_dirty_page_table_t * dirtyPages, pageid_t start, pageid_t stop, int count, pageid_t* range_starts, pageid_t* range_ends);

/**
   @todo flushRange's API sucks.  It should be two functions, "startRangeFlush" and "waitRangeFlushes" or something.
 */
void stasis_dirty_page_table_flush_range(stasis_dirty_page_table_t * dirtyPages, pageid_t start, pageid_t stop);

END_C_DECLS

#endif /* DIRTYPAGETABLE_H_ */
