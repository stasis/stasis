/*
 * segmentFile.c
 *
 *  Created on: Jul 7, 2009
 *      Author: sears
 */
#include <stasis/transactional.h>
#include <stasis/bufferManager.h>

#include <string.h>

static inline off_t stasis_offset_from_page(pageid_t pid, int pageoffset) {
  return (pid * PAGE_SIZE) + pageoffset;
}
static inline pageid_t stasis_page_from_offset(off_t off) {
  return off / PAGE_SIZE;
}
static inline int stasis_page_offset_from_offset(off_t off) {
  return off % PAGE_SIZE;
}
static inline off_t stasis_next_page_boundary(off_t off) {
  return 1 + (off/PAGE_SIZE);
}
static inline off_t stasis_min_offset(off_t a, off_t b) {
  return a < b ? a : b;
}
static ssize_t read_write_helper(int read, int xid, lsn_t lsn, byte* buf, size_t count, off_t offset) {
  // the first page that has at least one byte for us on it
  pageid_t start = stasis_page_from_offset(offset);
  // the last page that has such a byte
  pageid_t stop  = stasis_page_from_offset(offset + count - 1);

  // copy the first page
  Page * p = loadPageOfType(xid, start, SEGMENT_PAGE);
  if(read) { readlock(p->rwlatch, 0); } else { writelock(p->rwlatch,0); }
  off_t start_offset = stasis_page_offset_from_offset(offset);
  byte * page_buf = p->memAddr + start_offset;
  byte * user_buf = buf;
  size_t sz = stasis_min_offset(start_offset + count, PAGE_SIZE) - start_offset;
  if(read) {
    memcpy(user_buf, page_buf, sz);
  } else {
    memcpy(page_buf, user_buf, sz);
    stasis_page_lsn_write(xid, p, lsn);
  }
  unlock(p->rwlatch);
  releasePage(p);
  pageid_t n = 0;

  // calculate the number of bytes copied above
  // (assuming there is mbore data to copy; otherwise, this value will not be used)
  off_t buf_phase = PAGE_SIZE - start_offset;

  // copy all pages except for the first and last
  for(pageid_t i = start+1; i < stop-1; i++) {
    p = loadPageOfType(xid, i, SEGMENT_PAGE);
    if(read) { readlock(p->rwlatch, 0); } else { writelock(p->rwlatch,0); }
    page_buf = p->memAddr;
    user_buf = buf + buf_phase + (n * PAGE_SIZE);
    if(read) {
      memcpy(user_buf, page_buf, PAGE_SIZE);
    } else {
      memcpy(page_buf, user_buf, PAGE_SIZE);
      stasis_page_lsn_write(xid, p, lsn);
    }
    unlock(p->rwlatch);
    releasePage(p);
    n++;
  }

  // copy the last page (if necessary)
  if(start != stop) {
    p = loadPage(xid, stop);
    if(read) { readlock(p->rwlatch, 0); } else { writelock(p->rwlatch,0); }
    user_buf = buf + buf_phase + (n * PAGE_SIZE);
    page_buf = p->memAddr;
    sz = count % PAGE_SIZE;
    if(read) {
      memcpy(user_buf, page_buf, sz);
    } else {
      memcpy(page_buf, user_buf, sz);
      stasis_page_lsn_write(xid, p, lsn);
    }
    unlock(p->rwlatch);
    releasePage(p);
  }
  return count;
}

typedef struct {
  off_t offset;
} segment_file_arg_t;

ssize_t Tpread(int xid, byte* buf, size_t count, off_t offset) {
  return read_write_helper(1, xid, -1, buf, count, offset);
}
ssize_t Tpwrite(int xid, const byte * buf, size_t count, off_t offset) {
  byte * buf2 = stasis_malloc(count, byte);

  read_write_helper(1, xid, -1, buf2, count, offset);

  size_t entrylen = sizeof(segment_file_arg_t) + 2*count;
  segment_file_arg_t * entry = (segment_file_arg_t*)malloc(entrylen);
  entry->offset = offset;
  memcpy((entry+1), buf, count);
  memcpy(((byte*)(entry+1))+count, buf2, count);
  free(buf2);
  Tupdate(xid, SEGMENT_PAGEID, entry, entrylen, OPERATION_SEGMENT_FILE_PWRITE);
  return count;
}

static int op_segment_file_pwrite(const LogEntry* e, Page* p) {
  assert(p == 0);
  size_t count = (e->update.arg_size - sizeof(segment_file_arg_t)) / 2;
  const segment_file_arg_t * arg = (const segment_file_arg_t *)stasis_log_entry_update_args_cptr(e);
  off_t offset = arg->offset;
  read_write_helper(0, e->xid, e->LSN, (byte*)(arg+1), count, offset);
  return 0;
}

static int op_segment_file_pwrite_inverse(const LogEntry* e, Page* p) {
  assert(p == 0);
  size_t count = (e->update.arg_size - sizeof(segment_file_arg_t)) / 2;
  const segment_file_arg_t * arg = (const segment_file_arg_t *)stasis_log_entry_update_args_cptr(e);
  off_t offset = arg->offset;
  read_write_helper(0, e->xid, e->LSN, ((byte*)(arg+1))+count, count, offset);
  return 0;
}

stasis_operation_impl stasis_op_impl_segment_file_pwrite(void) {
  static stasis_operation_impl o = {
    OPERATION_SEGMENT_FILE_PWRITE,
    SEGMENT_PAGE,
    OPERATION_SEGMENT_FILE_PWRITE,
    OPERATION_SEGMENT_FILE_PWRITE_INVERSE,
    op_segment_file_pwrite
  };
  return o;
}

stasis_operation_impl stasis_op_impl_segment_file_pwrite_inverse(void) {
  static stasis_operation_impl o = {
    OPERATION_SEGMENT_FILE_PWRITE_INVERSE,
    SEGMENT_PAGE,
    OPERATION_SEGMENT_FILE_PWRITE_INVERSE,
    OPERATION_SEGMENT_FILE_PWRITE,
    op_segment_file_pwrite_inverse
  };
  return o;
}
