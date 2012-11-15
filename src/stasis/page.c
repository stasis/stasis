/*---
This software is copyrighted by the Regents of the University of
California, and other parties. The following terms apply to all files
associated with the software unless explicitly disclaimed in
individual files.

The authors hereby grant permission to use, copy, modify, distribute,
and license this software and its documentation for any purpose,
provided that existing copyright notices are retained in all copies
and that this notice is included verbatim in any distributions. No
written agreement, license, or royalty fee is required for any of the
authorized uses. Modifications to this software may be copyrighted by
their authors and need not follow the licensing terms described here,
provided that the new terms are clearly indicated on the first page of
each file where they apply.

IN NO EVENT SHALL THE AUTHORS OR DISTRIBUTORS BE LIABLE TO ANY PARTY
FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES
ARISING OUT OF THE USE OF THIS SOFTWARE, ITS DOCUMENTATION, OR ANY
DERIVATIVES THEREOF, EVEN IF THE AUTHORS HAVE BEEN ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

THE AUTHORS AND DISTRIBUTORS SPECIFICALLY DISCLAIM ANY WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
NON-INFRINGEMENT. THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, AND
THE AUTHORS AND DISTRIBUTORS HAVE NO OBLIGATION TO PROVIDE
MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.

GOVERNMENT USE: If you are acquiring this software on behalf of the
U.S. government, the Government shall have only "Restricted Rights" in
the software and related documentation as defined in the Federal
Acquisition Regulations (FARs) in Clause 52.227.19 (c) (2). If you are
acquiring the software on behalf of the Department of Defense, the
software shall be classified as "Commercial Computer Software" and the
Government shall have only "Restricted Rights" as defined in Clause
252.227-7013 (c) (1) of DFARs. Notwithstanding the foregoing, the
authors grant the U.S. Government and others acting in its behalf
permission to use and distribute the software in accordance with the
terms specified in this license.
---*/

/**

  @file

  Generic page interface.  This file handles updates to the LSN, but
  leaves finer grained concurrency to the implementor of each of the
  page types.  This interface's primary purpose is to wrap common
  functionality together, and to delegate responsibility for page
  handling to other modules.

 Latching summary:

   Each page has an associated read/write lock.  This lock only
   protects the internal layout of the page, and the members of the
   page struct.  Here is how it is held in various circumstances:

   Record allocation:  Write lock
   Record read:        Read lock
   Read LSN            Read lock
   Record write       *READ LOCK*
   Write LSN           Write lock

 Any circumstance where these locks are held during an I/O operation
 is a bug.

*/

#include <stasis/common.h>
#include <stasis/util/latches.h>
#include <stasis/page.h>
#include <stasis/constants.h>
#include <stasis/operations/blobs.h>
#include <stasis/lockManager.h>
#include <stasis/page/slotted.h>
#include <stasis/page/fixed.h>
#include <stasis/page/uninitialized.h>
#include <stasis/experimental/latchFree/lfSlotted.h>
#include <stasis/operations/arrayList.h>
#include <stasis/bufferPool.h>
#include <stasis/truncation.h>

#include <assert.h>

static page_impl page_impls[MAX_PAGE_TYPE];
static stasis_dirty_page_table_t * dirtyPages;

void stasis_page_lsn_write(int xid, Page * page, lsn_t lsn) {
  if(page->LSN < lsn) {
    page->LSN = lsn;
  }

  // XXX probably should be handled by releasePage or something...
  stasis_dirty_page_table_set_dirty(dirtyPages, page);
  return;
}

lsn_t stasis_page_lsn_read(const Page * page) {
  return page->LSN;
}

/* ----- (de)initialization functions.  Do not need to support multithreading. -----*/

/**
 * initializes all the important variables needed in
 * all the functions dealing with pages.
 */
void stasis_page_init(stasis_dirty_page_table_t * dpt) {
  dirtyPages = dpt;
  stasis_page_slotted_init();
  stasis_page_fixed_init();

  stasis_page_impl_register(stasis_page_uninitialized_impl());
  stasis_page_impl_register(stasis_page_slotted_impl());
  stasis_page_impl_register(stasis_page_fixed_impl());
  stasis_page_impl_register(stasis_page_boundary_tag_impl());
  stasis_page_impl_register(stasis_page_array_list_impl());
  stasis_page_impl_register(stasis_page_blob_impl());
  stasis_page_impl_register(slottedLsnFreeImpl());
  stasis_page_impl_register(segmentImpl());
  stasis_page_impl_register(stasis_page_slotted_latch_free_impl());
}

void stasis_page_deinit() {

  for(int i = 0; i < MAX_PAGE_TYPE; i++) {
    page_impl p = { 0 };
    page_impls[i] = p;
  }

  stasis_page_fixed_deinit();
  stasis_page_slotted_deinit();
}

int stasis_page_impl_register(page_impl p) {
  assert(page_impls[p.page_type].page_type == 0);
  page_impls[p.page_type] = p;
  return 0;
}
page_impl * stasis_page_impl_get(int id) {
  assert(page_impls[id].page_type == id);
  return & page_impls[id];
}
void stasis_record_write(int xid, Page * p, recordid rid, const byte *dat) {
  assert( (p->id == rid.page) && (p->memAddr != NULL) );
  assert(rid.size <= BLOB_THRESHOLD_SIZE);

  byte * buf = stasis_record_write_begin(xid, p, rid);
  memcpy(buf, dat, stasis_record_type_to_size(rid.size));
  stasis_record_write_done(xid,p,rid,buf);
  assert( (p->id == rid.page) && (p->memAddr != NULL) );
}
int stasis_record_read(int xid, Page * p, recordid rid, byte *buf) {
  assert(rid.page == p->id);
  assert(rid.size <= BLOB_THRESHOLD_SIZE);

  const byte * dat = stasis_record_read_begin(xid,p,rid);
  memcpy(buf, dat, stasis_record_length_read(xid,p,rid));
  stasis_record_read_done(xid,p,rid,dat);

  return 0;
}
/**
   @todo stasis_record_dereference should dispatch via page_impl...
 */
recordid stasis_record_dereference(int xid, Page * p, recordid rid) {
  int page_type = p->pageType;
  if(page_type == ARRAY_LIST_PAGE) {
    rid = stasis_array_list_dereference_recordid(xid, p, rid.slot);
  }
  return rid;
}

/// --------------  Dispatch functions

const byte * stasis_record_read_begin(int xid, Page * p, recordid rid) {
  int page_type = p->pageType;
  assert(page_type);
  return page_impls[page_type].recordRead(xid, p, rid);
}
byte * stasis_record_write_begin(int xid, Page * p, recordid rid) {
  int page_type = p->pageType;
  assert(page_type);
  if(p->pageType != SLOTTED_LATCH_FREE_PAGE) {
    assert(stasis_record_length_read(xid, p, rid) ==  stasis_record_type_to_size(rid.size));
  }
  return page_impls[page_type].recordWrite(xid, p, rid);
}
void stasis_record_read_done(int xid, Page *p, recordid rid, const byte *b) {
  int page_type = p->pageType;
  if(page_impls[page_type].recordReadDone) {
    page_impls[page_type].recordReadDone(xid,p,rid,b);
  }
}
void stasis_record_write_done(int xid, Page *p, recordid rid, byte *b) {
  int page_type = p->pageType;
  if(page_impls[page_type].recordWriteDone) {
    page_impls[page_type].recordWriteDone(xid,p,rid,b);
  }
}
int stasis_record_type_read(int xid, Page *p, recordid rid) {
  if(page_impls[p->pageType].recordGetType)
    return page_impls[p->pageType].recordGetType(xid, p, rid);
  else
    return INVALID_SLOT;
}
void stasis_record_type_write(int xid, Page *p, recordid rid, int type) {
  page_impls[p->pageType]
    .recordSetType(xid, p, rid, type);
}
int stasis_record_length_read(int xid, Page *p, recordid rid) {
  return page_impls[p->pageType]
    .recordGetLength(xid,p,rid);
}
recordid stasis_record_first(int xid, Page * p){
  return page_impls[p->pageType]
    .recordFirst(xid,p);
}
recordid stasis_record_next(int xid, Page * p, recordid prev){
  return page_impls[p->pageType]
    .recordNext(xid,p,prev);
}
recordid stasis_record_last(int xid, Page * p) {
  return page_impls[p->pageType]
    .recordLast(xid,p);
}
recordid stasis_record_alloc_begin(int xid, Page * p, int size){
  return page_impls[p->pageType]
    .recordPreAlloc(xid,p,size);
}
void stasis_record_alloc_done(int xid, Page * p, recordid rid){
  page_impls[p->pageType]
    .recordPostAlloc(xid, p, rid);
}
void stasis_record_splice(int xid, Page * p, slotid_t first, slotid_t second) {
  page_impls[p->pageType]
    .recordSplice(xid, p, first, second);
}
void stasis_record_free(int xid, Page * p, recordid rid){
  page_impls[p->pageType]
    .recordFree(xid, p, rid);
}
int stasis_block_supported(int xid, Page * p){
  return page_impls[p->pageType]
    .isBlockSupported(xid, p);
}
block_t * stasis_block_first(int xid, Page * p){
  int t = p->pageType;
  return page_impls[t]
    .blockFirst(xid, p);
}
block_t * stasis_block_next(int xid, Page * p, block_t * prev){
  return page_impls[p->pageType]
    .blockNext(xid, p,prev);
}
void stasis_block_done(int xid, Page * p, block_t * done){
  page_impls[p->pageType]
    .blockDone(xid, p,done);
}
int stasis_record_freespace(int xid, Page * p){
  return page_impls[p->pageType]
    .pageFreespace(xid, p);
}
void stasis_record_compact(Page * p){
  page_impls[p->pageType]
    .pageCompact(p);
}
void stasis_record_compact_slotids(int xid, Page * p) {
  page_impls[p->pageType]
    .pageCompactSlotIDs(xid, p);
}

void stasis_uninitialized_page_loaded(int xid, Page * p) {
  /// XXX this should be pushed into the pageLoaded callback, but the
  //  callback would need the xid, and currently is not given the xid.
  lsn_t xid_lsn;
  if(xid == INVALID_XID) {
    xid_lsn = INVALID_LSN;
  } else {
    xid_lsn = stasis_transaction_table_get(stasis_runtime_transaction_table(), xid)->prevLSN;
  }
  lsn_t log_lsn = ((stasis_log_t*)stasis_log())->next_available_lsn(stasis_log());
  // If this transaction has a prevLSN, prefer it.  Otherwise, set the LSN to nextAvailableLSN - 1
  p->LSN = *stasis_page_lsn_ptr(p) = (xid_lsn == INVALID_LSN) ? (log_lsn - 1) : xid_lsn;
  p->pageType = *stasis_page_type_ptr(p) = UNINITIALIZED_PAGE;
  if (page_impls[p->pageType].pageLoaded) page_impls[p->pageType].pageLoaded(p);
}
void stasis_page_loaded(Page * p, pagetype_t type){
  assert(type != UNINITIALIZED_PAGE);
  p->pageType = (type == UNKNOWN_TYPE_PAGE) ? *stasis_page_type_ptr(p) : type;
  assert(page_impls[p->pageType].page_type == p->pageType);  // XXX unsafe; what if the page has no header?
  if(page_impls[p->pageType].has_header) {
    p->LSN = *stasis_page_lsn_cptr(p);
  } else {
    // XXX Need to distinguish between lsn free and header free, then assert(type != UNKNOWN_TYPE_PAGE);
    p->LSN = 0; //XXX estimate LSN.
    static int dragons = 0;
    if(!dragons) {
      fprintf(stderr, "It looks like this program uses segments, which is not stable yet.  Beware of dragons!\n");
      dragons = 1;
    }
  }
  if (page_impls[p->pageType].pageLoaded) page_impls[p->pageType].pageLoaded(p);
}
void stasis_page_flushed(Page * p){

  pagetype_t type = p->pageType;

  assert(page_impls[type].page_type == type);
  if(page_impls[type].has_header) {
    *stasis_page_type_ptr(p)= type;
    *stasis_page_lsn_ptr(p) = p->LSN;
  } else {
    *stasis_page_type_ptr(p)= type; // XXX 'has_header' is a misnomer.

  }
  if(page_impls[type].pageFlushed) page_impls[type].pageFlushed(p);
}
void stasis_page_cleanup(Page * p) {
  short type = p->pageType;
  assert(page_impls[type].page_type == type);
  if(page_impls[type].pageCleanup) page_impls[type].pageCleanup(p);
}

/// Generic block implementations

static int blkTrue(block_t *b) { return 1; }
static int blkFalse(block_t *b) { return 0; }

typedef struct genericBlockImpl {
  Page * p;
  recordid pos;
} genericBlockImpl;

/**
   @todo The block API should pass around xids.
 */
static const byte * blkFirst(block_t * b) {
  genericBlockImpl * impl = b->impl;
  impl->pos = stasis_record_first(-1, impl->p);
  if(! memcmp(&(impl->pos), &(NULLRID), sizeof(recordid))) {
    return 0;
  } else {
    return stasis_record_read_begin(-1, impl->p, impl->pos);
  }
}
static const byte * blkNext(block_t * b) {
  genericBlockImpl * impl = b->impl;
  impl->pos = stasis_record_next(-1, impl->p, impl->pos);
  if(! memcmp(&(impl->pos), &NULLRID, sizeof(recordid))) {
    return 0;
  } else {
    return stasis_record_read_begin(-1, impl->p, impl->pos);
  }
}
static int blkSize(block_t * b) {
  genericBlockImpl * impl = b->impl;
  return stasis_record_type_to_size(impl->pos.size);
}
static void blkRelease(block_t * b) {
  free(b->impl);
  free(b);
}

block_t genericBlock = {
  blkTrue, // isValid
  blkFalse, //isOneValue
  blkFalse, //isValueSorted
  blkFalse, //isPosContig
  blkFirst,
  blkNext,
  blkSize,
  0, //recordCount
  0, //ptrArray can't do pointer array efficiently...
  0, //sizePtrArray
  blkFalse, //recordFixedLen
  0, //packedArray
  blkRelease,
  0
};

block_t* stasis_block_first_default_impl(int xid, Page * p) {
  block_t* ret = stasis_alloc(block_t);
  *ret = genericBlock;
  genericBlockImpl impl = { p, NULLRID };
  ret->impl = stasis_alloc(genericBlockImpl);
  *(genericBlockImpl*)(ret->impl) = impl;
  return ret;
}
block_t* stasis_block_next_default_impl(int xid, Page *p, block_t *prev) {
  stasis_block_done_default_impl(xid, p, prev);
  return 0;  // definitely done.
}
void stasis_block_done_default_impl(int xid, Page *p, block_t *b) {
  free(b->impl);
  free(b);
}
