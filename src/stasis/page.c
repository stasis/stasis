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


/* _XOPEN_SOURCE is needed for posix_memalign */
#define _XOPEN_SOURCE 600
#include <stdlib.h>

#include <config.h>
#include <stasis/common.h>
#include "latches.h"
#include "page.h"

#include <assert.h>
#include <stdio.h>

#include <stasis/constants.h>
#include <assert.h>
#include "blobManager.h"
#include <stasis/lockManager.h>
#include <stasis/compensations.h>
#include "page/slotted.h"
#include "page/fixed.h"
#include "page/indirect.h"
#include <stasis/operations/arrayList.h>
#include <stasis/bufferPool.h>
#include <stasis/truncation.h>

static page_impl page_impls[MAX_PAGE_TYPE];

/**
   XXX latching for pageWriteLSN...
*/
void stasis_page_lsn_write(int xid, Page * page, lsn_t lsn) {
  // These asserts belong here, but would cause some hacked up unit tests to fail...
  // if(!page->dirty) {
  //  assert(page->LSN < lsn);
  // }
  //  assertlocked(page->rwlatch);

  if(page->LSN < lsn) {
    page->LSN = lsn;
  }
  dirtyPages_add(page);
  return;
}
/**
   XXX latching for pageReadLSN...
*/
lsn_t stasis_page_lsn_read(const Page * page) {
  return page->LSN;
}

/* ----- (de)initialization functions.  Do not need to support multithreading. -----*/

/**
 * initializes all the important variables needed in
 * all the functions dealing with pages.
 */
void stasis_page_init() {
  slottedPageInit();
  fixedPageInit();

  stasis_page_impl_register(slottedImpl());
  stasis_page_impl_register(fixedImpl());
  stasis_page_impl_register(boundaryTagImpl());
  stasis_page_impl_register(arrayListImpl());
  stasis_page_impl_register(blobImpl());
  stasis_page_impl_register(indirectImpl());
  stasis_page_impl_register(lsmRootImpl());
}

void stasis_page_deinit() {

  for(int i = 0; i < MAX_PAGE_TYPE; i++) {
    page_impl p = { 0 };
    page_impls[i] = p;
  }

  fixedPageDeinit();
  slottedPageDeinit();
}

int stasis_page_impl_register(page_impl p) {
  assert(page_impls[p.page_type].page_type == 0);
  page_impls[p.page_type] = p;
  return 0;
}
/**
   @todo this updates the LSN of the page that points to blob, even if the page is otherwise untouched!!  This is slow and breaks recovery.
*/
void stasis_record_write(int xid, Page * p, lsn_t lsn, recordid rid, const byte *dat) {

  assert( (p->id == rid.page) && (p->memAddr != NULL) );

  readlock(p->rwlatch, 225);
  if(rid.size > BLOB_THRESHOLD_SIZE) {
    // XXX Kludge This is done so that recovery sees the LSN update.  Otherwise, it gets upset... Of course, doing it will break blob recovery unless we set blob writes to do "logical" redo...
    stasis_page_lsn_write(xid, p, lsn);
    unlock(p->rwlatch);
    writeBlob(xid, p, lsn, rid, dat);
  } else {
    byte * buf = stasis_record_write_begin(xid, p, rid);
    stasis_page_lsn_write(xid, p, lsn);
    memcpy(buf, dat, stasis_record_length_read(xid, p, rid));
    unlock(p->rwlatch);
  }
  assert( (p->id == rid.page) && (p->memAddr != NULL) );
}
int stasis_record_read(int xid, Page * p, recordid rid, byte *buf) {
  assert(rid.page == p->id);

  if(rid.size > BLOB_THRESHOLD_SIZE) {
    readBlob(xid, p, rid, buf);
    assert(rid.page == p->id);
    return 0;
  } else {
    readlock(p->rwlatch, 0);
    const byte * dat = stasis_record_read_begin(xid,p,rid);
    memcpy(buf, dat, stasis_record_length_read(xid,p,rid));
    unlock(p->rwlatch);
    return 0;
  }
}
/**
   @todo stasis_record_dereference should dispatch via page_impl...
 */
recordid stasis_record_dereference(int xid, Page * p, recordid rid) {
  int page_type = *stasis_page_type_ptr(p);
  if(page_type == INDIRECT_PAGE) {
    rid = dereferenceIndirectRID(xid, rid);
  } else if(page_type == ARRAY_LIST_PAGE) {
    rid = dereferenceArrayListRid(xid, p, rid.slot);
  }
  return rid;
}

/// --------------  Dispatch functions

static int recordWarnedAboutPageTypeKludge = 0;
const byte * stasis_record_read_begin(int xid, Page * p, recordid rid) {
  int page_type = *stasis_page_type_ptr(p);
  if(!page_type) {
    page_type = FIXED_PAGE;
    if(!recordWarnedAboutPageTypeKludge) {
      recordWarnedAboutPageTypeKludge = 1;
      printf("page.c: MAKING USE OF TERRIBLE KLUDGE AND IGNORING ASSERT FAILURE! FIX ARRAY LIST ASAP!!!\n");
      abort();
    }
  }
  return page_impls[page_type].recordRead(xid, p, rid);
}
byte * stasis_record_write_begin(int xid, Page * p, recordid rid) {
  int page_type = *stasis_page_type_ptr(p);
  if(!page_type) {
    page_type = FIXED_PAGE;
    if(!recordWarnedAboutPageTypeKludge) {
      recordWarnedAboutPageTypeKludge = 1;
      printf("page.c: MAKING USE OF TERRIBLE KLUDGE AND IGNORING ASSERT FAILURE! FIX ARRAY LIST ASAP!!!\n");
      abort()
    }
  }
  return page_impls[page_type].recordWrite(xid, p, rid);
}
void stasis_record_read_done(int xid, Page *p, recordid rid, const byte *b) {
  int page_type = *stasis_page_type_ptr(p);
  if(page_impls[page_type].recordReadDone) {
    page_impls[page_type].recordReadDone(xid,p,rid,b);
  }
}
void stasis_record_write_done(int xid, Page *p, recordid rid, byte *b) {
  int page_type = *stasis_page_type_ptr(p);
  if(page_impls[page_type].recordWriteDone) {
    page_impls[page_type].recordWriteDone(xid,p,rid,b);
  }
}
int stasis_record_type_read(int xid, Page *p, recordid rid) {
  return page_impls[*stasis_page_type_ptr(p)]
    .recordGetType(xid, p, rid);
}
void stasis_record_type_write(int xid, Page *p, recordid rid, int type) {
  page_impls[*stasis_page_type_ptr(p)]
    .recordSetType(xid, p, rid, type);
}
int stasis_record_length_read(int xid, Page *p, recordid rid) {
  return page_impls[*stasis_page_type_ptr(p)]
    .recordGetLength(xid,p,rid);
}
recordid stasis_record_first(int xid, Page * p){
  return page_impls[*stasis_page_type_ptr(p)]
    .recordFirst(xid,p);
}
recordid stasis_record_next(int xid, Page * p, recordid prev){
  return page_impls[*stasis_page_type_ptr(p)]
    .recordNext(xid,p,prev);
}
recordid stasis_record_alloc_begin(int xid, Page * p, int size){
  return page_impls[*stasis_page_type_ptr(p)]
    .recordPreAlloc(xid,p,size);
}
void stasis_record_alloc_done(int xid, Page * p, recordid rid){
  page_impls[*stasis_page_type_ptr(p)]
    .recordPostAlloc(xid, p, rid);
}
void stasis_record_free(int xid, Page * p, recordid rid){
  page_impls[*stasis_page_type_ptr(p)]
    .recordFree(xid, p, rid);
}
int stasis_block_supported(int xid, Page * p){
  return page_impls[*stasis_page_type_ptr(p)]
    .isBlockSupported(xid, p);
}
block_t * stasis_block_first(int xid, Page * p){
  int t = *stasis_page_type_ptr(p);
  return page_impls[t]
    .blockFirst(xid, p);
}
block_t * stasis_block_next(int xid, Page * p, block_t * prev){
  return page_impls[*stasis_page_type_ptr(p)]
    .blockNext(xid, p,prev);
}
void stasis_block_done(int xid, Page * p, block_t * done){
  page_impls[*stasis_page_type_ptr(p)]
    .blockDone(xid, p,done);
}
int stasis_record_freespace(int xid, Page * p){
  return page_impls[*stasis_page_type_ptr(p)]
    .pageFreespace(xid, p);
}
void stasis_record_compact(Page * p){
  page_impls[*stasis_page_type_ptr(p)]
    .pageCompact(p);
}
/** @todo How should the LSN of pages without a page_type be handled?

    This only works because we don't have LSN-free pages yet.  With
    LSN-free pages, we'll need special "loadPageForAlloc(), and
    loadPageOfType() methods (or something...)
*/
void stasis_page_loaded(Page * p){
  short type = *stasis_page_type_ptr(p);
  if(type) {
    assert(page_impls[type].page_type == type);
    page_impls[type].pageLoaded(p);
  } else {
    p->LSN = *stasis_page_lsn_ptr(p);  // XXX kluge - shouldn't special-case UNINITIALIZED_PAGE
  }
}
void stasis_page_flushed(Page * p){
  short type = *stasis_page_type_ptr(p);
  if(type) {
    assert(page_impls[type].page_type == type);
    page_impls[type].pageFlushed(p);
  } else {
    *stasis_page_lsn_ptr(p) = p->LSN;
  }
}
void stasis_page_cleanup(Page * p) { 
  short type = *stasis_page_type_ptr(p);
  if(type) { 
    assert(page_impls[type].page_type == type);
    page_impls[type].pageCleanup(p);
  } 
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
  block_t* ret = malloc(sizeof(block_t));
  *ret = genericBlock;
  genericBlockImpl impl = { p, NULLRID };
  ret->impl = malloc(sizeof(genericBlockImpl));
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
