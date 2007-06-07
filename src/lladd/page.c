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
#include <lladd/common.h>
#include "latches.h"
#include "page.h"

#include <assert.h>
#include <stdio.h>

#include <lladd/constants.h>
#include <assert.h>
#include "blobManager.h"
#include <lladd/lockManager.h>
#include <lladd/compensations.h>
#include "page/slotted.h"
#include "page/fixed.h"
#include "page/indirect.h"
#include <lladd/operations/arrayList.h>
#include <lladd/bufferPool.h>
#include <lladd/truncation.h>

static page_impl * page_impls;

/**
   XXX latching for pageWriteLSN...
*/
void pageWriteLSN(int xid, Page * page, lsn_t lsn) {
  // These asserts belong here, but would cause some hacked up unit tests to fail...
  // if(!page->dirty) {
  //  assert(page->LSN < lsn);
  // }
  //  assertlocked(page->rwlatch);

  if(page->LSN < lsn) {
    page->LSN = lsn;
    *lsn_ptr(page) = page->LSN;
  }
  dirtyPages_add(page);
  return;
}
/**
   XXX latching for pageReadLSN...
*/
lsn_t pageReadLSN(const Page * page) {
  return page->LSN;
}

/* ----- (de)initialization functions.  Do not need to support multithreading. -----*/

/**
 * pageInit() initializes all the important variables needed in
 * all the functions dealing with pages.
 */
void pageInit() {
  slottedPageInit();
  fixedPageInit();

  page_impls = malloc(MAX_PAGE_TYPE * sizeof(page_impl));
  for(int i = 0; i < MAX_PAGE_TYPE; i++) {
    page_impl p = { 0 };
    page_impls[i] = p;
  }
  registerPageType(slottedImpl());
  registerPageType(fixedImpl());
  registerPageType(boundaryTagImpl());
  registerPageType(arrayListImpl());
}

void pageDeinit() {
  fixedPageDeinit();
  slottedPageDeinit();
}

int registerPageType(page_impl p) {
  assert(page_impls[p.page_type].page_type == 0);
  page_impls[p.page_type] = p;
  return 0;
}
/**
   @todo this updates the LSN of the page that points to blob, even if the page is otherwise untouched!!  This is slow and breaks recovery.
*/
void recordWrite(int xid, Page * p, lsn_t lsn, recordid rid, const byte *dat) {

  assert( (p->id == rid.page) && (p->memAddr != NULL) );

  readlock(p->rwlatch, 225);
  //  page_impl p_impl;
  if(rid.size > BLOB_THRESHOLD_SIZE) {
    // XXX Kludge This is done so that recovery sees the LSN update.  Otherwise, it gets upset... Of course, doing it will break blob recovery unless we set blob writes to do "logical" redo...
    pageWriteLSN(xid, p, lsn);
    unlock(p->rwlatch);
    writeBlob(xid, p, lsn, rid, dat);
  } else {
    /*    p_impl = page_impls[*page_type_ptr(p)];
    if(!*page_type_ptr(p)) {
      // XXX kludge!!!!
      p_impl = page_impls[FIXED_PAGE];
    }
    assert(physical_slot_length(rid.size) == p_impl.recordGetLength(xid, p, rid));
    byte * buf = p_impl.recordWrite(xid, p, rid);
    pageWriteLSN(xid, p, lsn);
    memcpy(buf, dat, physical_slot_length(p_impl.recordGetLength(xid, p, rid))); */

    byte * buf = recordWriteNew(xid, p, rid);
    pageWriteLSN(xid, p, lsn);
    memcpy(buf, dat, recordGetLength(xid, p, rid));
    unlock(p->rwlatch);
  }
  assert( (p->id == rid.page) && (p->memAddr != NULL) );
}
int recordRead(int xid, Page * p, recordid rid, byte *buf) {
  assert(rid.page == p->id);

  if(rid.size > BLOB_THRESHOLD_SIZE) {
    readBlob(xid, p, rid, buf);
    assert(rid.page == p->id);
    return 0;
  } else {
    readlock(p->rwlatch, 0);

    /*    page_impl p_impl;
    int page_type = *page_type_ptr(p);

    if(!page_type) {
      if (! recordReadWarnedAboutPageTypeKludge) {
	recordReadWarnedAboutPageTypeKludge = 1;
	printf("page.c: MAKING USE OF TERRIBLE KLUDGE AND IGNORING ASSERT FAILURE! FIX ARRAY LIST ASAP!!!\n");
      }
      p_impl = page_impls[FIXED_PAGE];
    } else {
      p_impl = page_impls[page_type];
    }
    assert(physical_slot_length(rid.size) == p_impl.recordGetLength(xid, p, rid));
    const byte * dat = p_impl.recordRead(xid, p, rid);
    memcpy(buf, dat, physical_slot_length(p_impl.recordGetLength(xid, p, rid))); */

    const byte * dat = recordReadNew(xid,p,rid);
    memcpy(buf, dat, recordGetLength(xid,p,rid));
    unlock(p->rwlatch);
    return 0;
  }
}

recordid recordDereference(int xid, Page * p, recordid rid) {
  int page_type = *page_type_ptr(p);
  if(page_type == SLOTTED_PAGE || page_type == FIXED_PAGE || (!page_type) || page_type == BOUNDARY_TAG_PAGE ) {

  } else if(page_type == INDIRECT_PAGE) {
    rid = dereferenceRID(xid, rid);
  } else if(page_type == ARRAY_LIST_PAGE) {
    rid = dereferenceArrayListRid(xid, p, rid.slot);
  } else {
    abort();
  }
  return rid;
}

/// --------------  Dispatch functions

static int recordWarnedAboutPageTypeKludge = 0;
const byte * recordReadNew(int xid, Page * p, recordid rid) {
  int page_type = *page_type_ptr(p);
  if(!page_type) {
    page_type = FIXED_PAGE;
    if(!recordWarnedAboutPageTypeKludge) {
      recordWarnedAboutPageTypeKludge = 1;
      printf("page.c: MAKING USE OF TERRIBLE KLUDGE AND IGNORING ASSERT FAILURE! FIX ARRAY LIST ASAP!!!\n");
    }
  }
  return page_impls[page_type].recordRead(xid, p, rid);
}
byte * recordWriteNew(int xid, Page * p, recordid rid) {
  int page_type = *page_type_ptr(p);
  if(!page_type) {
    page_type = FIXED_PAGE;
    if(!recordWarnedAboutPageTypeKludge) {
      recordWarnedAboutPageTypeKludge = 1;
      printf("page.c: MAKING USE OF TERRIBLE KLUDGE AND IGNORING ASSERT FAILURE! FIX ARRAY LIST ASAP!!!\n");
    }
  }
  return page_impls[page_type].recordWrite(xid, p, rid);
}
int recordGetTypeNew(int xid, Page *p, recordid rid) {
  return page_impls[*page_type_ptr(p)]
    .recordGetType(xid, p, rid);
}
void recordSetTypeNew(int xid, Page *p, recordid rid, int type) {
  page_impls[*page_type_ptr(p)]
    .recordSetType(xid, p, rid, type);
}
int recordGetLength(int xid, Page *p, recordid rid) {
  return page_impls[*page_type_ptr(p)]
    .recordGetLength(xid,p,rid);
}
recordid recordFirst(int xid, Page * p){
  return page_impls[*page_type_ptr(p)]
    .recordFirst(xid,p);
}
recordid recordNext(int xid, Page * p, recordid prev){
  return page_impls[*page_type_ptr(p)]
    .recordNext(xid,p,prev);
}
recordid recordPreAlloc(int xid, Page * p, int size){
  return page_impls[*page_type_ptr(p)]
    .recordPreAlloc(xid,p,size);
}
void recordPostAlloc(int xid, Page * p, recordid rid){
  page_impls[*page_type_ptr(p)]
    .recordPostAlloc(xid, p, rid);
}
void recordFree(int xid, Page * p, recordid rid){
  page_impls[*page_type_ptr(p)]
    .recordFree(xid, p, rid);
}
int pageIsBlockSupported(int xid, Page * p){
  return page_impls[*page_type_ptr(p)]
    .isBlockSupported(xid, p);
}
int pageFreespace(int xid, Page * p){
  return page_impls[*page_type_ptr(p)]
    .pageFreespace(xid, p);
}
void pageCompact(Page * p){
  page_impls[*page_type_ptr(p)]
    .pageCompact(p);
}
void pageLoaded(Page * p){
  page_impls[*page_type_ptr(p)]
    .pageLoaded(p);
}
void pageFlushed(Page * p){
  page_impls[*page_type_ptr(p)]
    .pageFlushed(p);
}
