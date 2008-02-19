
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

/** @file

    @todo check_page should judiciously avoid stasis_page_lsn_ptr()
*/

#include <config.h>
#include <check.h>

#include "../../src/stasis/page.h"
#include "../../src/stasis/page/indirect.h"
#include "../../src/stasis/page/slotted.h"
#include "../../src/stasis/blobManager.h"
#include <stasis/bufferManager.h>
#include <stasis/transactional.h>

#include "../../src/stasis/latches.h"
#include <sched.h>
#include <assert.h>
#include "../check_includes.h"


#define LOG_NAME   "check_page.log"

#define RECORD_SIZE sizeof(int)

pthread_mutex_t random_mutex;
static lsn_t lsn;
static pthread_mutex_t lsn_mutex;

static void * multiple_simultaneous_pages ( void * arg_ptr) {
  Page * p = (Page*)arg_ptr;
  int i;
  lsn_t this_lsn;
  short j;
  int first = 1;
  int k;
  recordid rid[100];
  
  for(i = 0; i < 10000; i++) {
    pthread_mutex_lock(&lsn_mutex); 
    lsn++;
    this_lsn = lsn;
    assert(stasis_page_lsn_read(p) < this_lsn);
    pthread_mutex_unlock(&lsn_mutex);

    if(! first ) {
      for(k = 0; k < 100; k++) {
	stasis_record_read(1, p, rid[k], (byte*)&j);

	assert((j + 1) ==  i + k);
        writelock(p->rwlatch,0);
        stasis_record_free(-1, p, rid[k]);
        stasis_page_lsn_write(-1, p, this_lsn);
        unlock(p->rwlatch);
	sched_yield();
      }
    } 
    
    first = 0;
    
    for(k = 0; k < 100; k++) {
      writelock(p->rwlatch,0);
      rid[k] = stasis_record_alloc_begin(-1,p,sizeof(short));
      if(rid[k].size == INVALID_SLOT) { // Is rid[k] == NULLRID?
        stasis_record_compact(p);
        rid[k] = stasis_record_alloc_begin(-1,p,sizeof(short));
      }
      stasis_record_alloc_done(-1,p,rid[k]);
      short * buf = (short*)stasis_record_write_begin(-1,p,rid[k]);
      *buf = i+k;
      stasis_record_write_done(-1,p,rid[k],(byte*)buf);
      stasis_page_lsn_write(-1, p, this_lsn);
      assert(stasis_page_lsn_read(p) >= this_lsn);
      unlock(p->rwlatch);
      sched_yield();
    }
    
  }
  
  return NULL;
}


static void* fixed_worker_thread(void * arg_ptr) {
  Page * p = (Page*)arg_ptr;
  int i;
  lsn_t this_lsn;
  int first = 1;
  recordid rid;

  for(i = 0; i < 100; i++) {
    pthread_mutex_lock(&lsn_mutex);
    this_lsn = lsn;
    lsn++;
    pthread_mutex_unlock(&lsn_mutex);

    writelock(p->rwlatch,0);
    if(! first ) {
      int * buf = (int*)stasis_record_read_begin(-1,p,rid);
      assert(((*buf) + 1) ==  i);
      stasis_record_read_done(-1,p,rid,(byte*)buf);
    }
    first = 0;
    rid = stasis_record_alloc_begin(-1, p, sizeof(int));
    stasis_record_alloc_done(-1, p, rid);
    int * buf = (int*)stasis_record_write_begin(-1,p,rid);
    *buf = i;
    stasis_record_write_done(-1,p,rid,(byte*)buf);
    stasis_page_lsn_write(-1, p,lsn);
    assert(stasis_page_lsn_read(p) >= this_lsn);
    unlock(p->rwlatch);
    sched_yield();
  }
  return NULL;
}

static void* worker_thread(void * arg_ptr) {
  Page * p = (Page*)arg_ptr;
  int i;
  lsn_t this_lsn;
  int j;
  int first = 1;
  recordid rid;
  for(i = 0; i < 10000; i++) {
    pthread_mutex_lock(&lsn_mutex);

    this_lsn = lsn;
    lsn++;

    pthread_mutex_unlock(&lsn_mutex);

    if(! first ) {
      stasis_record_read(1, p, rid, (byte*)&j);
      assert((j + 1) ==  i);
      writelock(p->rwlatch,0);
      stasis_record_free(-1, p, rid);
      stasis_page_lsn_write(-1, p, this_lsn);
      unlock(p->rwlatch);
      sched_yield();
    }

    first = 0;

    // @todo In check_page, a condition variable would be more efficient...
    writelock(p->rwlatch,0);
    if(stasis_record_freespace(-1, p) < sizeof(int)) {
      first = 1;
    } else {
      rid = stasis_record_alloc_begin(-1, p, sizeof(int));
      stasis_record_alloc_done(-1, p, rid);
      int * buf = (int*)stasis_record_write_begin(-1, p, rid);
      stasis_page_lsn_write(-1,p,this_lsn);
      *buf = i;
      stasis_record_write_done(-1,p,rid,(byte*)buf);
      assert(stasis_page_lsn_read(p) >= this_lsn);
    }
    unlock(p->rwlatch);
    sched_yield();

  }

  return NULL;
}

/**
   @test 

   just run one the worker_thread function once to make sure that it passes
   without interference from other threads.

   The number of slots allocated by the page tests is too low to check
   that freed space is recovered.

*/
START_TEST(pageNoThreadTest)
{
  Page * p;

  pthread_mutex_init(&lsn_mutex, NULL);
  
  Tinit();
  p = loadPage(-1, 0);
  writelock(p->rwlatch,0);
  memset(p->memAddr, 0, PAGE_SIZE);
  stasis_slotted_initialize_page(p);
  unlock(p->rwlatch);
  worker_thread(p);

  p->LSN = 0;
  *stasis_page_lsn_ptr(p) = p->LSN;

  releasePage(p);

  Tdeinit();

  pthread_mutex_destroy(&lsn_mutex);

}
END_TEST

/**
   @test
*/

START_TEST(pageCheckMacros) {
  Page p;
  byte buffer[PAGE_SIZE];
  memset(buffer, -1, PAGE_SIZE);

  p.memAddr = buffer;

  lsn_t lsn = 5;
  
  *stasis_page_lsn_ptr(&p) = lsn;
  *stasis_page_type_ptr(&p) = 10;
  *freespace_ptr(&p) = 15;
  *numslots_ptr(&p)  = 20;
  *slot_ptr(&p, 0)   = 30;
  *slot_ptr(&p, 1)   = 35;
  *slot_ptr(&p, 40)   = 40;
  *slot_length_ptr(&p, 0)   = 31;
  *slot_length_ptr(&p, 1)   = 36;
  *slot_length_ptr(&p, 40)   = 41;

  *stasis_page_byte_ptr_from_start(&p, 0) = 50;
  *stasis_page_byte_ptr_from_start(&p, 1) = 51;
  *stasis_page_byte_ptr_from_start(&p, 2) = 52;
  *stasis_page_byte_ptr_from_start(&p, 3) = 53;
  *stasis_page_byte_ptr_from_start(&p, 4) = 54;

  assert(*stasis_page_lsn_ptr(&p) == lsn);
  assert(*stasis_page_type_ptr(&p) == 10);
  //assert(end_of_usable_space_ptr(&p) == stasis_page_type_ptr(&p));
  assert(*freespace_ptr(&p) == 15);
  assert(*numslots_ptr(&p)  == 20);
  assert(*slot_ptr(&p, 0) == 30);
  assert(*slot_ptr(&p, 1) == 35);
  assert(*slot_ptr(&p, 40) == 40);
  assert(*slot_length_ptr(&p, 0) == 31);
  assert(*slot_length_ptr(&p, 1) == 36);
  assert(*slot_length_ptr(&p, 40) == 41);
  
  assert(*stasis_page_byte_ptr_from_start(&p, 0) == 50);
  assert(*stasis_page_byte_ptr_from_start(&p, 1) == 51);
  assert(*stasis_page_byte_ptr_from_start(&p, 2) == 52);
  assert(*stasis_page_byte_ptr_from_start(&p, 3) == 53);
  assert(*stasis_page_byte_ptr_from_start(&p, 4) == 54);

} END_TEST

static void assertRecordCountSizeType(int xid, Page *p, int count, int size, int type) {
  int foundRecords = 0;

  recordid it = stasis_record_first(xid,p);
  assert(it.size != INVALID_SLOT);
  do {
    foundRecords++;
    assert(stasis_record_length_read(xid,p,it)  == size);
    assert(stasis_record_type_read(xid,p,it) == type);
    it.size = 0;
    assert(stasis_record_length_read(xid,p,it)  == size);
    assert(stasis_record_type_read(xid,p,it) == type);
    it.size = INVALID_SLOT;
    assert(stasis_record_length_read(xid,p,it)  == size);
    assert(stasis_record_type_read(xid,p,it) == type);
    it = stasis_record_next(xid,p,it);
  } while(it.size != INVALID_SLOT);

  assert(foundRecords == count);
  assert(it.page == NULLRID.page);
  assert(it.slot   == NULLRID.slot);
  assert(it.size   == NULLRID.size);
}

static void checkPageIterators(int xid, Page *p,int record_count) {
  recordid first = stasis_record_alloc_begin(xid, p, sizeof(int64_t));
  stasis_record_alloc_done(xid,p,first);

  for(int i = 1; i < record_count; i++) {
    stasis_record_alloc_done(xid,p,stasis_record_alloc_begin(xid,p,sizeof(int64_t)));
  }

  assertRecordCountSizeType(xid, p, record_count, sizeof(int64_t), NORMAL_SLOT);


  if(*stasis_page_type_ptr(p) == SLOTTED_PAGE) {
    recordid other = first;
    other.slot = 3;
    stasis_record_free(xid,p,other);
    assertRecordCountSizeType(xid, p, record_count-1, sizeof(int64_t), NORMAL_SLOT);
  }
}
/**
   @test

   Check functions used to iterate over pages

   XXX this should also test indirect pages.
*/
START_TEST(pageRecordSizeTypeIteratorTest) {
  Tinit();
  int xid = Tbegin();
  pageid_t pid = TpageAlloc(xid);

  Page * p = loadPage(xid,pid);
  writelock(p->rwlatch,0);
  memset(p->memAddr, 0, PAGE_SIZE);
  stasis_slotted_initialize_page(p);

  checkPageIterators(xid,p,10);

  unlock(p->rwlatch);

  pid = TpageAlloc(xid);

  p = loadPage(xid,pid);
  writelock(p->rwlatch,0);
  memset(p->memAddr, 0, PAGE_SIZE);
  stasis_fixed_initialize_page(p,sizeof(int64_t),0);

  checkPageIterators(xid,p,10);

  unlock(p->rwlatch);

  Tcommit(xid);
  Tdeinit();
} END_TEST
/**
   @test 
   
   Page test that allocates multiple records
   
*/
START_TEST(pageNoThreadMultPageTest)
{
  Page * p;
  /*    p->id = 0;*/


  pthread_mutex_init(&lsn_mutex, NULL);

  Tinit();

  p = loadPage(-1, 1);
  memset(p->memAddr, 0, PAGE_SIZE);
  p->LSN = 0;
  *stasis_page_lsn_ptr(p) = p->LSN;

  writelock(p->rwlatch,0);
  stasis_slotted_initialize_page(p);
  unlock(p->rwlatch);
  multiple_simultaneous_pages(p);
  // Normally, you would call pageWriteLSN() to update the LSN.  This
  // is a hack, since Tdeinit() will crash if it detects page updates
  // that are off the end of the log..
  p->LSN = 0;
  *stasis_page_lsn_ptr(p) = p->LSN;

  releasePage(p);

  /*  unlock(p->loadlatch); */


  Tdeinit();
  pthread_mutex_destroy(&lsn_mutex);

}
END_TEST

/** 
    Check the page implementation in the multi-threaded case.
*/
START_TEST(pageThreadTest) {

#define  THREAD_COUNT 10
  pthread_t workers[THREAD_COUNT];
  int i;
  pthread_mutex_init(&random_mutex, NULL);
  pthread_mutex_init(&lsn_mutex, NULL);

  fail_unless(1, NULL);
  Tinit();
  Tdeinit();
  Tinit();
  fail_unless(1, NULL);

  Page * p = loadPage(-1, 2);
  writelock(p->rwlatch,0);
  memset(p->memAddr, 0, PAGE_SIZE);
  stasis_slotted_initialize_page(p);
  unlock(p->rwlatch);
  p->LSN = 0;
  *stasis_page_lsn_ptr(p) = p->LSN;

  fail_unless(1, NULL);

  for(i = 0; i < THREAD_COUNT; i++) {
    pthread_create(&workers[i], NULL, worker_thread, p);
  }
  fail_unless(1, NULL);

  for(i = 0; i < THREAD_COUNT; i++) {
    pthread_join(workers[i], NULL);
  }

  /*  unlock(p->loadlatch); */
  p->LSN = 0;
  *stasis_page_lsn_ptr(p) = p->LSN;
  releasePage(p);

  Tdeinit();

  pthread_mutex_destroy(&lsn_mutex);
  pthread_mutex_destroy(&random_mutex);

} END_TEST


/** 
    @test
*/
START_TEST(fixedPageThreadTest) {
  pthread_t workers[THREAD_COUNT];
  int i;
  pthread_mutex_init(&random_mutex, NULL);
  pthread_mutex_init(&lsn_mutex, NULL);
  Tinit();
  Page * p = loadPage(-1, 2);
  writelock(p->rwlatch,0);
  memset(p->memAddr, 0, PAGE_SIZE);
  stasis_fixed_initialize_page(p, sizeof(int), 0);
  unlock(p->rwlatch);
  p->LSN = 0;
  *stasis_page_lsn_ptr(p) = p->LSN;


  for(i = 0; i < THREAD_COUNT; i++) {
    pthread_create(&workers[i], NULL, fixed_worker_thread, p);
  }

  for(i = 0; i < THREAD_COUNT; i++) {
    pthread_join(workers[i], NULL);
  }

  p->LSN = 0;
  *stasis_page_lsn_ptr(p) = p->LSN;
  releasePage(p);
  Tdeinit();
  pthread_mutex_destroy(&lsn_mutex);
  pthread_mutex_destroy(&random_mutex);
} END_TEST

START_TEST(pageCheckSlotTypeTest) {
	Tinit();
	int xid = Tbegin();
	
	recordid slot      = Talloc(xid, sizeof(int));
	recordid fixedRoot = TarrayListAlloc(xid, 2, 10, 10);
	recordid blob      = Talloc(xid, PAGE_SIZE * 2);
	
	Page * p = loadPage(-1, slot.page);
        readlock(p->rwlatch, 0);
        assert(stasis_record_type_read(xid, p, slot) == NORMAL_SLOT);
        assert(stasis_record_length_read(xid, p, slot) == sizeof(int));
        unlock(p->rwlatch);
	releasePage(p);
	
	/** @todo the use of the fixedRoot recordid to check getRecordType is 
		  a bit questionable, but should work. */
	p = loadPage(-1, fixedRoot.page); 

        readlock(p->rwlatch, 0);
	assert(stasis_record_type_read(xid, p, fixedRoot) == NORMAL_SLOT);

	fixedRoot.slot = 1;
	// Force it to use indirect implementation (we're checking an array list page...)
	recordid  fixedEntry = dereferenceIndirectRID(xid, fixedRoot);

	fixedRoot.slot = 0;
	
        unlock(p->rwlatch);
	releasePage(p);
	
	p = loadPage(-1, fixedEntry.page);
        readlock(p->rwlatch, 0);
        assert(stasis_record_type_read(xid, p, fixedEntry) == NORMAL_SLOT);
        unlock(p->rwlatch);
	releasePage(p);
	
	p = loadPage(-1, blob.page);
        readlock(p->rwlatch, 0);
 	int type = stasis_record_type_read(xid, p, blob);
        unlock(p->rwlatch);
        assert(type == BLOB_SLOT);
	releasePage(p); 

	recordid bad;
	bad.page = slot.page;
	bad.slot = slot.slot + 10;
	bad.size = 4;
	
	p = loadPage(xid, bad.page);
        readlock(p->rwlatch, 0);
	assert(stasis_record_type_read(xid, p, bad) == INVALID_SLOT);
	bad.size = 100000;
	assert(stasis_record_type_read(xid, p, bad) == INVALID_SLOT);
	/** recordGetType now ignores the size field, so this (correctly) returns SLOTTED_RECORD */
	bad.slot = slot.slot;
	assert(stasis_record_type_read(xid, p, bad) == NORMAL_SLOT);
	p->LSN = 0;
	*stasis_page_lsn_ptr(p) = p->LSN;
        unlock(p->rwlatch);
	releasePage(p);
	
	Tcommit(xid);
	
	Tdeinit();
} END_TEST
/**
  @test unit test for TrecordType
*/
START_TEST(pageTrecordTypeTest) {
	Tinit();
	int xid = Tbegin();
	
	recordid slot      = Talloc(xid, sizeof(int));
	recordid fixedRoot = TarrayListAlloc(xid, 2, 10, 10);
	recordid blob      = Talloc(xid, PAGE_SIZE * 2);
	
	assert(TrecordType(xid, slot) == NORMAL_SLOT);
	assert(TrecordType(xid, fixedRoot) == NORMAL_SLOT);  
	
	fixedRoot.slot = 1;
	// This is an array list page, but we want to check the state
	// of the internal node.
	recordid  fixedEntry = dereferenceIndirectRID(xid, fixedRoot);

	fixedRoot.slot = 0;
	
	assert(TrecordType(xid, fixedEntry) == NORMAL_SLOT);
	
	int type = TrecordType(xid, blob);
	assert(type == BLOB_SLOT);
		
	recordid bad;
	bad.page = slot.page;
	bad.slot = slot.slot + 10;
	bad.size = 4;
	
	assert(TrecordType(xid, bad) == INVALID_SLOT);
	bad.size = 100000;
	assert(TrecordType(xid, bad) == INVALID_SLOT);

	bad.slot = slot.slot;
	assert(TrecordType(xid, bad) == NORMAL_SLOT);
	/* Try to trick TrecordType by allocating a record that's the
	   same length as the slot used by blobs. */
	recordid rid2 = Talloc(xid, sizeof(blob_record_t));
	assert(TrecordType(xid, rid2) == NORMAL_SLOT);

	Tcommit(xid);
	
	Tdeinit();
} END_TEST


Suite * check_suite(void) {
  Suite *s = suite_create("page");
  /* Begin a new test */
  TCase *tc = tcase_create("pagethreads");
  tcase_set_timeout(tc, 0); // disable timeouts
  /* Sub tests are added, one per line, here */

  tcase_add_test(tc, pageRecordSizeTypeIteratorTest);
  tcase_add_test(tc, pageCheckMacros);
  tcase_add_test(tc, pageCheckSlotTypeTest);
  tcase_add_test(tc, pageTrecordTypeTest);
  tcase_add_test(tc, pageNoThreadMultPageTest);
  tcase_add_test(tc, pageNoThreadTest);
  tcase_add_test(tc, pageThreadTest);
  tcase_add_test(tc, fixedPageThreadTest);

  /* --------------------------------------------- */
  
  tcase_add_checked_fixture(tc, setup, teardown);

  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
