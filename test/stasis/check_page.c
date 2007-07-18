
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

    @todo check_page should judiciously avoid lsn_ptr()
*/

#include <config.h>
#include <check.h>

#include "../../src/stasis/page.h"
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
    assert(pageReadLSN(p) < this_lsn);
    pthread_mutex_unlock(&lsn_mutex);

    if(! first ) {
      for(k = 0; k < 100; k++) {
	recordRead(1, p, rid[k], (byte*)&j);

	assert((j + 1) ==  i + k);
        writelock(p->rwlatch,0);
        recordFree(-1, p, rid[k]);
        pageWriteLSN(-1, p, this_lsn);
        unlock(p->rwlatch);
	sched_yield();
      }
    } 
    
    first = 0;
    
    for(k = 0; k < 100; k++) {
      writelock(p->rwlatch,0);
      rid[k] = recordPreAlloc(-1,p,sizeof(short));
      if(rid[k].size == INVALID_SLOT) { // Is rid[k] == NULLRID?
        pageCompact(p);
        rid[k] = recordPreAlloc(-1,p,sizeof(short));
      }
      recordPostAlloc(-1,p,rid[k]);
      int * buf = (int*)recordWriteNew(-1,p,rid[k]);
      *buf = i+k;
      pageWriteLSN(-1, p, this_lsn);
      assert(pageReadLSN(p) >= this_lsn);
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
  int j;
  int first = 1;
  recordid rid;

  for(i = 0; i < 100; i++) {
    pthread_mutex_lock(&lsn_mutex);
    this_lsn = lsn;
    lsn++;
    pthread_mutex_unlock(&lsn_mutex);

    writelock(p->rwlatch,0);
    if(! first ) {
      j = *(int*)recordReadNew(-1,p,rid);
      assert((j + 1) ==  i);
    }
    first = 0;
    rid = recordPreAlloc(-1, p, sizeof(int));
    recordPostAlloc(-1, p, rid);
    (*(int*)recordWriteNew(-1,p,rid)) = i;
    pageWriteLSN(-1, p,lsn);
    assert(pageReadLSN( p) >= this_lsn);
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
      recordRead(1, p, rid, (byte*)&j);
      assert((j + 1) ==  i);
      writelock(p->rwlatch,0);
      recordFree(-1, p, rid);
      pageWriteLSN(-1, p, this_lsn);
      unlock(p->rwlatch);
      sched_yield();
    }

    first = 0;

    // @todo In check_page, a condition variable would be more efficient...
    writelock(p->rwlatch,0);
    if(pageFreespace(-1, p) < sizeof(int)) {
      first = 1;
    } else {
      rid = recordPreAlloc(-1, p, sizeof(int));
      recordPostAlloc(-1, p, rid);
      int * buf = (int*)recordWriteNew(-1, p, rid);
      pageWriteLSN(-1,p,this_lsn);
      *buf = i;
      assert(pageReadLSN(p) >= this_lsn);
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
  slottedPageInitialize(p);
  unlock(p->rwlatch);
  worker_thread(p);

  p->LSN = 0;
  *lsn_ptr(p) = p->LSN;

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
  
  *lsn_ptr(&p) = lsn;
  *page_type_ptr(&p) = 10;
  *freespace_ptr(&p) = 15;
  *numslots_ptr(&p)  = 20;
  *slot_ptr(&p, 0)   = 30;
  *slot_ptr(&p, 1)   = 35;
  *slot_ptr(&p, 40)   = 40;
  *slot_length_ptr(&p, 0)   = 31;
  *slot_length_ptr(&p, 1)   = 36;
  *slot_length_ptr(&p, 40)   = 41;

  *bytes_from_start(&p, 0) = 50;
  *bytes_from_start(&p, 1) = 51;
  *bytes_from_start(&p, 2) = 52;
  *bytes_from_start(&p, 3) = 53;
  *bytes_from_start(&p, 4) = 54;

  assert(*lsn_ptr(&p) == lsn);
  assert(*page_type_ptr(&p) == 10);
  assert(end_of_usable_space_ptr(&p) == page_type_ptr(&p));
  assert(*freespace_ptr(&p) == 15);
  assert(*numslots_ptr(&p)  == 20);
  assert(*slot_ptr(&p, 0) == 30);
  assert(*slot_ptr(&p, 1) == 35);
  assert(*slot_ptr(&p, 40) == 40);
  assert(*slot_length_ptr(&p, 0) == 31);
  assert(*slot_length_ptr(&p, 1) == 36);
  assert(*slot_length_ptr(&p, 40) == 41);
  
  assert(*bytes_from_start(&p, 0) == 50);
  assert(*bytes_from_start(&p, 1) == 51);
  assert(*bytes_from_start(&p, 2) == 52);
  assert(*bytes_from_start(&p, 3) == 53);
  assert(*bytes_from_start(&p, 4) == 54);

  assert(isValidSlot(&p, 0));
  assert(isValidSlot(&p, 1));
  assert(isValidSlot(&p, 40));

  /*  invalidateSlot(&p, 0);
  invalidateSlot(&p, 1);
  invalidateSlot(&p, 40);
  
  assert(!isValidSlot(&p, 0));
  assert(!isValidSlot(&p, 1));
  assert(!isValidSlot(&p, 40));*/


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
  p->LSN = 0;
  *lsn_ptr(p) = p->LSN;

  writelock(p->rwlatch,0);
  slottedPageInitialize(p);
  unlock(p->rwlatch);
  multiple_simultaneous_pages(p);
  // Normally, you would call pageWriteLSN() to update the LSN.  This
  // is a hack, since Tdeinit() will crash if it detects page updates
  // that are off the end of the log..
  p->LSN = 0;
  *lsn_ptr(p) = p->LSN;

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
  slottedPageInitialize(p);
  unlock(p->rwlatch);
  p->LSN = 0;
  *lsn_ptr(p) = p->LSN;

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
  *lsn_ptr(p) = p->LSN;
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
  fixedPageInitialize(p, sizeof(int), 0);
  unlock(p->rwlatch);
  p->LSN = 0;
  *lsn_ptr(p) = p->LSN;


  for(i = 0; i < THREAD_COUNT; i++) {
    pthread_create(&workers[i], NULL, fixed_worker_thread, p);
  }

  for(i = 0; i < THREAD_COUNT; i++) {
    pthread_join(workers[i], NULL);
  }

  p->LSN = 0;
  *lsn_ptr(p) = p->LSN;
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
        assert(recordGetTypeNew(xid, p, slot) == NORMAL_SLOT);
        unlock(p->rwlatch);
	releasePage(p);
	
	/** @todo the use of the fixedRoot recordid to check getRecordType is 
		  a bit questionable, but should work. */
	p = loadPage(-1, fixedRoot.page); 

        readlock(p->rwlatch, 0);
	assert(recordGetTypeNew(xid, p, fixedRoot) == NORMAL_SLOT);

        unlock(p->rwlatch);
	releasePage(p);
	
	fixedRoot.slot = 1;
	recordid  fixedEntry = dereferenceRID(xid, fixedRoot);
	fixedRoot.slot = 0;
	
	p = loadPage(-1, fixedEntry.page);
        readlock(p->rwlatch, 0);
        assert(recordGetTypeNew(xid, p, fixedEntry) == NORMAL_SLOT);
        unlock(p->rwlatch);
	releasePage(p);
	
	p = loadPage(-1, blob.page);
        readlock(p->rwlatch, 0);
 	int type = recordGetTypeNew(xid, p, blob);
        unlock(p->rwlatch);
        assert(type == BLOB_SLOT);
	releasePage(p); 

	recordid bad;
	bad.page = slot.page;
	bad.slot = slot.slot + 10;
	bad.size = 4;
	
	p = loadPage(xid, bad.page);
        readlock(p->rwlatch, 0);
	assert(recordGetTypeNew(xid, p, bad) == INVALID_SLOT);
	bad.size = 100000;
	assert(recordGetTypeNew(xid, p, bad) == INVALID_SLOT);
	/** recordGetType now ignores the size field, so this (correctly) returns SLOTTED_RECORD */
	bad.slot = slot.slot;
	assert(recordGetTypeNew(xid, p, bad) == NORMAL_SLOT);
	p->LSN = 0;
	*lsn_ptr(p) = p->LSN;
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
	recordid  fixedEntry = dereferenceRID(xid, fixedRoot);
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
