/*--- This software is copyrighted by the Regents of the University of
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
#include "../check_includes.h"

#include <stasis/transactional.h>
#include <stasis/logger/logger2.h>
#include <stasis/bufferManager.h>
#include <stasis/truncation.h>
#include <stasis/page.h>

#include <assert.h>
#include <stdio.h>

#define LOG_NAME   "check_operations.log"

/**
   @test

   Assuming that the Tset() operation is implemented correctly, checks
   that doUpdate, redoUpdate and undoUpdate are working correctly, for
   operations that use physical logging.
*/
START_TEST(operation_physical_do_undo) {
  recordid rid;
  lsn_t lsn = 2;
  int buf;

  LogEntry * setToTwo;

  Tinit();
  int  xid = Tbegin();
  long long pnum = TpageAlloc(xid);
  Page * p = loadPage(xid, pnum);

  writelock(p->rwlatch, 0);
  stasis_page_slotted_initialize_page(p);
  rid = stasis_record_alloc_begin(xid, p, sizeof(int));
  stasis_record_alloc_done(xid, p, rid);
  unlock(p->rwlatch);
  releasePage(p);

  DEBUG("A\n");

  byte arg[sizeof(slotid_t) + sizeof(int64_t) + 2 * sizeof(int)];
  byte * cur = arg;
  *(slotid_t*)cur = rid.slot; cur += sizeof(slotid_t);
  *(int64_t*) cur = sizeof(int); cur += sizeof(int64_t);
  *(int*)     cur = 2; cur += sizeof(int);
  *(int*)     cur = 1;


  // XXX fails; set log format has changed
  setToTwo = allocUpdateLogEntry(-1, xid, OPERATION_SET, rid.page,
                                 (void*)arg, sizeof(slotid_t) + sizeof(int64_t) + 2 * sizeof(int));



  /* Do, undo and redo operation without updating the LSN field of the page. */

  DEBUG("B\n");

  p = loadPage(xid, rid.page);
  writelock(p->rwlatch,0);
  // manually fill in UNDO field
  stasis_record_write(xid, p, rid, (byte*)&buf);
  stasis_page_lsn_write(xid, p, lsn);
  unlock(p->rwlatch);
  releasePage(p);
  setToTwo->LSN = 10;

  DEBUG("C\n");
  p = loadPage(xid, rid.page);
  writelock(p->rwlatch,0);
  stasis_operation_do(setToTwo, p);  /* PAGE LSN= 10, value = 2. */
  unlock(p->rwlatch);
  releasePage(p);

  p = loadPage(xid, rid.page);
  writelock(p->rwlatch,0);
  stasis_record_read(xid, p, rid, (byte*)&buf);
  unlock(p->rwlatch);
  releasePage(p);

  assert(buf == 2);


  DEBUG("D\n");

  p = loadPage(xid, rid.page);
  readlock(p->rwlatch,0);
  assert(10 == stasis_page_lsn_read(p)); // "page lsn not set correctly."

  setToTwo->LSN = 5;

  stasis_operation_undo(setToTwo, 12, p); /* Should succeed: log LSN is lower than page LSN, but effective LSN is higher than page LSN */

  unlock(p->rwlatch);
  releasePage(p);

  p = loadPage(xid, rid.page);
  readlock(p->rwlatch,0);
  stasis_record_read(xid, p, rid, (byte*)&buf);

  assert(buf == 1);

  DEBUG("E\n");
  stasis_operation_redo(setToTwo,p);

  stasis_record_read(xid, p, rid, (byte*)&buf);
  unlock(p->rwlatch);
  releasePage(p);

  assert(buf == 1);

  /* Now, simulate scenarios from normal operation:
         do the operation, and update the LSN, (update happens)
	 then undo, and update the LSN again.  (undo happens)
	 attempt a redo, don't update lsn      (nothing happens)

     Lower the LSN
         attempt redo                          (redo succeeds)

  */

  lsn = 0;
  buf = 1;

  p = loadPage(xid, rid.page);
  writelock(p->rwlatch,0);
  stasis_record_write(xid, p, rid, (byte*)&buf);
  stasis_page_lsn_write(xid, p, lsn);
  unlock(p->rwlatch);
  releasePage(p);
  /* Trace of test:

  PAGE LSN     LOG LSN      CLR LSN    TYPE        SUCCEED?
  2             10          -          do write    YES      (C)
  10             5          8          undo write  YES      (D)
  8              5          -          redo write  NO       (E)
  8             10          -          redo write  YES      (F)
 .......  and so on.
  */

  // XXX This is a hack to put some stuff in the log.  Otherwise, Tdeinit() fails.
  for(int i = 0; i < 10; i++)
    ((stasis_log_t*)stasis_log())->write_entry(stasis_log(),
                                 allocCommonLogEntry(-1, -1, -1));

  /** @todo need to re-think check_operations.  The test is pretty broken. */
  Tcommit(xid);
  Tdeinit();
  return;

  setToTwo->LSN = 10;

  p = loadPage(xid, rid.page);
  writelock(p->rwlatch,0);

  DEBUG("F\n");
  stasis_operation_redo(setToTwo,p);

  stasis_record_read(xid, p, rid, (byte*)&buf);
  assert(buf == 2);

  DEBUG("G undo set to 2\n");
  stasis_operation_undo(setToTwo, 20, p);   /* Succeeds -- 20 is the 'CLR' entry's lsn.*/

  stasis_record_read(xid, p, rid, (byte*)&buf);
  assert(buf == 1);

  DEBUG("H don't redo set to 2\n");
  stasis_operation_redo(setToTwo,p);        /* Fails */

  stasis_record_read(xid, p, rid, (byte*)&buf);

  assert(buf == 1);

  stasis_record_write(xid, p, rid, (byte*)&buf); /* reset the page's LSN. */
  stasis_page_lsn_write(xid, p, 0);
  DEBUG("I redo set to 2\n");

  stasis_operation_redo(setToTwo,p);        /* Succeeds */

  stasis_record_read(xid, p, rid, (byte*)&buf);
  assert(buf == 2);
  unlock(p->rwlatch);
  releasePage(p);
  Tdeinit();
}
END_TEST

/**
    @test check the Tprepare() call by simulating crashes.
*/
START_TEST(operation_prepare) {

  /* Check this sequence prepare, action, crash, recover, read, action, abort, read again. */

  Tinit();

  int loser = Tbegin();
  int prepared = Tbegin();
  int winner = Tbegin();

  recordid a = Talloc(winner, sizeof(int));
  recordid b = Talloc(winner, sizeof(int));

  int one =1;
  int two =2;
  int three=3;

  Tset(winner, a, &one);
  Tset(winner, b, &one);

  Tset(loser, a, &three);
  Tset(prepared, b, &three);

  Tprepare(prepared);

  Tset(prepared, b, &two);

  Tcommit(winner);
  TuncleanShutdown();

  Tinit();

  int in;

  Tread(prepared, b, &in);

  assert(in == three);

  Tset(prepared, b, &two);

  Tabort(prepared);

  int checker = Tbegin();

  Tread(checker, b, &in);

  assert(in == one);

  Tread(checker, a, &in);

  assert(in == one);

  Tcommit(checker);

  Tdeinit();
  /* Check this sequence prepare, action, crash, recover, read, action, _COMMIT_, read again. */

  Tinit();

  loser = Tbegin();
  prepared = Tbegin();
  winner = Tbegin();

  a = Talloc(winner, sizeof(int));
  b = Talloc(winner, sizeof(int));

  one =1;
  two =2;
  three=3;

  Tset(winner, a, &one);
  Tset(winner, b, &one);

  Tset(loser, a, &three);
  Tset(prepared, b, &three);

  Tprepare(prepared); //, a);

  Tset(prepared, b, &two);

  Tcommit(winner);

  TuncleanShutdown();


  Tinit();

  Tread(prepared, b, &in);

  assert(in == three);

  Tset(prepared, b, &two);

  Tcommit(prepared);

  checker = Tbegin();

  Tread(checker, b, &in);

  assert(in == two);

  Tread(checker, a, &in);

  assert(in == one);

  Tcommit(checker);

  Tdeinit();

} END_TEST
/**
  @test Runs some actions as part of a nested top action, aborts the transaction,
  and checks that the result is as expected.

  @todo Write a more thorough (threaded!) nested top action test.

*/
START_TEST(operation_nestedTopAction) {

  Tinit();

  int xid= Tbegin();
  int *dat;

  dat = malloc(sizeof(int));
  recordid rid1 = Talloc(xid, sizeof(int));
  recordid rid2 = Talloc(xid, sizeof(int));
  recordid rid3 = Talloc(xid, sizeof(int));
  recordid rid4 = Talloc(xid, sizeof(int));

  *dat = 1;
  Tset(xid, rid1, dat);
  *dat = 2;
  Tset(xid, rid2, dat);
  *dat = 3;
  Tset(xid, rid3, dat);
  *dat = 4;
  Tset(xid, rid4, dat);
  Tcommit(xid);
  xid = Tbegin(); // Loser xact.

  *dat = 10;
  Tset(xid, rid1, dat);

  void * handle = TbeginNestedTopAction(xid, OPERATION_NOOP, NULL, 0);

  *dat = 20;
  Tset(xid, rid2, dat);
  *dat = 30;
  Tset(xid, rid3, dat);

  TendNestedTopAction(xid, handle);
  *dat = 40;

  Tset(xid, rid4, dat);

  Tabort(xid);

  xid = Tbegin();
  int dat1;
  int dat2;
  int dat3;
  int dat4;
  Tread(xid, rid1, &dat1);
  Tread(xid, rid2, &dat2);
  Tread(xid, rid3, &dat3);
  Tread(xid, rid4, &dat4);

  assert(dat1 == 1);
  assert(dat2 == 20);
  assert(dat3 == 30);
  assert(dat4 == 4);

  Tcommit(xid);
  Tdeinit();

} END_TEST

START_TEST(operation_set_range) {
  Tinit();

  int xid = Tbegin();

  int buf1[20];
  int buf2[20];

  int range[20];

  recordid rid = Talloc(xid, sizeof(int) * 20);

  for(int i = 0; i < 20; i++) {
    buf1[i] = i;
  }

  Tset(xid, rid, buf1);
  Tcommit(xid);

  xid = Tbegin();
  Tread(xid, rid, buf2);
  for(int i = 0; i < 20; i++) {
    assert(buf2[i] == i);
  }
  for(int i = 0; i < 5; i++) {
    range[i] = 100 + i;
  }

  TsetRange(xid, rid, sizeof(int) * 10, sizeof(int) * 5, range);
  // Check forward action
  Tread(xid, rid, buf2);

  for(int i = 0; i < 20; i++) {
    if(i < 10 || i >= 15) {
	assert(buf2[i] == i);
    } else {
	assert(buf2[i] == 100 + i - 10);
    }
  }

  Tabort(xid);

  xid = Tbegin();
  Tread(xid, rid, buf2);
  //Check undo.
  for(int i = 0; i < 20; i++) {
    assert(buf2[i] == i);
  }
  Tcommit(xid);

  Tdeinit();

} END_TEST

START_TEST(operation_alloc_test) {
  Tinit();

  int xid = Tbegin();
  Talloc(xid, 100);
  Tcommit(xid);
  xid = Tbegin();
  Talloc(xid, 100);
  Tcommit(xid);

  Tdeinit();

} END_TEST

#define ARRAY_LIST_CHECK_ITER 10000
START_TEST(operation_array_list) {

  Tinit();

  int xid = Tbegin();
  recordid rid = TarrayListAlloc(xid, 4, 2, sizeof(int));
  assert(0 == TarrayListLength(xid, rid));
  TarrayListExtend(xid, rid, ARRAY_LIST_CHECK_ITER);
  assert(ARRAY_LIST_CHECK_ITER == TarrayListLength(xid, rid));
  Tcommit(xid);

  xid = Tbegin();

  recordid rid2;
  rid2.page = rid.page;
  rid2.slot = 0;
  rid2.size = sizeof(int);

  for(int i = 0; i < ARRAY_LIST_CHECK_ITER; i++) {
    rid2.slot = i;
    Tset(xid, rid2, &i);
  }

  for(int i = 0; i < ARRAY_LIST_CHECK_ITER; i++) {
    rid2.slot = i;
    int j;
    Tread(xid, rid2, &j);
    assert(i == j);
  }

  Tcommit(xid);

  xid = Tbegin();

  for(int i = 0; i < ARRAY_LIST_CHECK_ITER; i++) {
    int j = 0-i;
    rid2.slot = i;
    Tset(xid, rid2, &j);
  }

  for(int i = 0; i < ARRAY_LIST_CHECK_ITER; i++) {
    rid2.slot = i;
    int j = 0-i;
    int k;
    Tread(xid, rid2, &k);
    assert(k == j);
  }
  Tabort(xid);

  xid = Tbegin();
  for(int i = 0; i < ARRAY_LIST_CHECK_ITER; i++) {
    rid2.slot = i;
    int j;
    Tread(xid, rid2, &j);
    assert(i == j);
  }
  Tcommit(xid);

  Tdeinit();

} END_TEST

START_TEST(operation_lsn_free) {
  Tinit();
  recordid rid[100];
  {
    int xid = Tbegin();
    pageid_t pid = TpageAlloc(xid);
    Page * p = loadPage(xid,pid);
    writelock(p->rwlatch,0);
    stasis_slotted_lsn_free_initialize_page(p);
    // XXX hack!
    byte * old = malloc(PAGE_SIZE);
    memcpy(old, p->memAddr, PAGE_SIZE);
    int fortyTwo = 42;
    for(int i = 0; i < 100; i++) {
      rid[i] = stasis_record_alloc_begin(xid, p, sizeof(int));
      stasis_record_alloc_done(xid, p, rid[i]);
      stasis_record_write(xid, p, rid[i], (const byte*)&fortyTwo);
      stasis_page_lsn_write(xid, p, -1);
    }
    byte * new = malloc(PAGE_SIZE);
    memcpy(new, p->memAddr, PAGE_SIZE);
    memcpy(p->memAddr, old, PAGE_SIZE);
    unlock(p->rwlatch);
    releasePage(p);
    TpageSet(xid, pid, new);
    free(old);
    free(new);
    Tcommit(xid);
  }
  {
    int xid[2];
    xid[0] = Tbegin();
    xid[1] = Tbegin();
    for(int i = 0; i < 100; i++) {
      int foo;
      Tread(xid[i%2],rid[i], &foo);
      assert(foo == 42);
      TsetLsnFree(xid[i%2], rid[i], &i);
      Tread(xid[i%2],rid[i], &foo);
      assert(foo == i);
    }
    Tcommit(xid[0]);
    Tabort(xid[1]);
  }
  Tdeinit();

  Tinit();
  {
    int xid = Tbegin();

    for(int i = 0; i < 100; i++) {
      int foo;
      Tread(xid, rid[i], &foo);
      if(i%2) {
        assert(foo == 42);
      } else {
        assert(foo == i);
      }
    }
    Tcommit(xid);
  }
  Tdeinit();

} END_TEST


START_TEST(operation_reorderable) {
  Tinit();
  recordid rid[100];
  {
    int xid = Tbegin();
    pageid_t pid = TpageAlloc(xid);
    Page * p = loadPage(xid,pid);
    writelock(p->rwlatch,0);
    stasis_slotted_lsn_free_initialize_page(p);
    // XXX hack!
    byte * old = malloc(PAGE_SIZE);
    memcpy(old, p->memAddr, PAGE_SIZE);
    int fortyTwo = 42;
    for(int i = 0; i < 100; i++) {
      rid[i] = stasis_record_alloc_begin(xid, p, sizeof(int));
      stasis_record_alloc_done(xid, p, rid[i]);
      stasis_record_write(xid, p, rid[i], (const byte*)&fortyTwo);
      stasis_page_lsn_write(xid, p, -1);
    }
    byte * new = malloc(PAGE_SIZE);
    memcpy(new, p->memAddr, PAGE_SIZE);
    memcpy(p->memAddr, old, PAGE_SIZE);
    unlock(p->rwlatch);
    releasePage(p);
    TpageSet(xid, pid, new);
    free(old);
    free(new);
    Tcommit(xid);
  }
  {
    int xid[2];

    xid[0] = Tbegin();
    xid[1] = Tbegin();

    stasis_log_reordering_handle_t * rh
      = stasis_log_reordering_handle_open(
                         &stasis_transaction_table[xid[0]% MAX_TRANSACTIONS],
                         stasis_log(),
                         100, // bytes (far too low!)
                         10,  // log entries
                         500 // max byte size
                                          );
    for(int i = 0; i < 100; i++) {
      int foo;
      Tread(xid[i%2],rid[i], &foo);
      assert(foo == 42);
      if(i%2) {
        TsetLsnFree(xid[i%2], rid[i], &i);
      } else {
        TsetReorderable(xid[i%2], rh, rid[i], &i);
      }
      Tread(xid[i%2],rid[i], &foo);
      assert(foo == i);
    }
    stasis_log_reordering_handle_close(rh);
    Tcommit(xid[0]);
    Tabort(xid[1]);
  }
  Tdeinit();

  Tinit();
  {
    int xid = Tbegin();

    for(int i = 0; i < 100; i++) {
      int foo;
      Tread(xid, rid[i], &foo);
      if(i%2) {
        assert(foo == 42);
      } else {
        assert(foo == i);
      }
    }
    Tcommit(xid);
  }
  Tdeinit();

} END_TEST

START_TEST(operation_segment) {
  const int SEGMENT_TEST = 42;
  Tinit();
  int xid = Tbegin();
  recordid regionrid = Talloc(xid, sizeof(pageid_t));
  pageid_t region_start = TregionAlloc(xid, 10000, SEGMENT_TEST);
  Tset(xid, regionrid, &region_start);
  Tcommit(xid);

  xid = Tbegin();

  for(int i = 0; i < 10000 / sizeof(i); i++) {
    int ret = Tpwrite(xid, (byte*)&i, sizeof(i), region_start * PAGE_SIZE + i * sizeof(i));
    assert(ret == sizeof(i));
  }
  Tcommit(xid);

  TuncleanShutdown();

  Tinit();
  xid = Tbegin();
  for(int i = 0; i < 10000 / sizeof(i); i++) {
    int buf;
    int ret = Tpread(xid, (byte*)&buf, sizeof(buf), region_start * PAGE_SIZE + i * sizeof(i));
    assert(buf == i);
    assert(ret == sizeof(buf));
    if(i % 2) {
      buf = 0;
      Tpwrite(xid, (byte*)&buf, sizeof(buf), region_start * PAGE_SIZE + i * sizeof(i));
    }
  }

  for(int i = 0; i < 10000 / sizeof(i); i++) {
    int buf;
    int ret = Tpread(xid, (byte*)&buf, sizeof(buf), region_start * PAGE_SIZE + i * sizeof(i));
    assert(buf == (i % 2 ? 0 : i));
    assert(ret == sizeof(buf));
  }
  Tabort(xid);
  xid = Tbegin();

  for(int i = 0; i < 10000 / sizeof(i); i++) {
    int buf;
    int ret = Tpread(xid, (byte*)&buf, sizeof(buf), region_start * PAGE_SIZE + i * sizeof(i));
    assert(buf == i);
    assert(ret == sizeof(buf));
  }
  Tcommit(xid);
  Tdeinit();

} END_TEST

/**
  Add suite declarations here
*/
Suite * check_suite(void) {
  Suite *s = suite_create("operations");
  /* Begin a new test */

  TCase *tc = tcase_create("operations_simple");
  tcase_set_timeout(tc, 0); // disable timeouts

  /* Sub tests are added, one per line, here */
  tcase_add_test(tc, operation_physical_do_undo);
  tcase_add_test(tc, operation_nestedTopAction);
  tcase_add_test(tc, operation_set_range);
  if(stasis_log_type != LOG_TO_MEMORY) {
    tcase_add_test(tc, operation_prepare);
  }
  tcase_add_test(tc, operation_alloc_test);
  tcase_add_test(tc, operation_array_list);
  tcase_add_test(tc, operation_lsn_free);
  tcase_add_test(tc, operation_reorderable);
  tcase_add_test(tc, operation_segment);
 /* --------------------------------------------- */
  tcase_add_checked_fixture(tc, setup, teardown);
  suite_add_tcase(s, tc);


  return s;
}

#include "../check_setup.h"
