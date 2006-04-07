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
#include <config.h>
#include <check.h>
#include <assert.h>

#include <lladd/transactional.h>
#include "../../src/lladd/logger/logWriter.h"
#include <lladd/logger/logger2.h>
#include <lladd/bufferManager.h>

#include "../check_includes.h"
#include "../../src/lladd/page.h"
#include "../../src/lladd/page/slotted.h"
#define LOG_NAME   "check_operations.log"

#include <stdio.h>


void simulateBufferManagerCrash();
extern int numActiveXactions;
/**
   @test

   Assuming that the Tset() operation is implemented correctly, checks
   that doUpdate, redoUpdate and undoUpdate are working correctly, for
   operations that use physical logging.
*/
START_TEST(operation_physical_do_undo) {
  int xid = 1;
  recordid rid;
  lsn_t lsn = 2;
  int buf;
  int arg;
  LogEntry * setToTwo;
  Page * p;
  
  Tinit();
  xid = -1;
  rid  = slottedPreRalloc(xid, sizeof(int), &p);
  releasePage(p);
  buf = 1;
  arg = 2;

  DEBUG("A\n");
  setToTwo = allocUpdateLogEntry(-1, xid, OPERATION_SET, rid, (void*)&arg, sizeof(int), (void*)&buf);

  /* Do, undo and redo operation without updating the LSN field of the page. */

  DEBUG("B\n");
  
  p = loadPage(xid, rid.page);
  writeRecord(xid, p, lsn, rid, &buf);
  releasePage(p);
  setToTwo->LSN = 10;
  
  DEBUG("C\n");
  p = loadPage(xid, rid.page);
  doUpdate(setToTwo, p);  /* PAGE LSN= 10, value = 2. */
  releasePage(p);

  p = loadPage(xid, rid.page);
  readRecord(xid, p, rid, &buf);
  releasePage(p);

  fail_unless(buf == 2, NULL);



  DEBUG("D\n");

  p = loadPage(xid, rid.page);

  fail_unless(10 == pageReadLSN(p), "page lsn not set correctly.");

  setToTwo->LSN = 5;

  undoUpdate(setToTwo, p, 8);  /* Should succeed, CLR LSN is too low, but undoUpdate only checks the log entry. */
  releasePage(p);

  p = loadPage(xid, rid.page);
  readRecord(xid, p, rid, &buf);
  releasePage(p);

  fail_unless(buf == 1, NULL);
  
  DEBUG("E\n");
  redoUpdate(setToTwo);
  

  p = loadPage(xid, rid.page);
  readRecord(xid, p, rid, &buf);
  releasePage(p);

  fail_unless(buf == 1, NULL);
  
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
  writeRecord(xid, p, lsn, rid, &buf);
  releasePage(p);
  /* Trace of test: 

  PAGE LSN     LOG LSN      CLR LSN    TYPE        SUCCEED?
              
  2             10          -          do write    YES      (C) 
  10             5          8          undo write  YES      (D)
  8              5          -          redo write  NO       (E)
  8             10          -          redo write  YES      (F)
 .......  and so on.
  */


  /** @todo need to re-think check_operations.  The test is pretty broken. */

  return;

  setToTwo->LSN = 10;
  
  DEBUG("F\n");
  redoUpdate(setToTwo);

  p = loadPage(xid, rid.page);
  readRecord(xid, p, rid, &buf);
  assert(buf == 2);
  fail_unless(buf == 2, NULL);

  DEBUG("G undo set to 2\n");
  undoUpdate(setToTwo, p, 20);        /* Succeeds -- 20 is the 'CLR' entry's lsn.*/

  readRecord(xid, p, rid, &buf);

  fail_unless(buf == 1, NULL);
  releasePage(p);
  
  DEBUG("H don't redo set to 2\n");
  redoUpdate(setToTwo);        /* Fails */

  p = loadPage(xid, rid.page);

  readRecord(xid, p, rid, &buf);

  fail_unless(buf == 1, NULL);
  
  writeRecord(xid, p, 0, rid, &buf); /* reset the page's LSN. */

  DEBUG("I redo set to 2\n");

  releasePage(p);
  redoUpdate(setToTwo);        /* Succeeds */
  p = loadPage(xid, rid.page);
  readRecord(xid, p, rid, &buf);

  fail_unless(buf == 2, NULL);
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

  Tprepare(prepared, a);

  Tset(prepared, b, &two);

  Tcommit(winner);

  simulateBufferManagerCrash();
  //  closeLogWriter();
  LogDeinit();
  numActiveXactions = 0;


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

  Tprepare(prepared, a);

  Tset(prepared, b, &two);

  Tcommit(winner);

  simulateBufferManagerCrash();
  //  closeLogWriter();
  LogDeinit();
  numActiveXactions = 0;


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
  
  printf("\nNested Top Action\n"); fflush(NULL);
  
  Tinit();
  
  int xid= Tbegin();
  int *dat;
  //printf("W"); fflush(NULL);
  dat = malloc(sizeof(int));
  recordid rid1 = Talloc(xid, sizeof(int));
 // printf("{%d,%d,%d}\n", rid1.page, rid1.slot, rid1.size); fflush(NULL);
  recordid rid2 = Talloc(xid, sizeof(int));
 // printf("{%d,%d,%d}\n", rid2.page, rid2.slot, rid2.size); fflush(NULL);
  recordid rid3 = Talloc(xid, sizeof(int));
//  printf("{%d,%d,%d}\n", rid3.page, rid3.slot, rid3.size); fflush(NULL);
  recordid rid4 = Talloc(xid, sizeof(int));
  //printf("{%d,%d,%d}\n", rid4.page, rid4.slot, rid4.size); fflush(NULL);
 // printf("E"); fflush(NULL);
  *dat = 1;
  Tset(xid, rid1, dat);
  *dat = 2;
  Tset(xid, rid2, dat);
 // printf("F"); fflush(NULL);
  *dat = 3;
  Tset(xid, rid3, dat);
 // printf("G"); fflush(NULL);
  *dat = 4;
  Tset(xid, rid4, dat);
 // printf("H"); fflush(NULL);
  Tcommit(xid);
 // printf("A"); fflush(NULL);
  xid = Tbegin(); // Loser xact.
  
  *dat = 10;
  Tset(xid, rid1, dat);
  
  void * handle = TbeginNestedTopAction(xid, OPERATION_NOOP, NULL, 0);
  
  *dat = 20;
  Tset(xid, rid2, dat);
  *dat = 30;
  Tset(xid, rid3, dat);
  
  TendNestedTopAction(xid, handle);
 // printf("B"); fflush(NULL);
  *dat = 40;

  Tset(xid, rid4, dat);
  
  Tabort(xid);

  xid = Tbegin();
  int dat1;
  int dat2;
  int dat3;
  int dat4;
 // printf("C"); fflush(NULL);
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

/** 
    @test make sure the TinstantSet() operation works as expected during normal operation. 
    @todo need to write test for TinstantSet() for the recovery case...
*/

START_TEST(operation_instant_set) {

  Tinit();

  int xid = Tbegin();
  
  recordid rid = Talloc(xid, sizeof(int));  /** @todo probably need an immediate version of TpageAlloc... */
  int one = 1;
  int two = 2;
  int three = 3;
  Tset(xid, rid, &one);

  Tcommit(xid);

  xid = Tbegin();

  TinstantSet(xid, rid, &two);

  Tset(xid, rid, &three);

  Tabort(xid);
  
  xid = Tbegin();

  Tread(xid, rid, &three);

  assert(two == three);

  Tcommit(xid);
 
  Tdeinit();
 
  Tinit();

  xid = Tbegin();

  Tread(xid, rid, &three);

  assert(two == three);

  Tcommit(xid);
 
  Tdeinit();
 


} END_TEST

START_TEST(operation_set_range) {
  printf("Set Range");
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
  recordid rid1 = Talloc(xid, 100);
  Tcommit(xid);
  xid = Tbegin();
  recordid rid2 = Talloc(xid, 100);
  Tcommit(xid);
  
  printf("rid1={%d,%d,%lld} rid2={%d,%d,%lld}\n", 
	 rid1.page, rid1.slot, rid1.size,
	 rid2.page, rid2.slot, rid2.size);  
  
  
  
  Tdeinit();
} END_TEST

#define ARRAY_LIST_CHECK_ITER 10000
START_TEST(operation_array_list) {

  Tinit();
  
  int xid = Tbegin();

  recordid rid = TarrayListAlloc(xid, 4, 2, sizeof(int));

  TarrayListExtend(xid, rid, ARRAY_LIST_CHECK_ITER);
  
  printf("commit");
  fflush(stdout);
  Tcommit(xid);
  printf("done1\n");
  fflush(stdout);

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

  printf("commit");
  fflush(stdout);
  Tcommit(xid);
  printf("-done2\n");
  fflush(stdout);

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

  printf("abort");
  fflush(stdout);
  Tabort(xid);
  printf("-done\n");
  fflush(stdout);

  xid = Tbegin();
  for(int i = 0; i < ARRAY_LIST_CHECK_ITER; i++) {
    rid2.slot = i;
    int j;
    Tread(xid, rid2, &j);
    assert(i == j);
  }

  printf("commit");
  fflush(stdout);
  Tcommit(xid);
  printf("done3\n");
  fflush(stdout);


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
  tcase_add_test(tc, operation_instant_set);
  tcase_add_test(tc, operation_set_range);
  if(loggerType != LOG_TO_MEMORY) { 
    tcase_add_test(tc, operation_prepare);
  }
  tcase_add_test(tc, operation_alloc_test);
  tcase_add_test(tc, operation_array_list);
  /* --------------------------------------------- */
  tcase_add_checked_fixture(tc, setup, teardown);
  suite_add_tcase(s, tc);


  return s;
}

#include "../check_setup.h"
