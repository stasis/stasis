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
#include <config.h>
#include <check.h>
#include <assert.h> 

#include <lladd/transactional.h>
#include "../../src/lladd/logger/logWriter.h"
#include "../check_includes.h"

#define LOG_NAME   "check_recovery.log"

/** 
    @test
    Simple test: Insert some stuff.  Commit.  Call Tdeinit().  Call
    Tinit() (Which initiates recovery), and see if the stuff we
    inserted is still there.

    Only performs idempotent operations (Tset).
*/
START_TEST (recovery_idempotent) {
  int xid; 
  int j;
  int k;
  recordid rid;
  Tinit();
  xid = Tbegin();
  
  rid = Talloc(xid, sizeof(int));

  j = 1;

  Tset(xid, rid, &j);
 
  Tcommit(xid);

  xid = Tbegin();

  Tread(xid, rid, &k);
  
  fail_unless(j == k, "Get/Set broken?");

  Tcommit(xid);
  
  Tdeinit();

  Tinit();  /* Runs recovery.. */

  k = 12312;

  xid = Tbegin(); 

  Tread(xid, rid, &k);

  fail_unless(j == k, "Recovery messed something up!");

  Tcommit(xid);

  Tdeinit();

}
END_TEST

/** 
    @test
    Simple test: Alloc a record, commit.  Call Tincrement on it, and
    remember its value and commit.  Then, call Tdeinit() and Tinit()
    (Which initiates recovery), and see if the value changes. 
*/
START_TEST (recovery_exactlyOnce) {
  
  int xid; 
  int j;
  int k;
  recordid rid;
  Tinit();
  xid = Tbegin();
  
  rid = Talloc(xid, sizeof(int));

  Tincrement(xid, rid);

  Tread(xid, rid, &j);
 
  Tcommit(xid);

  xid = Tbegin();

  Tread(xid, rid, &k);
  
  fail_unless(j == k, "Get/Set broken?");

  Tcommit(xid);
  
  Tdeinit();

  Tinit();  /* Runs recovery.. */

  k = 12312;

  xid = Tbegin(); 

  Tread(xid, rid, &k);

  printf("j = %d, k = %d\n", j, k);

  assert(j == k);
  fail_unless(j == k, "Recovery messed something up!");

  Tcommit(xid);

  Tdeinit();



}
END_TEST


/** 
    @test
    Makes sure that aborted idempotent operations are correctly undone.
*/
START_TEST (recovery_idempotentAbort) {
  
  int xid; 
  int j;
  int k;
  recordid rid;
  Tinit();
  xid = Tbegin();
  
  rid = Talloc(xid, sizeof(int));
  j = 1;
  Tset(xid, rid, &j);

  Tread(xid, rid, &j);
 
  Tcommit(xid);

  xid = Tbegin();

  Tread(xid, rid, &k);
  
  fail_unless(j == k, "Get/Set broken?");

  Tcommit(xid);
  xid = Tbegin();
  k = 2;
  Tset(xid, rid, &k);
  k = 4;
  Tread(xid, rid, &k);
  fail_unless(k == 2, NULL);
  Tabort(xid);

  xid = Tbegin();
  k = 4;
  Tread(xid, rid, &k);

  Tabort(xid);

  fail_unless(j == k, "Didn't abort!");

  Tdeinit();

  Tinit();  /* Runs recovery.. */

  k = 12312;

  xid = Tbegin(); 

  Tread(xid, rid, &k);

  fail_unless(j == k, "Recovery messed something up!");

  Tcommit(xid);

  Tdeinit();

}
END_TEST


/** 
    @test
    Makes sure that aborted idempotent operations are correctly undone.
*/
START_TEST (recovery_exactlyOnceAbort) {
  
  int xid; 
  int j;
  int k;
  recordid rid;
  Tinit();
  xid = Tbegin();
  
  rid = Talloc(xid, sizeof(int));
  j = 1;
  Tincrement(xid, rid);

  Tread(xid, rid, &j);
 
  Tcommit(xid);

  xid = Tbegin();

  Tincrement(xid, rid);
  Tread(xid, rid, &k);
  fail_unless(j == k-1, NULL);
  Tabort(xid);
  xid = Tbegin();
  Tread(xid, rid, &k);
  fail_unless(j == k, "didn't abort?");
  Tcommit(xid);

  Tdeinit();
  Tinit();

  xid = Tbegin();

  Tread(xid, rid, &k);
  fail_unless(j == k, "Recovery didn't abort correctly");
  Tcommit(xid);
  Tdeinit();

}
END_TEST

/**
   @test 
   Check the CLR mechanism with an aborted logical operation, and multipl Tinit()/Tdeinit() cycles.
*/
START_TEST(recovery_clr) {
  recordid rid;
  int xid;
  int j;
  int k;

  DEBUG("\n\nStart CLR test\n\n");

  Tinit();
  
  xid = Tbegin();

  rid = Talloc(xid, sizeof(int));
	       
  Tread(xid, rid, &j);

  Tcommit(xid);
  xid = Tbegin();

  Tincrement(xid, rid);

  Tabort(xid); 
  
  xid = Tbegin(); 
  
  Tread(xid, rid, &k);
  
  Tcommit(xid);
  
  fail_unless(j == k, NULL); 

  Tdeinit(); 


  Tinit();
  Tdeinit();

  Tinit();

  xid = Tbegin();

  Tread(xid, rid, &k);

  Tcommit(xid);

  fail_unless(j == k, NULL);

  Tdeinit();
  Tinit();

  xid = Tbegin();

  Tread(xid, rid, &k);

  Tcommit(xid);

  fail_unless(j == k, NULL);

  Tdeinit();


} END_TEST

void simulateBufferManagerCrash();
extern int numActiveXactions;
/** 
    @test

    Tests the undo phase of recovery by simulating a crash, and calling Tinit(). */
START_TEST(recovery_crash) {
  int xid;
  recordid rid;
  int j;

  Tinit();
  
  xid = Tbegin();
  rid = Talloc(xid, sizeof(int));

  j = 0;

  Tset(xid, rid, &j);

  Tincrement(xid, rid);
  Tincrement(xid, rid);
  Tincrement(xid, rid);

  Tincrement(xid, rid);
  Tincrement(xid, rid);
  Tincrement(xid, rid);

  Tincrement(xid, rid);
  Tincrement(xid, rid);
  Tincrement(xid, rid);
  
  /* RID = 9. */

  Tread(xid, rid, &j);
  fail_unless(j == 9, "Increment not working?");


  Tcommit(xid);
  xid = Tbegin();
  
  Tdecrement(xid, rid);
  Tdecrement(xid, rid);
  Tdecrement(xid, rid);

  /* RID = 6. */

  Tread(xid, rid, &j);
  fail_unless(j == 6, "Decrement not working?");

  simulateBufferManagerCrash();
  closeLogWriter();
  numActiveXactions = 0;

  Tinit();

  Tread(xid, rid, &j);

  fail_unless(j == 9, "Recovery didn't roll back in-progress xact!");

  Tdeinit();
  Tinit();

  Tread(xid, rid, &j);

  fail_unless(j == 9, "Recovery failed on second re-open.");

  Tdeinit();

} END_TEST
/**
   @test
   
   Tests recovery when more than one transaction is in progress at the time of the crash.
*/
START_TEST (recovery_multiple_xacts) {
  int xid1, xid2, xid3, xid4;
  recordid rid1, rid2, rid3, rid4;
  int j1, j2, j3, j4, k;
  Tinit();
  j1 = 1;
  j2 = 2; 
  j3 = 4;
  j4 = 3;
  xid1 = Tbegin();
  rid1 = Talloc(xid1, sizeof(int));

  xid2 = Tbegin();

  xid3 = Tbegin();

  Tset(xid1, rid1, &j1);
  
  rid2 = Talloc(xid2, sizeof(int));
  rid3 = Talloc(xid3, sizeof(int));
  Tread(xid3, rid3, &k);
  
  Tset(xid3, rid3, &j3);

  Tcommit(xid3);
  xid3 = Tbegin();
  
  Tincrement(xid3, rid3);
  Tset(xid2, rid2, &j2);
  Tcommit(xid1);
  

  xid4 = Tbegin();
  Tcommit(xid2);
  
  rid4 = Talloc(xid4, sizeof(int));
  Tset(xid4, rid4, &j4);
  Tincrement(xid4, rid4);
  Tcommit(xid4);

  xid1 = Tbegin();
  k = 100000;
  Tset(xid1, rid1,&k);
  xid2 = Tbegin();
  Tdecrement(xid2, rid2);

  Tdecrement(xid2, rid2);
  Tdecrement(xid2, rid2);
  Tdecrement(xid2, rid2);
  Tdecrement(xid2, rid2);
  Tincrement(xid1, rid1);
  Tset(xid1, rid1,&k);
  
  /*simulate crash */

  simulateBufferManagerCrash();
  closeLogWriter();
  numActiveXactions = 0;
  Tinit();
  Tdeinit();
  Tinit();
  xid1 = Tbegin();
  xid2 = Tbegin();
  xid3 = Tbegin();
  xid4 = Tbegin();

  Tread(xid1, rid1, &j1);
  Tread(xid2, rid2, &j2);
  Tread(xid3, rid3, &j3);
  Tread(xid4, rid4, &j4);

  fail_unless(j1 == 1, NULL);
  fail_unless(j2 == 2, NULL);
  fail_unless(j3 == 4, NULL);
  fail_unless(j4 == 4, NULL);
  assert(j4 == 4);
  Tdeinit();
} END_TEST


/** 
  Add suite declarations here
*/
Suite * check_suite(void) {
  Suite *s = suite_create("recovery_suite");
  /* Begin a new test */
  TCase *tc = tcase_create("recovery");

  /* Sub tests are added, one per line, here */
  tcase_add_test(tc, recovery_idempotent);
  tcase_add_test(tc, recovery_exactlyOnce);

  tcase_add_test(tc, recovery_idempotentAbort);
  tcase_add_test(tc, recovery_exactlyOnceAbort);

  tcase_add_test(tc, recovery_clr);
  tcase_add_test(tc, recovery_crash);
  tcase_add_test(tc, recovery_multiple_xacts);

  /* --------------------------------------------- */
  tcase_add_checked_fixture(tc, setup, teardown);
  suite_add_tcase(s, tc);

  return s;
}

#include "../check_setup.h"
