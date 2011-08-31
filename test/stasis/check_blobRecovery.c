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
#include "../check_includes.h"

#include <stasis/transactional.h>
#include <stasis/logger/logger2.h>
#include <stasis/truncation.h>
#include <stasis/util/random.h>

#include <assert.h>

#define LOG_NAME   "check_blobRecovery.log"

#define ARRAY_SIZE 20321


static void arraySet(int * a, int mul) {
  int i;

  for ( i = 0 ; i < ARRAY_SIZE; i++) {
    a[i]= mul*i;
  }
}

static int arryCmp(int * a, int * b) {
  return memcmp(a,b,ARRAY_SIZE*sizeof(int));
}

const int NUM_BLOBS = 1000;
const int BLOB_SIZE = PAGE_SIZE * 4;
const int NUM_OPS = 5000;

static byte * gen_blob(int i) {
  static uint16_t buf[4096*4/sizeof(uint16_t)];

  for(int j = 0; j < BLOB_SIZE/sizeof(uint16_t); j++) {
    buf[j] = i+j;
  }

  return (byte*)buf;
}

START_TEST(recoverBlob__randomized) {
  static uint16_t buf[4096*4/sizeof(uint16_t)];

  recordid * blobs = malloc(sizeof(recordid) * NUM_BLOBS);

  for(int i = 0; i < NUM_BLOBS; i++) {
    blobs[i].size = -1;
  }
  Tinit();
  int xid = Tbegin();
  for(int i = 0; i < NUM_OPS; i++) {
    if(!(i % 100)) { printf("."); fflush(stdout); }
    int blobid = stasis_util_random64(NUM_BLOBS);
    if(blobs[blobid].size == -1) {
      blobs[blobid] = Talloc(xid, BLOB_SIZE);
      Tset(xid, blobs[blobid], gen_blob(blobid));
    } else if (stasis_util_random64(2)) {
      Tread(xid, blobs[blobid], buf);
      assert(!memcmp(buf, gen_blob(blobid),BLOB_SIZE));
    } else {
      Tdealloc(xid, blobs[blobid]);
      blobs[blobid].size = -1;
    }
  }
  Tcommit(xid);
  Tdeinit();

} END_TEST


/**
    @test
    Simple test: Insert some stuff.  Commit.  Call Tdeinit().  Call
    Tinit() (Which initiates recovery), and see if the stuff we
    inserted is still there.

    Only performs idempotent operations (Tset).
*/
START_TEST (recoverBlob__idempotent) {
  int xid;


  int j[ARRAY_SIZE];
  int k[ARRAY_SIZE];

  recordid rid;
  Tinit();
  xid = Tbegin();

  rid = Talloc(xid, ARRAY_SIZE * sizeof(int));
  assert(TrecordSize(xid, rid) == (ARRAY_SIZE * sizeof(int)));
  arraySet(j, 1);

  Tset(xid, rid, j);

  Tread(xid, rid, k);
  fail_unless(!memcmp(j, k, ARRAY_SIZE * sizeof(int)), "Get/Set broken?");

  arraySet(k, 12312);

  Tcommit(xid);

  xid = Tbegin();

  Tread(xid, rid, k);

  fail_unless(!memcmp(j, k, ARRAY_SIZE * sizeof(int)), "commit broken");

  Tcommit(xid);

  Tdeinit();

  Tinit();  /* Runs recoverBlob_.. */

  arraySet(k, 12312);

  xid = Tbegin();

  Tread(xid, rid, k);

  fail_unless(!memcmp(j, k, ARRAY_SIZE * sizeof(int)), "Recovery messed something up!");

  Tcommit(xid);

  Tdeinit();

}
END_TEST

/*
    @test
    Simple test: Alloc a blob, commit.  Call Tincrement on it, and
    remember its value and commit.  Then, call Tdeinit() and Tinit()
    (Which initiates recovery), and see if the value changes.

    @todo:  Until we have a non-idempotent operation on blobs, this test can't be written.
*/
/* START_TEST (recoverBlob__exactlyOnce) {
}
END_TEST

*/
/**
    @test
    Makes sure that aborted idempotent operations are correctly undone.
*/
START_TEST (recoverBlob__idempotentAbort) {

  int xid;
  int j[ARRAY_SIZE];
  int k[ARRAY_SIZE];
  recordid rid;

  Tinit();
  xid = Tbegin();

  rid = Talloc(xid, ARRAY_SIZE * sizeof(int));

  arraySet(j, 1);

  Tset(xid, rid, j);

  arraySet(k, 2);
  Tread(xid, rid, k);

  fail_unless(!memcmp(j, k, ARRAY_SIZE * sizeof(int)), "Get/set broken?!");

  Tcommit(xid);

  xid = Tbegin();
  arraySet(k, 3);
  Tread(xid, rid, k);

  fail_unless(!memcmp(j, k, ARRAY_SIZE * sizeof(int)), "commit broken?");

  Tcommit(xid);
  xid = Tbegin();
  arraySet(k, 2);

  Tset(xid, rid, k);

  arraySet(k, 4);
  Tread(xid, rid, k);

  arraySet(j, 2);

  fail_unless(!memcmp(j, k, ARRAY_SIZE * sizeof(int)),NULL);

  Tabort(xid);

  xid = Tbegin();
  arraySet(j, 1);
  arraySet(k, 4);

  Tread(xid, rid, &k);

  Tabort(xid);

  fail_unless(!memcmp(j, k, ARRAY_SIZE * sizeof(int)),"Didn't abort!");

  Tdeinit();

  Tinit();  /* Runs recovery.. */

  arraySet(k, 12312);

  xid = Tbegin();

  Tread(xid, rid, k);

  fail_unless(!memcmp(j, k, ARRAY_SIZE * sizeof(int)),"recovery messed something up.");

  Tcommit(xid);

  Tdeinit();

}
END_TEST


/**
    @test Makes sure that aborted non-idempotent operations are
    correctly undone.  Curently, we don't support such operations on
    blobs, so this test is not implemented.

    @todo  need non-idempotent blob operation to implement this test.
*/
/* START_TEST (recoverBlob__exactlyOnceAbort) {
}
END_TEST
*/
/**
   @test Check the CLR mechanism with an aborted logical operation, and multiple Tinit()/Tdeinit() cycles.

   @todo  need blob operation w/ logical undo to implement this.
*/
/*START_TEST(recoverBlob__clr) {
} END_TEST
*/
/**
    @test

    Tests the undo phase of recovery by simulating a crash, and calling Tinit().

    @todo logical operations, if they are ever supported for blobs.

*/
START_TEST(recoverBlob__crash) {
  int xid;
  recordid rid;
  int j[ARRAY_SIZE];
  int k[ARRAY_SIZE];

  Tinit();

  xid = Tbegin();
  rid = Talloc(xid, sizeof(int)* ARRAY_SIZE);

  arraySet(j, 3);

  Tset(xid, rid, &j);

  arraySet(j, 9);

  Tset(xid, rid, &j);


  /* RID = 9. */

  Tread(xid, rid, &j);

  arraySet(k, 9);
  fail_unless(!memcmp(j,k,ARRAY_SIZE * sizeof(int)), "set not working?");


  Tcommit(xid);
  xid = Tbegin();

  arraySet(k, 6);

  Tset(xid, rid, &k);

  /* RID = 6. */

  Tread(xid, rid, &j);
  fail_unless(!memcmp(j,k,ARRAY_SIZE * sizeof(int)), NULL);
  TuncleanShutdown();

  printf("\nreopen 1\n");
  Tinit();
  printf("\nreopen 1 done\n");

  Tread(xid, rid, &j);

  arraySet(k, 9);

  fail_unless(!memcmp(j,k,ARRAY_SIZE * sizeof(int)),
              "Recovery didn't roll back in-progress xact!");

  Tdeinit();

  printf("\nreopen 2\n");
  Tinit();

  Tread(xid, rid, &j);

  assert(!memcmp(j,k,ARRAY_SIZE * sizeof(int)));

  fail_unless(!memcmp(j,k,ARRAY_SIZE * sizeof(int)),
              "Recovery failed on second re-open.");

  Tdeinit();

} END_TEST

/**
   @test Tests blob allocation and deallocation, and recovery
 */

START_TEST(recoverBlob__allocation) {
  Tinit();
  int xid = Tbegin();
  int arry1[ARRAY_SIZE];
  int arry2[ARRAY_SIZE];
  int arry3[ARRAY_SIZE];
  int arry4[ARRAY_SIZE];
  int scratch[ARRAY_SIZE];

  arraySet(arry1, 1);
  arraySet(arry2, 2);
  arraySet(arry3, 3);
  arraySet(arry4, 4);

  recordid rid1, rid2, rid3, rid4;

  // Abort w/ allocation (no set)
  rid1 = Talloc(xid, ARRAY_SIZE * sizeof(int));
  assert(TrecordType(xid,rid1)==BLOB_SLOT);
  Tabort(xid);
  xid = Tbegin();
  assert(TrecordType(xid,rid1)==INVALID_SLOT);

  // Abort w/ allocation (set)
  rid2 = Talloc(xid, ARRAY_SIZE * sizeof(int));
  assert((!memcmp(&rid1,&rid2,sizeof(rid1)))||
         TrecordType(xid,rid1)==INVALID_SLOT);
  assert(TrecordType(xid,rid2)==BLOB_SLOT);
  Tset(xid,rid1,arry1);
  Tabort(xid);

  xid = Tbegin();
  assert(TrecordType(xid,rid1)==INVALID_SLOT);
  assert(TrecordType(xid,rid2)==INVALID_SLOT);

  // Abort w/ committed alloc (no set)
  rid2 = Talloc(xid, ARRAY_SIZE * sizeof(int));
  Tset(xid, rid2, arry2);
  Tcommit(xid);

  // Abort alloc of rid A + dealloc, alloc + overwrite rid B
  xid = Tbegin();
  rid3 = Talloc(xid, ARRAY_SIZE * sizeof(int));
  Tread(xid, rid2, scratch); assert(!arryCmp(arry2,scratch));
  Tset(xid, rid3, arry3);
  Tread(xid, rid2, scratch); assert(!arryCmp(arry2,scratch));
  Tread(xid, rid3, scratch); assert(!arryCmp(arry3,scratch));
  Tdealloc(xid,rid2);
  rid4 = Talloc(xid, ARRAY_SIZE * sizeof(int));
  Tset(xid, rid4, arry4);
  Tabort(xid);

  xid = Tbegin();
  Tread(xid, rid2, scratch); assert(!arryCmp(arry2,scratch));
  assert((!memcmp(&rid2,&rid4,sizeof(rid2))) ||
         TrecordType(xid,rid4) == INVALID_SLOT);
  Tcommit(xid);
  Tdeinit();

  // make sure downing + upping stasis doesn't change state.

  Tinit();
  xid = Tbegin();
  Tread(xid, rid2, scratch); assert(!arryCmp(arry2,scratch));
  assert((!memcmp(&rid2,&rid4,sizeof(rid2))) ||
         TrecordType(xid,rid4) == INVALID_SLOT);
  Tabort(xid);
  Tdeinit();

  Tinit();
  xid = Tbegin();
  Tread(xid, rid2, scratch); assert(!arryCmp(arry2,scratch));
  assert((!memcmp(&rid2,&rid4,sizeof(rid2))) ||
         TrecordType(xid,rid4) == INVALID_SLOT);
  Tcommit(xid);
  Tdeinit();

} END_TEST

/**
   @test Tests recovery when more than one transaction is in progress
   at the time of the crash.  This test is interesting because blob
   operations from multiple transactions could hit the same page.

   @todo implement this one transactions may write subset of blob pages
*/
/*START_TEST (recoverBlob__multiple_xacts) {
} END_TEST*/

/**
  Add suite declarations here
*/
Suite * check_suite(void) {
  Suite *s = suite_create("recovery_suite");
  /* Begin a new test */
  TCase *tc = tcase_create("recovery");

  tcase_set_timeout(tc, 0); // disable timeouts
  if(LOG_TO_MEMORY != stasis_log_type) {
    /* void * foobar; */  /* used to supress warnings. */
    /* Sub tests are added, one per line, here */

    tcase_add_test(tc, recoverBlob__randomized);

    tcase_add_test(tc, recoverBlob__idempotent);
    tcase_add_test(tc, recoverBlob__idempotentAbort);

    tcase_add_test(tc, recoverBlob__allocation);

    tcase_add_test(tc, recoverBlob__crash);

    // The following tests are analagous to those in check_recovery,
    // but would test functionality that hasn't been implemented for blobs.

    //tcase_add_test(tc, recoverBlob__exactlyOnce);
    //tcase_add_test(tc, recoverBlob__exactlyOnceAbort);
    //tcase_add_test(tc, recoverBlob__clr);

    //tcase_add_test(tc, recoverBlob__multiple_xacts);
  }
  /* --------------------------------------------- */
  tcase_add_checked_fixture(tc, setup, teardown);
  suite_add_tcase(s, tc);

  return s;
}

#include "../check_setup.h"
