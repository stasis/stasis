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
#include "../check_includes.h"

#include <lladd/transactional.h>

#include <assert.h>
#include <lladd/hash.h>
#include <limits.h>
#include <math.h>

#define LOG_NAME   "check_linearHash.log"

/** @test 
    executes each of the insert / remove / lookup operations a few times.
*/
//#define NUM_ENTRIES 100000   
#define NUM_ENTRIES 10000
/* #define NUM_ENTRIES 1000  */
/*#define NUM_ENTRIES 100  */

/**
   @test Runs some simple tests on the hash() function.  Not comprehensive enough.
   @todo the checkHashFcn test is broken.
*/
START_TEST(checkHashFcn) {
  int i;
  srandom(12312313);
  for(i = 0; i < 100000;i++) {
    int j = (int) (100000.0*random()/(RAND_MAX+1.0));  /* int for CRC. */
    int k = (int) 2+(30.0*random()/(RAND_MAX+1.0));  /* number of bits in result. */

    unsigned long first = hash(&j, sizeof(int), k, ULONG_MAX);
    int boundary = first + 10;
    unsigned long second = hash(&j, sizeof(int), k, boundary);
    assert(first == second);
    unsigned long third =  hash(&j, sizeof(int), k+1, ULONG_MAX);
    assert((first == third) || (powl(2,k)+ first == third));
  }
} END_TEST

/**
   @test Insert some stuff into a linear hash, delete some stuff, and
   make sure that abort() and commit() work.
*/
START_TEST(simpleLinearHashTest)
{
  Tinit();

  int xid = Tbegin();

  recordid hashRoot =  ThashAlloc(xid, sizeof(int), sizeof(recordid));

  for(int i = 0; i < NUM_ENTRIES; i++) {
    recordid rid;
    rid.page=i+1;
    rid.slot=i+2;
    rid.size=i+3;

    /*    assert(isNullRecord(lHtInsert(xid, hash, &i, sizeof(int), rid))); 
	  assert(!isNullRecord(lHtInsert(xid, hash, &i, sizeof(int), rid))); */

    TnaiveHashInsert(xid, hashRoot, &i, sizeof(int), &rid, sizeof(recordid));
    assert(TnaiveHashLookup(xid, hashRoot, &i, sizeof(int), &rid, sizeof(recordid)));

    assert(rid.page == i+1);
    assert(rid.slot == i+2);
    assert(rid.size == i+3);


    if(! (i % 1000)) {
      printf("%d\n", i);
      fflush(NULL);
    }

  }
  printf("Done inserting.\n");
  fflush(NULL);

  for(int i = 0; i < NUM_ENTRIES; i+=10) {
    /*recordid rid = lHtRemove(xid, hash, &i, sizeof(int)); */
    recordid rid;
    assert(TnaiveHashLookup(xid, hashRoot, &i, sizeof(int), &rid, sizeof(recordid)));
    assert(rid.page == (i+1));
    assert(rid.slot == (i+2));
    assert(rid.size == (i+3));
    assert(TnaiveHashDelete(xid, hashRoot, &i, sizeof(int), sizeof(recordid)));
    assert(!TnaiveHashDelete(xid, hashRoot, &i, sizeof(int), sizeof(recordid)));
    assert(!TnaiveHashLookup(xid, hashRoot, &i, sizeof(int), &rid, sizeof(recordid)));

  }
	
  printf("Done deleting mod 10.\n");
  fflush(NULL);


  for(int i = 0; i < NUM_ENTRIES; i++) {
    recordid rid;
    if(i % 10) {
      assert(TnaiveHashLookup(xid, hashRoot, &i, sizeof(int), &rid, sizeof(recordid)));
      assert(rid.page == (i+1));
      assert(rid.slot == (i+2));
      assert(rid.size == (i+3));
    } else {
      assert(!TnaiveHashLookup(xid, hashRoot, &i, sizeof(int), &rid, sizeof(recordid)));
    }
  }

  printf("Done checking mod 10.\n");

  Tcommit(xid);
  xid = Tbegin();
  recordid rid;
  for(int i = 0; i < NUM_ENTRIES; i++) {

    if(i % 10) {
      assert(TnaiveHashLookup(xid, hashRoot, &i, sizeof(int), &rid, sizeof(recordid)));
      TnaiveHashDelete(xid, hashRoot, &i, sizeof(int), sizeof(recordid));
      assert(rid.page == (i+1));
      assert(rid.slot == (i+2));
      assert(rid.size == (i+3));
    } else {
      assert(!TnaiveHashLookup(xid, hashRoot, &i, sizeof(int), &rid, sizeof(recordid)));
      TnaiveHashDelete(xid, hashRoot, &i, sizeof(int), sizeof(recordid));
    }
  }

  printf("Done deleting rest.\n");
  fflush(NULL);

  for(int i = 0; i < NUM_ENTRIES; i++) {
    assert(!TnaiveHashLookup(xid, hashRoot, &i, sizeof(int),  &rid, sizeof(recordid)));
  }

  printf("Aborting..\n");
  fflush(NULL);
  Tabort(xid);
  printf("done aborting..\n");
  fflush(NULL);

  xid = Tbegin();

  for(int i = 0; i < NUM_ENTRIES; i++) {
    if(i % 10) {
      assert( TnaiveHashLookup(xid, hashRoot, &i, sizeof(int), &rid, sizeof(recordid)));
      assert(rid.page == (i+1));
      assert(rid.slot == (i+2));
      assert(rid.size == (i+3));
    } else {
      assert(!TnaiveHashLookup(xid, hashRoot, &i, sizeof(int), &rid, sizeof(recordid)));
    }
  }
  printf("done checking..\n");
  fflush(NULL);

  Tcommit(xid);
  
  Tdeinit();

}
END_TEST
#define NUM_ENTRIES_XACT 10000
START_TEST(transactionalLinearHashTest)
{
  Tinit();

  int xid = Tbegin();

	recordid foo = Talloc(xid, 1);

	printf("%d %d %ld\n", foo.page, foo.slot, foo.size);
	
  recordid hashRoot =  ThashAlloc(xid, sizeof(int), sizeof(recordid));

	printf("%d %d %ld", hashRoot.page, hashRoot.slot, hashRoot.size);
	
// Insert some entries, see if they stick around. 
	
  int i;

  for(i = 0; i < NUM_ENTRIES_XACT; i+=10) {
		recordid insMe;
		insMe.page = i;
		insMe.slot = i+1;
		insMe.size = i+2;
		TnaiveHashInsert(xid, hashRoot, &i, sizeof(int), &insMe, sizeof(recordid));
  }	  
	
  for(i = 0; i < NUM_ENTRIES_XACT; i+=10) {
    recordid theVal;
    assert(TnaiveHashLookup(xid, hashRoot, &i, sizeof(int), &theVal, sizeof(recordid)));
    assert(theVal.page == i);
    assert(theVal.slot == i+1);
    assert(theVal.size == i+2);
  }
	
  Tcommit(xid);
	
  xid = Tbegin();
	
  for(i = 0; i < NUM_ENTRIES_XACT; i++) {
    if(!(i%10)) {
      recordid theVal;
      assert(TnaiveHashLookup(xid, hashRoot, &i, sizeof(int), &theVal, sizeof(recordid)));
      assert(theVal.page == i);
      assert(theVal.slot == i+1);
      assert(theVal.size == i+2);
    } else {
      recordid insMe;
      insMe.page = i;
      insMe.slot = i+1;
      insMe.size = i+2;
      TnaiveHashInsert(xid, hashRoot, &i, sizeof(int), &insMe, sizeof(recordid));
    }
  }
	
  Tabort(xid);
  Tdeinit();
  Tinit();
  xid = Tbegin();
  ThashOpen(xid, hashRoot, sizeof(int), sizeof(recordid));
  for(i = 0; i < NUM_ENTRIES_XACT; i++) {
    if(!(i%10)) {
      recordid theVal;
      assert(TnaiveHashLookup(xid, hashRoot, &i, sizeof(int), &theVal, sizeof(recordid)));
      assert(theVal.page == i);
      assert(theVal.slot == i+1);
      assert(theVal.size == i+2);	
    } else {
      recordid theVal;
      assert(!TnaiveHashLookup(xid, hashRoot, &i, sizeof(int), &theVal, sizeof(recordid)));
    }
  }
  Tabort(xid);
  Tdeinit();
	
} END_TEST



Suite * check_suite(void) {
  Suite *s = suite_create("linearHash");
  /* Begin a new test */
  TCase *tc = tcase_create("simple");


  /* Sub tests are added, one per line, here */

  /*  tcase_add_test(tc, checkHashFcn); */
  tcase_add_test(tc, simpleLinearHashTest);
  tcase_add_test(tc, transactionalLinearHashTest);

  /* --------------------------------------------- */
  
  tcase_add_checked_fixture(tc, setup, teardown);

  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
