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
/*#define NUM_ENTRIES 10000*/
#define NUM_ENTRIES 1000  
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
    assert((first == third) || (pow(2,k)+ first == third));
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

    TlogicalHashInsert(xid, hashRoot, &i, sizeof(int), &rid, sizeof(recordid));
    assert(TnaiveHashLookup(xid, hashRoot, &i, sizeof(int), &rid, sizeof(recordid)));

    assert(rid.page == i+1);
    assert(rid.slot == i+2);
    assert(rid.size == i+3);


    if(! (i % 1000)) {
      printf("%d\n", i);
      /* flush(NULL); */
    }

  }
  printf("Done inserting.\n");
  /*  fflush(NULL); */

  for(int i = 0; i < NUM_ENTRIES; i+=10) {
    /*recordid rid = lHtRemove(xid, hash, &i, sizeof(int)); */
    recordid rid;
    assert(TnaiveHashLookup(xid, hashRoot, &i, sizeof(int), &rid, sizeof(recordid)));
    assert(rid.page == (i+1));
    assert(rid.slot == (i+2));
    assert(rid.size == (i+3));
    TlogicalHashDelete(xid, hashRoot, &i, sizeof(int), &rid, sizeof(recordid));

  }

  printf("Done deleting mod 10.\n");
  /*  fflush(NULL); */


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
      TlogicalHashDelete(xid, hashRoot, &i, sizeof(int), &rid, sizeof(recordid));
      assert(rid.page == (i+1));
      assert(rid.slot == (i+2));
      assert(rid.size == (i+3));
    } else {
      assert(!TnaiveHashLookup(xid, hashRoot, &i, sizeof(int), &rid, sizeof(recordid)));
      TlogicalHashDelete(xid, hashRoot, &i, sizeof(int), &rid, sizeof(recordid));
    }
  }

  printf("Done deleting rest.\n");
  /*  fflush(NULL);*/

  for(int i = 0; i < NUM_ENTRIES; i++) {
    assert(!TnaiveHashLookup(xid, hashRoot, &i, sizeof(int),  &rid, sizeof(recordid)));
  }


  printf("Aborting..\n");
  /*  fflush(NULL); */
  Tabort(xid);
  printf("done aborting..\n");
  /*  fflush(NULL); */

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
  /*  fflush(NULL); */

  Tcommit(xid);
  
  Tdeinit();

}
END_TEST
#define NUM_ITERATOR_ENTRIES 2000

START_TEST(check_linearHashIterator) {
  Tinit();
  
  int xid = Tbegin();
  
  recordid rid = ThashAlloc(xid, sizeof(int), sizeof(int));
  
  printf("Testing iterator.\n");  
  int key;
  int val;
  
//  int * keySeen = calloc(NUM_ITERATOR_ENTRIES, sizeof(int));
  for(int i = 0; i < NUM_ITERATOR_ENTRIES; i++) {
    key = i;
    val = NUM_ITERATOR_ENTRIES * key;
    TnaiveHashInsert(xid, rid, &key, sizeof(int), &val, sizeof(int));
  }
  Tcommit(xid);
  
  xid = Tbegin();
  for(int i = 0; i < NUM_ITERATOR_ENTRIES; i++) {
    key = i;
    TlogicalHashLookup(xid, rid, &key, sizeof(int), &val, sizeof(int));
    assert(key == i);
    assert(val == NUM_ITERATOR_ENTRIES * key);
  }
  Tcommit(xid);
  xid = Tbegin();
  linearHash_iterator * it = TlogicalHashIterator(xid, rid);
  
  linearHash_iteratorPair next = TlogicalHashIteratorNext(xid,rid, it, sizeof(int), sizeof(int));
  assert(next.key );
  while(next.key != NULL) {
    //	printf("%d -> %d\n", *(next.key), *(next.value));
	next = TlogicalHashIteratorNext(xid, rid, it, sizeof(int), sizeof(int));
  }
  TlogicalHashIteratorFree(it);
  Tcommit(xid);
  Tdeinit();
} END_TEST
Suite * check_suite(void) {
  Suite *s = suite_create("linearHash");
  /* Begin a new test */
  TCase *tc = tcase_create("simple");


  /* Sub tests are added, one per line, here */

  /*  tcase_add_test(tc, checkHashFcn); */
  tcase_add_test(tc, simpleLinearHashTest);
  tcase_add_test(tc, check_linearHashIterator);
  /* --------------------------------------------- */
  
  tcase_add_checked_fixture(tc, setup, teardown);

  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
