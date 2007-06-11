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
/*#include <assert.h> */

#include <lladd/transactional.h>

#define LOG_NAME   "check_lht.log"

/** Insert 1000 values into a hashtable.  
    @test */
static void test_lht_insert1000(int xid, lladdHash_t * ht) {
  int j;
  for(j =0 ; j < 1000; j++) {
    lHtInsert(xid, ht, &j, sizeof(int), &j, sizeof(int));
  }
}

/** Lookup 1000 vales from the hashtable 
    @test */
static void test_lht_lookup1000(int xid, lladdHash_t * ht, int shouldBeThere) {
  int j, k;
  char * message = shouldBeThere ? "Couldn't lookup value" : "Found value that shouldn't exist";
  int expected_return = shouldBeThere ? 0 : -1;

  for(j = 0; j < 1000; j++) {
    int actual_return;

    k = 10000000;

    actual_return = lHtLookup(xid, ht, &j, sizeof(int), &k);
    /*    assert(expected_return == actual_return);  */
    fail_unless(expected_return == actual_return, message);
    if(shouldBeThere) {
      fail_unless(j == k, "Got incorrect value from lookup");
    }
  }
}


/** Lookup 1000 vales from the hashtable 
    @test
 */
void test_lht_remove1000(int xid, lladdHash_t * ht, int shouldBeThere) {
  int j, k;
  char * message = shouldBeThere ? "Couldn't remove value" : "Removed value that shouldn't exist";
  int expected_return = shouldBeThere ? 0 : -1;

  for(j = 0; j < 1000; j++) {

    fail_unless(expected_return == lHtRemove(xid, ht, &j, sizeof(int), &k, sizeof(int)), message);
    if(shouldBeThere) {
      fail_unless(j == k, "Got incorrect value from remove");
    }
  }
}

/** Iterate until the hash table reports no more entries, then return
    the count.  Also, make sure that key == value (assuming they're ints) */
static int test_lht_iterateCounter(int xid, lladdHash_t * ht) {
  int j = 0;
  int key, value;
  /*  int first = 1; */
  int ret;
  for(ret = lHtCurrent(xid, ht, &value); ret != -1; ret = lHtNext(xid, ht, &value)) {
    int inner_ret = lHtCurrentKey(xid, ht, &key);
    fail_unless(-1 != inner_ret, "A");
    fail_unless(key == value, "B");

    if(! (j % 10)) {
      /* Run current and current key once more, just in case. */
      ret = lHtCurrent(xid, ht, &value);
      inner_ret = lHtCurrentKey(xid, ht, &key);
      
      fail_unless(-1 != ret, "C");
      fail_unless(-1 != inner_ret, "D");
      fail_unless(key == value, "E");
    }

    j++;
  }
  return j;
}



/** Iterate until the hash table returns the desired value, and return the number of hits encountered. */
static int test_lht_iterateCountUntil(int xid, lladdHash_t * ht, const int guard) {
  int j = 0;
  int key, value;
  /*  int first = 1; */
  int ret;
  for(ret = lHtCurrent(xid, ht, &value); ret != -1 && value != guard; ret = lHtNext(xid, ht, &value)) {
    int inner_ret = lHtCurrentKey(xid, ht, &key);
    fail_unless(-1 != inner_ret, "A");
    fail_unless(key == value, "B");

    if(! (j % 10)) {
      /* Run current and current key once more, just in case. */
      ret = lHtCurrent(xid, ht, &value);
      inner_ret = lHtCurrentKey(xid, ht, &key);
      
      fail_unless(-1 != ret, "C");
      fail_unless(-1 != inner_ret, "D");
      fail_unless(key == value, "E");
    }

    j++;
  }

  fail_unless(ret != -1, "Iterator didn't find value.");
  fail_unless(guard == value, "Test case bug??");

  return j;
}



/** @test 
    Check lHtInsert for transactions that commit
*/
START_TEST(lht_insert_commit)
{
	int xid;
	lladdHash_t * ht;

	Tinit();
	xid = Tbegin();
	
	ht = lHtCreate(xid, 700);

	test_lht_insert1000(xid, ht);
	test_lht_lookup1000(xid, ht, 1);

	Tcommit(xid);
	xid = Tbegin();

	test_lht_lookup1000(xid, ht, 1);

	Tcommit(xid);
	Tdeinit();
}
END_TEST

/** @test 
    Check lHtInsert for transactions that abort
*/
START_TEST(lht_insert_abort)
{
	int xid;
	lladdHash_t * ht;
	Tinit();
	xid = Tbegin();
	ht = lHtCreate(xid, 700);

	test_lht_insert1000(xid, ht);
	test_lht_lookup1000(xid, ht, 1);

	Tabort(xid);
	xid = Tbegin();

	/*	printf("G\n"); */

	test_lht_lookup1000(xid, ht, 0);
	/*	printf("H\n"); */

	Tcommit(xid);
	Tdeinit();
}
END_TEST

/** @test 
    Check lHtRemove for transactions that commit
*/
START_TEST(lht_remove_commit)
{
	int xid;
	lladdHash_t * ht;

	Tinit();
	xid = Tbegin();

	ht = lHtCreate(xid, 700);

	test_lht_insert1000(xid, ht);
	/*	printf("A\n"); fflush(NULL); */
	test_lht_lookup1000(xid, ht, 1);

	Tcommit(xid);
	xid = Tbegin();

	test_lht_lookup1000(xid, ht, 1);
	test_lht_remove1000(xid, ht, 1);
	/*	printf("B\n"); fflush(NULL); */
	test_lht_lookup1000(xid, ht, 0);
	test_lht_lookup1000(xid, ht, 0);

	Tcommit(xid);
	xid = Tbegin();

	/*	printf("C\n"); fflush(NULL); */
	test_lht_lookup1000(xid, ht, 0);

	/*	printf("D\n"); */

	Tcommit(xid);
	Tdeinit();
}
END_TEST

/** @test 
    Check lHtRemove for transactions that abort
*/
START_TEST(lht_remove_abort)
{
	int xid;
	lladdHash_t * ht;
	Tinit();
	xid = Tbegin();

	ht = lHtCreate(xid, 700);

	test_lht_insert1000(xid, ht);
	test_lht_lookup1000(xid, ht, 1);

	Tcommit(xid);
	xid = Tbegin();

	test_lht_lookup1000(xid, ht, 1);
	test_lht_remove1000(xid, ht, 1);
	/*	printf("E\n"); */
	test_lht_lookup1000(xid, ht, 0);
	/*	printf("F\n"); */

	Tabort(xid);
	xid = Tbegin();

	/*	printf("FA\n"); */
	test_lht_lookup1000(xid, ht, 1);
	/*	printf("FB\n"); fflush(NULL); */

	Tcommit(xid);
	Tdeinit();
}
END_TEST

/**
   @test Tests lladd hash's iterator (but not the lHtPosition function)
*/
START_TEST(iterator_vanilla)
{
  int xid, hits, j;
  lladdHash_t * ht;

  Tinit();
  xid = Tbegin();

  ht = lHtCreate(xid, 700);

  test_lht_insert1000(xid, ht);
  
  lHtFirst(xid, ht, &j);

  hits = test_lht_iterateCounter(xid, ht);

  fail_unless(hits == 1000, "Wrong number of items returned by vanilla iterator.");

  Tcommit(xid);

  lHtFirst(xid, ht, &j);

  hits = test_lht_iterateCounter(xid, ht);

  fail_unless(hits == 1000, "Iterator returned wrong number of hits after commit.");
  
  Tdeinit();
}
END_TEST

/**
   @test Tests lladd hash's position function.
*/
START_TEST(iterator_position)
{
  int xid, hits, next_hits, j;
  const int thirty_nine = 39;
  lladdHash_t * ht;
  Tinit();
  xid = Tbegin();

  ht = lHtCreate(xid, 700);

  test_lht_insert1000(xid, ht);
  
  lHtFirst(xid, ht, &j);

  hits = test_lht_iterateCountUntil(xid, ht, 39);

  lHtCurrent(xid, ht, &j);

  /*  printf("Iterator at: %d\n", j); */
  next_hits = test_lht_iterateCounter(xid, ht);

  fail_unless((hits+next_hits) == 1000, "Wrong number of items returned by iterator setup.");

  Tcommit(xid);

  lHtPosition(xid, ht, &thirty_nine, sizeof(int));

  lHtCurrent(xid, ht, &j);

  /*  printf("Iterator at: %d\n", j); */
  hits = test_lht_iterateCounter(xid, ht);
  /*  printf("hits = %d, next_hits = %d\n", hits, next_hits); */
  
  fail_unless(hits == next_hits, "Iterator returned wrong number of hits after position.");
  
  Tdeinit();
}
END_TEST

/** 
  Add suite declarations here
*/
Suite * check_suite(void) {
  Suite *s = suite_create("lladd_hash");
  /* Begin a new test */
  TCase *tc = tcase_create("insert_remove_lookup");

  /* Sub tests are added, one per line, here */
  tcase_add_test(tc, lht_insert_commit);
  tcase_add_test(tc, lht_insert_abort);
  tcase_add_test(tc, lht_remove_commit);
  tcase_add_test(tc, lht_remove_abort);
  /* --------------------------------------------- */
  suite_add_tcase(s, tc);

  tc = tcase_create("iterator");

  tcase_add_test(tc, iterator_vanilla);
  tcase_add_test(tc, iterator_position);

  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
