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

#include <assert.h>

#define LOG_NAME   "check_pageOrientedListNTA.log"
/** @test */
#define NUM_ENTRIES 3000
START_TEST(emptyIterator) {
  Tinit();
  int xid = Tbegin();
  recordid list = TpagedListAlloc(xid);
  lladd_pagedList_iterator * it = TpagedListIterator(xid, list);

  int keySize;
  int valueSize;
  int * key = 0;
  int ** bkey = &key;
  recordid * value = 0;
  recordid ** bvalue = &value;

  while(TpagedListNext(xid, it, (byte**)bkey, &keySize, (byte**)bvalue, &valueSize)) {
    abort();
  }
  Tcommit(xid);
  Tdeinit();
} END_TEST

START_TEST(pagedListCheck) {
  Tinit();

  int xid = Tbegin();

  recordid list = TpagedListAlloc(xid);

  int a;
  recordid b;
  int i;

  printf("\n");
  for(i = 0; i < NUM_ENTRIES; i++) {

    if(!(i % (NUM_ENTRIES/10))) {
      printf("."); fflush(stdout);
    }

    a = i;
    b.page = i+1;
    b.slot = i+2;
    b.size = i+3;

    int ret;

    {
      byte * t;

      ret = TpagedListFind(xid, list, (byte*)&a, sizeof(int), &t);
      assert(-1 == ret);
    }
    ret = TpagedListInsert(xid, list, (byte*)&a, sizeof(int), (byte*)&b, sizeof(recordid));

    assert(!ret);

    recordid * bb;
    recordid ** bbb = &bb;
    ret = TpagedListFind(xid, list, (byte*)&a, sizeof(int), (byte**)bbb);

    assert(ret == sizeof(recordid));
    assert(!memcmp(bb, &b, sizeof(recordid)));
  }
  Tcommit(xid);
  printf("\n");
  xid = Tbegin();
  for(i = 0; i < NUM_ENTRIES; i++ ) {

    if(!(i % (NUM_ENTRIES/10))) {
      printf("."); fflush(stdout);
    }

    a = i;
    b.page = i+1;
    b.slot = i+2;
    b.size = i+3;

    recordid * bb;
    recordid ** bbb = &bb;
    int ret = TpagedListFind(xid, list, (byte*)&a, sizeof(int), (byte**)bbb);

    assert(ret == sizeof(recordid));
    assert(!memcmp(bb, &b, sizeof(recordid)));


    if(!(i % 10)) {

      ret = TpagedListRemove(xid, list, (byte*)&a, sizeof(int));

      assert(ret);

      free(bb);
      bb = 0;

      ret = TpagedListFind(xid, list, (byte*)&a, sizeof(int), (byte**)bbb);

      assert(-1==ret);
      assert(!bb);
    }
  }
  Tabort(xid);

  xid = Tbegin();
  printf("\n");
  for(i = 0; i < NUM_ENTRIES; i++) {

    if(!(i % (NUM_ENTRIES/10))) {
      printf("."); fflush(stdout);
    }

    a = i;
    b.page = i+1;
    b.slot = i+2;
    b.size = i+3;

    recordid * bb;
    recordid ** bbb = &bb;
    int ret = TpagedListFind(xid, list, (byte*)&a, sizeof(int), (byte**)bbb);

    assert(ret == sizeof(recordid));
    assert(!memcmp(bb, &b, sizeof(recordid)));
  }

  byte * seen = calloc(NUM_ENTRIES, sizeof(byte));

  lladd_pagedList_iterator * it = TpagedListIterator(xid, list);

  int keySize;
  int valueSize;
  int * key = 0;
  int ** bkey = &key;
  recordid * value = 0;
  recordid ** bvalue = &value;

  while(TpagedListNext(xid, it, (byte**)bkey, &keySize, (byte**)bvalue, &valueSize)) {
    assert(!seen[*key]);
    seen[*key] = 1;

    assert(value->page == *key+1);
    assert(value->slot == *key+2);
    assert(value->size == *key+3);


    free(key);
    free(value);
    key = 0;
    value = 0;
  }

  for(i = 0; i < NUM_ENTRIES; i++) {
    assert(seen[i] == 1);
  }

  Tcommit(xid);
  Tdeinit();

} END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("pageOrientedList");
  /* Begin a new test */
  TCase *tc = tcase_create("pageOrientedList");
  tcase_set_timeout(tc, 0); // disable timeouts

  /* Sub tests are added, one per line, here */

  tcase_add_test(tc, emptyIterator);
  tcase_add_test(tc, pagedListCheck);

  /* --------------------------------------------- */

  tcase_add_checked_fixture(tc, setup, teardown);

  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
