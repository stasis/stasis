#include <config.h>
#include <check.h>
#include "../check_includes.h"

#include <stasis/transactional.h>

#include <assert.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>

#include <sys/time.h>
#include <time.h>

#define LOG_NAME   "check_lsmTree.log"
#define NUM_ENTRIES_A 100000
#define NUM_ENTRIES_B 10
#define NUM_ENTRIES_C 0

#define OFFSET      (NUM_ENTRIES * 10)

typedef int64_t lsmkey_t;

int cmp(const void *ap, const void *bp) {
  lsmkey_t a = *(lsmkey_t*)ap;
  lsmkey_t b = *(lsmkey_t*)bp;
  if(a < b) { return -1; }
  if(a == b) { return 0; }
  return 1;
}

void insertProbeIter(lsmkey_t NUM_ENTRIES) {
  int intcmp = 0;
  lsmTreeRegisterComparator(intcmp,cmp);

  Tinit();
  int xid = Tbegin();
  recordid tree = TlsmCreate(xid, intcmp, sizeof(lsmkey_t));
  for(lsmkey_t i = 0; i < NUM_ENTRIES; i++) {
    long pagenum = TlsmFindPage(xid, tree, (byte*)&i);
    assert(pagenum == -1);
    DEBUG("TlsmAppendPage %d\n",i);
    TlsmAppendPage(xid, tree, (const byte*)&i, i + OFFSET);
    pagenum = TlsmFindPage(xid, tree, (byte*)&i);
    assert(pagenum == i + OFFSET);
  }

  for(lsmkey_t i = 0; i < NUM_ENTRIES; i++) {
    long pagenum = TlsmFindPage(xid, tree, (byte*)&i);
    assert(pagenum == i + OFFSET);
  }

  int64_t count = 0;

  lladdIterator_t * it = lsmTreeIterator_open(xid, tree);

  while(lsmTreeIterator_next(xid, it)) {
    lsmkey_t * key;
    lsmkey_t **key_ptr = &key;
    int size = lsmTreeIterator_key(xid, it, (byte**)key_ptr);
    assert(size == sizeof(lsmkey_t));
    long *value;
    long **value_ptr = &value;
    size = lsmTreeIterator_value(xid, it, (byte**)value_ptr);
    assert(size == sizeof(pageid_t));
    assert(*key + OFFSET == *value);
    assert(*key == count);
    count++;
  }
  assert(count == NUM_ENTRIES);

  lsmTreeIterator_close(xid, it);

  Tcommit(xid);
  Tdeinit();
}
/** @test
*/
START_TEST(lsmTreeTest)
{
  insertProbeIter(NUM_ENTRIES_A);
  insertProbeIter(NUM_ENTRIES_B);
  insertProbeIter(NUM_ENTRIES_C);
} END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("lsmTree");
  /* Begin a new test */
  TCase *tc = tcase_create("simple");

  tcase_set_timeout(tc, 1200); // 20 minute timeout
  /* Sub tests are added, one per line, here */
  tcase_add_test(tc, lsmTreeTest);

  /* --------------------------------------------- */

  tcase_add_checked_fixture(tc, setup, teardown);

  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
