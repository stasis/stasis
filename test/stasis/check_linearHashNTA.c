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
#include <limits.h>
#include <math.h>
#include <pthread.h>
#include <sys/time.h>
#include <time.h>

#define LOG_NAME   "check_linearHashNTA.log"
static const int NUM_ENTRIES = 100000;

#define ARRAY_SIZE (2 * 3 * (int)(PAGE_SIZE * 1.5))
static void arraySet(int * a, int mul) {
  int i;

  for ( i = 0 ; i < ARRAY_SIZE; i++) {
    a[i]= mul*i;
  }
}

static int arryCmp(int * a, int * b, int len) {
  return memcmp(a,b,len);
}

START_TEST(linearHashNTAabortTest) {
  Tinit();

  int xid = Tbegin();
  recordid rid = ThashCreate(xid, -1, -1);
  assert(0 == ThashInsert(xid, rid, (byte*)"foo", 4, (byte*)"bar", 4));
  char * ret;
  int len = ThashLookup(xid, rid, (byte*)"foo", 4, (byte**)&ret);
  assert(len == 4);
  assert(!strcmp("bar", ret));
  free(ret);
  Tcommit(xid);
	 
  xid = Tbegin();

  assert(1 == ThashInsert(xid, rid, (byte*)"foo", 4, (byte*)"baz", 4));
  assert(0 == ThashInsert(xid, rid, (byte*)"bar", 4, (byte*)"bat", 4));
  
  len = ThashLookup(xid, rid, (byte*)"foo", 4, (byte**)&ret);
  assert(len == 4);
  assert(!strcmp("baz", ret));
  free(ret);

  len = ThashLookup(xid, rid, (byte*)"bar", 4, (byte**)&ret);
  assert(len == 4);
  assert(!strcmp("bat", ret));
  free(ret);

  Tabort(xid);

  len = ThashLookup(xid, rid, (byte*)"foo", 4, (byte**)&ret);
  assert(len == 4);
  assert(!strcmp("bar", ret));
  free(ret);

  Tdeinit();
} END_TEST

/**
   @test
*/
START_TEST(linearHashNTAtest)
{
  Tinit();

  int xid = Tbegin();
  recordid val;
  recordid hashHeader = ThashCreate(xid, sizeof(int), sizeof(recordid));
  recordid * val2;
  recordid ** bval2 = &val2;
  int i;
  printf("\n"); fflush(stdout);
  for(i = 0; i < NUM_ENTRIES; i++) {
    if(!(i % (NUM_ENTRIES/10))) {
      printf("."); fflush(stdout);
    }
    val.page = i * NUM_ENTRIES;
    val.slot = val.page * NUM_ENTRIES;
    val.size = val.slot * NUM_ENTRIES;
    int found = ThashLookup(xid, hashHeader, (byte*)&i, sizeof(int), (byte**)bval2);
    assert(-1 == found);
    ThashInsert(xid, hashHeader, (byte*)&i, sizeof(int), (byte*)&val, sizeof(recordid));
    found = ThashLookup(xid, hashHeader, (byte*)&i, sizeof(int), (byte**)bval2);
    assert(sizeof(recordid) == found);
    assert(val2->page == val.page);
    assert(val2->slot == val.slot);
    assert(val2->size == val.size);
    free(val2);
  }
  Tcommit(xid);
  printf("\n"); fflush(stdout);

  xid = Tbegin();
  for(i = 0; i < NUM_ENTRIES; i+=10){
    if(!(i % (NUM_ENTRIES/10))) {
      printf("-"); fflush(stdout);
    }
    int found = ThashLookup(xid, hashHeader, (byte*)&i, sizeof(int), (byte**)bval2);
    assert(sizeof(recordid) == found);
    free(val2);
    found = ThashRemove(xid, hashHeader, (byte*)&i, sizeof(int));
    assert(found);
    found = ThashLookup(xid, hashHeader, (byte*)&i, sizeof(int), (byte**)bval2);
    assert(-1==found);
    found = ThashRemove(xid, hashHeader, (byte*)&i, sizeof(int));
    assert(!found);
  }
  printf("\n"); fflush(stdout);
  Tabort(xid);
  xid = Tbegin();
  for(i = 0; i < NUM_ENTRIES; i++) {
    if(!(i % (NUM_ENTRIES/10))) {
      printf("+"); fflush(stdout);
    }
    int found = ThashLookup(xid, hashHeader, (byte*)&i, sizeof(int), (byte**)bval2);
    assert(sizeof(recordid) == found);
    assert(val2->page == i * NUM_ENTRIES);
    assert(val2->slot == (slotid_t)val2->page * NUM_ENTRIES);
    assert(val2->size == val2->slot * NUM_ENTRIES);
    free(val2);
  }
  Tcommit(xid);
  Tdeinit();
  printf("\n"); fflush(stdout);
} END_TEST

/** @test
*/
START_TEST(linearHashNTAVariableSizetest)
{
  fflush(stdout); printf("\n");

  Tinit();

  int xid = Tbegin();
  recordid val;
  memset(&val,0,sizeof(val));
  recordid hashHeader = ThashCreate(xid, VARIABLE_LENGTH, VARIABLE_LENGTH);
  recordid * val2;
  recordid ** bval2 = &val2;
  int i;
  for(i = 0; i < NUM_ENTRIES; i++) {
    if(!(i % (NUM_ENTRIES/10))) {
      printf("."); fflush(stdout);
    }
    val.page = i * NUM_ENTRIES;
    val.slot = val.page * NUM_ENTRIES;
    val.size = val.slot * NUM_ENTRIES;
    val2 = 0;
    int found = ThashLookup(xid, hashHeader, (byte*)&i, sizeof(int), (byte**)bval2);
    assert(-1 == found);
    ThashInsert(xid, hashHeader, (byte*)&i, sizeof(int), (byte*)&val, sizeof(recordid));
    val2 =0;
    int ret = ThashLookup(xid, hashHeader, (byte*)&i, sizeof(int), (byte**)bval2);

    assert(sizeof(recordid) == ret);
    assert(val2->page == val.page);
    assert(val2->slot == val.slot);
    assert(val2->size == val.size);
    free(val2);
  }

  Tcommit(xid);

  printf("\n");

  xid = Tbegin();
  for(i = 0; i < NUM_ENTRIES; i+=10){
    if(!(i % (NUM_ENTRIES/10))) {
      printf("-"); fflush(stdout);
    }
    int found = ThashLookup(xid, hashHeader, (byte*)&i, sizeof(int), (byte**)bval2);
    assert(sizeof(recordid) == found);
    free(val2);
    found = ThashRemove(xid, hashHeader, (byte*)&i, sizeof(int));
    assert(found);
    found = ThashLookup(xid, hashHeader, (byte*)&i, sizeof(int), (byte**)bval2);
    assert(-1==found);
    found = ThashRemove(xid, hashHeader, (byte*)&i, sizeof(int));
    assert(!found);
  }
  printf("\n"); fflush(stdout);
  Tabort(xid);
  xid = Tbegin();
  for(i = 0; i < NUM_ENTRIES; i++) {
    if(!(i % (NUM_ENTRIES/10))) {
      printf("+"); fflush(stdout);
    }
    int ret = ThashLookup(xid, hashHeader, (byte*)&i, sizeof(int), (byte**)bval2);
    assert(sizeof(recordid) == ret);
    assert(val2->page == i * NUM_ENTRIES);
    assert(val2->slot == (slotid_t)val2->page * NUM_ENTRIES);
    assert(val2->size == val2->slot * NUM_ENTRIES);
    free(val2);
  }
  printf("\n");
  Tcommit(xid);
  Tdeinit();
} END_TEST


#define DEFAULT_NUM_THREADS 100

#define DEFAULT_NUM_T_ENTRIES 500

int NUM_THREADS = DEFAULT_NUM_THREADS;
int NUM_T_ENTRIES = DEFAULT_NUM_T_ENTRIES;

typedef struct {
  int thread;
  recordid rid;
} linear_hash_worker_args;
recordid makekey(int thread, int i) {
  recordid ret;
  ret.page = thread * NUM_T_ENTRIES + i;
  ret.slot = thread * NUM_T_ENTRIES + i * 2;
  ret.size= thread * NUM_T_ENTRIES + i * 3;
  return ret;
}
void * worker(void* arg) {
  linear_hash_worker_args * args = arg;
  int thread = args->thread;
  recordid hash = args->rid;

  int xid = Tbegin();

  int i;

  for(i = 0; i < NUM_T_ENTRIES; i++) {
    int value = i + thread * NUM_T_ENTRIES;
    recordid key = makekey(thread,i);
    ThashInsert(xid, hash, (byte*)&key, sizeof(recordid), (byte*)&value, sizeof(int));
  }

  Tcommit(xid);
  xid = Tbegin();

  int * value;
  int ** bvalue = &value;
  for(i = 0; i < NUM_T_ENTRIES; i+=10) {
    recordid key = makekey(thread,i);
    int found = ThashRemove(xid, hash, (byte*)&key, sizeof(recordid));
    assert(found);
    found = ThashLookup(xid, hash, (byte*)&key, sizeof(recordid), (byte**)bvalue);
    assert(-1==found);
    found = ThashRemove(xid, hash, (byte*)&key, sizeof(recordid));
    assert(!found);
  }

  Tabort(xid);
  xid = Tbegin();

  for(i = 0; i < NUM_T_ENTRIES; i+=10) {
    recordid key = makekey(thread,i);
    int found = ThashLookup(xid, hash, (byte*)&key, sizeof(recordid), (byte**)bvalue);
    assert(sizeof(int) == found);
    assert(*value == i + thread * NUM_T_ENTRIES);
    free (value);
  }
  Tcommit(xid);
  return NULL;
}
START_TEST(linearHashNTAThreadedTest) {
  Tinit();
  int xid = Tbegin();
  recordid rid = ThashCreate(xid, sizeof(recordid), sizeof(int));
  int i;
  Tcommit(xid);
  pthread_t threads[NUM_THREADS];
  for(i = 0; i < NUM_THREADS; i++) {
    linear_hash_worker_args * args = stasis_alloc(linear_hash_worker_args);
    args->thread = i;
    args->rid= rid;
    pthread_create(&threads[i], NULL, &worker, args);
  }
  for(i = 0; i < NUM_THREADS; i++) {
    void * ret;
    pthread_join(threads[i], &ret);
  }
  Tdeinit();
} END_TEST
#ifdef LONG_TEST
START_TEST(linearHashNTAThreadedTestRandomized) {
  Tinit();
  struct timeval tv;
  gettimeofday(&tv, 0);

  srandom(tv.tv_sec * 1000000 + tv.tv_usec);
  NUM_THREADS = (int)(((double)random()/(double)RAND_MAX)* ((double)DEFAULT_NUM_THREADS) * 2.0);
  NUM_T_ENTRIES = (int)(((double)random()/(double)RAND_MAX) * ((double)DEFAULT_NUM_T_ENTRIES) * 1.0);

  printf("\n%d threads, %d entries", NUM_THREADS, NUM_T_ENTRIES);

  int xid = Tbegin();
  recordid rid = ThashCreate(xid, sizeof(recordid), sizeof(int));
  int i;
  Tcommit(xid);
  pthread_t threads[NUM_THREADS];
  for(i = 0; i < NUM_THREADS; i++) {
    linear_hash_worker_args * args = stasis_alloc(linear_hash_worker_args);
    args->thread = i;
    args->rid= rid;
    pthread_create(&threads[i], NULL, &worker, args);
    if(!(i % 50)) {
      sleep(1);
    }
  }
  for(i = 0; i < NUM_THREADS; i++) {
    void * ret;
    pthread_join(threads[i], &ret);
  }
  Tdeinit();
} END_TEST
#endif // LONG_TEST
/**
   @test Test linear hash nta when the values it stores are larger
   than a single page.
 */
START_TEST(linearHashNTABlobTest) {
  Tinit();

  int arry1[ARRAY_SIZE];
  int arry2[ARRAY_SIZE];
  int arry3[ARRAY_SIZE];
  int arry4[ARRAY_SIZE];
  int *scratch;
  int alen=ARRAY_SIZE*sizeof(int);
  int one, two, three, four;
  int len1,len2,len3; // len4;

  arraySet(arry1,1); one = 1;
  arraySet(arry2,1); two = 2;
  arraySet(arry3,1); three = 3;
  arraySet(arry4,1); four = 4;

  int xid = Tbegin();
  recordid rid = ThashCreate(xid, VARIABLE_LENGTH, VARIABLE_LENGTH);
  ThashInsert(xid,rid,(byte*)&one,sizeof(one),(byte*)arry1,alen);
  len1 = ThashLookup(xid,rid,(byte*)&one,sizeof(one),(byte**)&scratch);
  assert(len1==alen);
  assert(!arryCmp(arry1,scratch,alen));
  free(scratch);
  Tcommit(xid);
  xid = Tbegin();
  ThashInsert(xid,rid,(byte*)&two,  sizeof(two),  (byte*)arry2,alen/2);
  ThashInsert(xid,rid,(byte*)&three,sizeof(three),(byte*)arry3,alen/3);

  len2 = ThashLookup(xid,rid,(byte*)&two, sizeof(two),  (byte**)&scratch);
  assert(len2 == alen/2);
  assert(!arryCmp(scratch,arry2,alen/2));
  free(scratch);

  len3 = ThashLookup(xid,rid,(byte*)&three, sizeof(three),  (byte**)&scratch);
  assert(len3 == alen/3);
  assert(!arryCmp(scratch,arry3,alen/3));
  free(scratch);

  Tabort(xid);

  Tdeinit();
  Tinit();

  xid = Tbegin();
  len1 = ThashLookup(xid,rid,(byte*)&one, sizeof(one),  (byte**)&scratch);
  assert(len1 == alen);
  assert(!arryCmp(scratch,arry1,alen));
  free(scratch);

  len3 = ThashLookup(xid,rid,(byte*)&two, sizeof(two),  (byte**)&scratch);
  assert(len3 == -1);
  Tcommit(xid);

  Tdeinit();

  Tinit();

  Tdeinit();
} END_TEST

void iteratorTest(int variableLength) {
  int seen[NUM_ENTRIES];
  recordid hash;
  {
    Tinit();
    int xid = Tbegin();

    if(variableLength) {
      hash = ThashCreate(xid, VARIABLE_LENGTH, VARIABLE_LENGTH);
    } else {
      hash = ThashCreate(xid, sizeof(int), sizeof(recordid));
    }

    int i = 0;

    for(i = 0; i < NUM_ENTRIES; i++) {
      recordid value = makekey(0, i);
      int found = ThashInsert(xid, hash, (byte*)&i, sizeof(int), (byte*)&value, sizeof(recordid));
      assert(!found);
    }

    for(i = 0; i < NUM_ENTRIES; i++) {
      seen[i] = 0;
    }

    lladd_hash_iterator * it = ThashIterator(xid, hash, sizeof(int), sizeof(recordid));

    int * key;
    int ** bkey = &key;
    recordid * value;
    recordid ** bvalue = &value;
    int keySize;
    int valueSize;

    while(ThashNext(xid, it, (byte**)bkey, &keySize, (byte**)bvalue, &valueSize)) {

      recordid check = makekey(0, *key);
      assert(!memcmp(value, &check, sizeof(recordid)));

      assert(!seen[*key]);
      seen[*key]++;

      free(key);
      free(value);
    }

    for(i = 0 ; i < NUM_ENTRIES; i++) {
      assert(seen[i] == 1);
      seen[i] = 0;
    }

    Tcommit(xid);
    Tdeinit();
  }
  {
    Tinit();
    int xid = Tbegin();

    for(int i = 0; i < NUM_ENTRIES; i++) {
      recordid value = makekey(0, i);
      int found = ThashInsert(xid, hash, (byte*)&i, sizeof(int), (byte*)&value, sizeof(recordid));
      assert(found);
    }

    lladd_hash_iterator * it = ThashIterator(xid, hash, sizeof(int), sizeof(recordid));

    int * key;
    int ** bkey = &key;
    recordid * value;
    recordid ** bvalue = &value;
    int keySize;
    int valueSize;

    while(ThashNext(xid, it, (byte**)bkey, &keySize, (byte**)bvalue, &valueSize)) {

      recordid check = makekey(0, *key);
      assert(!memcmp(value, &check, sizeof(recordid)));

      assert(!seen[*key]);
      seen[*key]++;

      free(key);
      free(value);
    }

    for(int i = 0 ; i < NUM_ENTRIES; i++) {
      assert(seen[i] == 1);
      seen[i] = 0;
    }
    Tabort(xid);
    Tdeinit();
  }
}

START_TEST(linearHashNTAFixedLengthIteratortest) {
  iteratorTest(0);
} END_TEST

START_TEST(linearHashNTAVariableLengthIteratortest) {
  iteratorTest(1);
} END_TEST

START_TEST(emptyHashIterator) {
  Tinit();
  int xid = Tbegin();

  recordid hash = ThashCreate(xid, sizeof(int), sizeof(recordid));

  lladd_hash_iterator * it = ThashIterator(xid, hash, sizeof(int), sizeof(recordid));

  byte * key;
  byte * value;
  int keySize;
  int valueSize;

  while(ThashNext(xid, it, &key, &keySize, &value, &valueSize)) {
    abort();
  }

  Tabort(xid);

  Tdeinit();


} END_TEST
START_TEST(emptyHashIterator2) {
  Tinit();
  int xid = Tbegin();

  recordid hash = ThashCreate(xid, sizeof(int), VARIABLE_LENGTH);

  lladd_hash_iterator * it = ThashIterator(xid, hash, sizeof(int), VARIABLE_LENGTH);

  byte * key;
  byte * value;
  int keySize;
  int valueSize;

  while(ThashNext(xid, it, &key, &keySize, &value, &valueSize)) {
    abort();
  }

  Tabort(xid);

  Tdeinit();


} END_TEST

START_TEST(lookupPrefix) {
  int xid;
  recordid directoryTable, rid, *rid1;
  int found;

  const char *path1 = "/home/ashok/datastore/ashok_vm/.lck-0300000000000000";
  const char *path2 = "/home/ashok/datastore/ashok_vm/.lck-0400000000000000";
  const char *path3 = "/home/ashok/datastore/ashok_vm/ashok_vm.vmx";
  const char *path4 = "/home/ashok/datastore/";

  Tinit();

  xid = Tbegin();
  directoryTable = ThashCreate(xid, VARIABLE_LENGTH, sizeof(recordid));
  assert(directoryTable.page == ROOT_RECORD.page &&
     directoryTable.slot == ROOT_RECORD.slot);

  Tcommit(xid);

  /* INSERT: /home/ashok/datastore/ashok_vm/.lck-0300000000000000 */
  xid = Tbegin();
  rid = Talloc(xid, sizeof(int));
  ThashInsert(xid, directoryTable,
          (byte*) path1, strlen(path1),
          (byte*) &rid, sizeof(recordid));
  Tcommit(xid);

  /* INSERT: /home/ashok/datastore/ashok_vm/.lck-0400000000000000 */
  xid = Tbegin();
  rid = Talloc(xid, sizeof(int));
  ThashInsert(xid, directoryTable,
          (byte*) path2, strlen(path2),
          (byte*) &rid, sizeof(recordid));
  Tcommit(xid);

  /* INSERT: /home/ashok/datastore/ashok_vm/ashok_vm.vmx */
  xid = Tbegin();
  rid = Talloc(xid, sizeof(int));
  ThashInsert(xid, directoryTable,
          (byte*) path3, strlen(path3),
          (byte*) &rid, sizeof(recordid));
  Tcommit(xid);

  /* RETRIEVE: /home/ashok/datastore/ashok_vm/.lck-0300000000000000 */
  xid = Tbegin();
  found = ThashLookup(xid, directoryTable, (byte*) path1,
              strlen(path1), (byte**) &rid1);
  Tcommit(xid);
  assert(found == sizeof(recordid));

  /* RETRIEVE: /home/ashok/datastore/ashok_vm/.lck-0400000000000000 */
  xid = Tbegin();
  found = ThashLookup(xid, directoryTable, (byte*) path2,
              strlen(path2), (byte**) &rid1);
  Tcommit(xid);
  assert(found == sizeof(recordid));

  /* RETRIEVE: /home/ashok/datastore/ashok_vm/ashok_vm.vmx */
  xid = Tbegin();
  found = ThashLookup(xid, directoryTable, (byte*) path3,
              strlen(path3), (byte**) &rid1);
  Tcommit(xid);
  assert(found == sizeof(recordid));

  /* RETRIEVE: /home/ashok/datastore/ */
  /* EXPECT FAILURE */
  xid = Tbegin();
  found = ThashLookup(xid, directoryTable, (byte*) path4,
              strlen(path4), (byte**) &rid1);
  Tcommit(xid);
  DEBUG("found = %d\n", found);
  assert(found == -1);

  Tdeinit();

} END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("linearHashNTA");
  /* Begin a new test */
  TCase *tc = tcase_create("simple");

  tcase_set_timeout(tc, 1200); // 20 minute timeout
  /* Sub tests are added, one per line, here */
  tcase_add_test(tc, linearHashNTAabortTest);
  tcase_add_test(tc, lookupPrefix);
  tcase_add_test(tc, emptyHashIterator);
  tcase_add_test(tc, emptyHashIterator2);
  tcase_add_test(tc, linearHashNTAVariableSizetest);
  tcase_add_test(tc, linearHashNTAFixedLengthIteratortest);
  tcase_add_test(tc, linearHashNTAVariableLengthIteratortest);
  tcase_add_test(tc, linearHashNTAtest);
  tcase_add_test(tc, linearHashNTAThreadedTest);
  tcase_add_test(tc, linearHashNTABlobTest);
#ifdef LONG_TEST
  tcase_add_test(tc, linearHashNTAThreadedTestRandomized);
  tcase_add_test(tc, linearHashNTAThreadedTestRandomized);
  tcase_add_test(tc, linearHashNTAThreadedTestRandomized);
  tcase_add_test(tc, linearHashNTAThreadedTestRandomized);
  tcase_add_test(tc, linearHashNTAThreadedTestRandomized);
  tcase_add_test(tc, linearHashNTAThreadedTestRandomized);
  #endif

  /* --------------------------------------------- */

  tcase_add_checked_fixture(tc, setup, teardown);

  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
