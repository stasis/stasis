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
#include <limits.h>
#include <math.h>
#include <pthread.h>
#define LOG_NAME   "check_linearHashNTA.log"
#define NUM_ENTRIES 100000
/** @test
*/
START_TEST(linearHashNTAtest)
{
  Tinit();
  
  int xid = Tbegin();
  recordid val;
  recordid hashHeader = ThashCreate(xid, sizeof(int), sizeof(recordid));
  recordid * val2;
  int i;
  printf("\n"); fflush(stdout);
  for(i = 0; i < NUM_ENTRIES; i++) {
    if(!(i % (NUM_ENTRIES/10))) {
      printf("."); fflush(stdout);
    }
    val.page = i * NUM_ENTRIES;
    val.slot = val.page * NUM_ENTRIES;
    val.size = val.slot * NUM_ENTRIES;
    int found = ThashLookup(xid, hashHeader, (byte*)&i, sizeof(int), (byte**)&val2);
    assert(-1 == found);
    ThashInsert(xid, hashHeader, (byte*)&i, sizeof(int), (byte*)&val, sizeof(recordid));
    found = ThashLookup(xid, hashHeader, (byte*)&i, sizeof(int), (byte**)&val2);
    assert(sizeof(recordid) == found);
    assert(val2->page == i * NUM_ENTRIES);
    assert(val2->slot == val2->page * NUM_ENTRIES);
    assert(val2->size == val2->slot * NUM_ENTRIES);
    free(val2);
  }
  
  Tcommit(xid);
  printf("\n"); fflush(stdout);

  xid = Tbegin();
  for(i = 0; i < NUM_ENTRIES; i+=10){
    if(!(i % (NUM_ENTRIES/10))) {
      printf("-"); fflush(stdout);
    }
    int found = ThashLookup(xid, hashHeader, (byte*)&i, sizeof(int), (byte**)&val2);
    assert(sizeof(recordid) == found);
    free(val2);
    found = ThashRemove(xid, hashHeader, (byte*)&i, sizeof(int)); 
    assert(found);
    found = ThashLookup(xid, hashHeader, (byte*)&i, sizeof(int), (byte**)&val2);
    assert(-1==found);
    found = ThashRemove(xid, hashHeader, (byte*)&i, sizeof(int));
    assert(!found);
  }
  printf("\nabort()\n"); fflush(stdout);
  Tabort(xid);
  xid = Tbegin();
  for(i = 0; i < NUM_ENTRIES; i++) {
    if(!(i % (NUM_ENTRIES/10))) {
      printf("+"); fflush(stdout);
    }
    int found = ThashLookup(xid, hashHeader, (byte*)&i, sizeof(int), (byte**)&val2);
    assert(sizeof(recordid) == found);
    assert(val2->page == i * NUM_ENTRIES);
    assert(val2->slot == val2->page * NUM_ENTRIES);
    assert(val2->size == val2->slot * NUM_ENTRIES);
    free(val2);
  }
  Tcommit(xid);
  Tdeinit();
} END_TEST

/** @test
*/
START_TEST(linearHashNTAVariableSizetest)
{
  Tinit();
  
  int xid = Tbegin();
  recordid val;
  recordid hashHeader = ThashCreate(xid, VARIABLE_LENGTH, VARIABLE_LENGTH);
  recordid * val2;
  int i;
  printf("\n"); fflush(stdout);
  for(i = 0; i < NUM_ENTRIES; i++) {
    if(!(i % (NUM_ENTRIES/10))) {
      printf("."); fflush(stdout);
    }
    val.page = i * NUM_ENTRIES;
    val.slot = val.page * NUM_ENTRIES;
    val.size = val.slot * NUM_ENTRIES;
    val2 = 0;
    int found = ThashLookup(xid, hashHeader, (byte*)&i, sizeof(int), (byte**)&val2);
    assert(-1 == found);
    ThashInsert(xid, hashHeader, (byte*)&i, sizeof(int), (byte*)&val, sizeof(recordid));
    val2 =0;
    int ret = ThashLookup(xid, hashHeader, (byte*)&i, sizeof(int), (byte**)&val2);
    assert(sizeof(recordid) == ret);
    assert(val2->page == i * NUM_ENTRIES);
    assert(val2->slot == val2->page * NUM_ENTRIES);
    assert(val2->size == val2->slot * NUM_ENTRIES);
    free(val2);
  }
  
  Tcommit(xid);
  printf("\n"); fflush(stdout);

  xid = Tbegin();
  for(i = 0; i < NUM_ENTRIES; i+=10){
    if(!(i % (NUM_ENTRIES/10))) {
      printf("-"); fflush(stdout);
    }
    int found = ThashLookup(xid, hashHeader, (byte*)&i, sizeof(int), (byte**)&val2);
    assert(sizeof(recordid) == found);
    free(val2);
    found = ThashRemove(xid, hashHeader, (byte*)&i, sizeof(int)); 
    assert(found);
    found = ThashLookup(xid, hashHeader, (byte*)&i, sizeof(int), (byte**)&val2);
    assert(-1==found);
    found = ThashRemove(xid, hashHeader, (byte*)&i, sizeof(int));
    assert(!found);
  }
  printf("\nabort()\n"); fflush(stdout);
  Tabort(xid);
  xid = Tbegin();
  for(i = 0; i < NUM_ENTRIES; i++) {
    if(!(i % (NUM_ENTRIES/10))) {
      printf("+"); fflush(stdout);
    }
    int ret = ThashLookup(xid, hashHeader, (byte*)&i, sizeof(int), (byte**)&val2);
    assert(sizeof(recordid) == ret);
    assert(val2->page == i * NUM_ENTRIES);
    assert(val2->slot == val2->page * NUM_ENTRIES);
    assert(val2->size == val2->slot * NUM_ENTRIES);
    free(val2);
  }
  Tcommit(xid);
  Tdeinit();
} END_TEST



#define NUM_THREADS 100
#define NUM_T_ENTRIES 500
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
  
  for(i = 0; i < NUM_T_ENTRIES; i+=10) {
    int * value;
    recordid key = makekey(thread,i);
    int found = ThashRemove(xid, hash, (byte*)&key, sizeof(recordid)); 
    assert(found);
    found = ThashLookup(xid, hash, (byte*)&key, sizeof(recordid), (byte**)&value);
    assert(-1==found);
    found = ThashRemove(xid, hash, (byte*)&key, sizeof(recordid));
    assert(!found);
  }
  
  Tabort(xid);
  xid = Tbegin();
  
  for(i = 0; i < NUM_T_ENTRIES; i+=10) {
    recordid key = makekey(thread,i);
    int * value;
    int found = ThashLookup(xid, hash, (byte*)&key, sizeof(recordid), (byte**)&value);
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
    linear_hash_worker_args * args = malloc(sizeof(linear_hash_worker_args));
    args->thread = i;
    args->rid= rid;
    pthread_create(&threads[i], NULL, &worker, args);
  }
  for(i = 0; i < NUM_THREADS; i++) {
    void * ret;
    pthread_join(threads[i], ret);
  }
  Tdeinit();
} END_TEST

START_TEST(linearHashNTAIteratortest) {
  Tinit();
  int xid = Tbegin();
  
  recordid hash = ThashCreate(xid, sizeof(int), sizeof(recordid));
  
  int i = 0;
  
  for(i = 0; i < NUM_ENTRIES; i++) {
    recordid value = makekey(0, i);
    int found = ThashInsert(xid, hash, (byte*)&i, sizeof(int), (byte*)&value, sizeof(recordid));
    assert(!found);
  }
  
  int seen[NUM_ENTRIES];
  
  for(i = 0; i < NUM_ENTRIES; i++) {
    seen[i] = 0;
  }
  
  lladd_hash_iterator * it = ThashIterator(xid, hash, sizeof(int), sizeof(recordid));
  
  int * key;
  recordid * value;
  int keySize; 
  int valueSize;
  
  while(ThashNext(xid, it, (byte**)&key, &keySize, (byte**)&value, &valueSize)) {
    
    recordid check = makekey(0, *key);
    assert(!memcmp(value, &check, sizeof(recordid)));
    
    assert(!seen[*key]);
    seen[*key]++;
    
    free(key);
    free(value);
  }
  
  for(i = 0 ; i < NUM_ENTRIES; i++) { 
    assert(seen[i] == 1);
  }
  
  Tcommit(xid);
  Tdeinit();
} END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("linearHashNTA");
  /* Begin a new test */
  TCase *tc = tcase_create("simple");


  /* Sub tests are added, one per line, here */
  tcase_add_test(tc, linearHashNTAVariableSizetest);
  tcase_add_test(tc, linearHashNTAIteratortest);
  tcase_add_test(tc, linearHashNTAtest);
  tcase_add_test(tc, linearHashNTAThreadedTest);
  
  /* --------------------------------------------- */
  
  tcase_add_checked_fixture(tc, setup, teardown);

  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
