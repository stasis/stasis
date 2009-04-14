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
#include <pbl/pbl.h>

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/time.h>
#include <time.h>

#define LOG_NAME   "check_iterator.log"

/**
   @test

*/


/** Take two iterators, and make sure they represent the same set. */
static void iterator_test(int xid,
			 lladdIterator_t * reference_impl,
			 lladdIterator_t * tested_impl) {

  pblHashTable_t * hash = pblHtCreate();

  int numEntries = 0;

  while(Titerator_next(xid, reference_impl)) {
    int    keySize,  valSize;
    byte  *key,     *val,    *valScratch;

    keySize = Titerator_key(xid, reference_impl,   &key);
    valSize = Titerator_value(xid, reference_impl, &valScratch);
    val = malloc(valSize);
    memcpy(val, valScratch, valSize);  // pblHtInsert stores values a pointers to application managed memory.

    pblHtInsert(hash, key, keySize, val);
    numEntries ++;
  }

  while(Titerator_next(xid, tested_impl)) {
    numEntries --;

    int    keySize,  valSize;
    byte  *key,     *val,    *valScratch;

    keySize = Titerator_key(xid, tested_impl,   &key);
    valSize = Titerator_value(xid, tested_impl, &valScratch);

    val = pblHtLookup(hash, key, keySize);

    assert(val);

    assert(!memcmp(val, valScratch, valSize));

    free(val);

    pblHtRemove(hash, key, keySize);

  }

  assert(!numEntries);

}

#define NUM_ENTRIES 10000

START_TEST(iteratorTest)
{
  Tinit();
  int xid = Tbegin();
  unsigned int keyArray[NUM_ENTRIES];
  byte valueArray[NUM_ENTRIES];

  unsigned int i;

  recordid hash = ThashCreate(xid, sizeof(unsigned int), sizeof(byte));

  for(i = 0; i < NUM_ENTRIES; i++) {
    keyArray[i] = i;
    valueArray[i] = i % 256;
    byte j = i % 256;
    ThashInsert(xid, hash, (byte*)&i, sizeof(unsigned int), &j, sizeof(byte));
  }

  lladdIterator_t * arrayIt = arrayIterator((byte *)keyArray, sizeof(int), valueArray, sizeof(char), NUM_ENTRIES);
  i = 0;
  while(Titerator_next(-1, arrayIt)) {
    unsigned int * key;
    unsigned int ** bkey = &key;
    unsigned char * value;
    unsigned char ** bvalue = &value;
    int keySize   = Titerator_key(-1, arrayIt, (byte**)bkey);
    int valueSize = Titerator_value(-1, arrayIt, (byte**)bvalue);
    assert(keySize == sizeof(unsigned int));
    assert(valueSize == sizeof(unsigned char));
    assert(*key == i);
    assert(*value == (i % 256));
    i++;
  }

  Titerator_close(-1, arrayIt);

  arrayIt = arrayIterator((byte*)keyArray, sizeof(int), valueArray, sizeof(char), NUM_ENTRIES);

  lladdIterator_t * hashIt = ThashGenericIterator(xid, hash);

  iterator_test(xid, arrayIt, hashIt);

  Tcommit(xid);
  Tdeinit();
} END_TEST


Suite * check_suite(void) {
  Suite *s = suite_create("iterator");
  /* Begin a new test */
  TCase *tc = tcase_create("iterator");

  tcase_set_timeout(tc, 0); // disable timeouts
  /* Sub tests are added, one per line, here */

  tcase_add_test(tc, iteratorTest);

  /* --------------------------------------------- */

  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
