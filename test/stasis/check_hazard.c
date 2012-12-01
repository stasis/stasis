/*
 * check_hazard.c
 *
 *  Created on: Feb 7, 2012
 *      Author: sears
 */
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

int hazard_finalize(void*p, void* ignored) {
  free(p);
  return 1;
}

#include <stasis/util/hazard.h>
#include <stasis/constants.h>
#include <stasis/util/random.h>

#include <sys/time.h>
#include <time.h>
#include <assert.h>

#define LOG_NAME   "check_lhtable.log"

/**
   @test
*/
START_TEST(hazard_smokeTest) {
  hazard_t * h = hazard_init(2, 2, 10, hazard_finalize, 0);
  char * a = stasis_malloc(1, char);
  char * b = stasis_malloc(1, char);
  *a = 0;
  *b = 1;
  char * ap = hazard_ref(h, 0, (hazard_ptr*)&a);
  char * bp = hazard_ref(h, 1, (hazard_ptr*)&b);
  hazard_free(h, ap);
  hazard_free(h, bp);
  hazard_scan(h,0);
  assert(*ap == 0);
  assert(*bp == 1);
  hazard_release(h, 0);
  hazard_release(h, 1);
  hazard_scan(h,0);
  hazard_deinit(h);
} END_TEST
/**
   @test
*/
#define NUM_OPS 1000
#define NUM_THREADS 1000
#define NUM_SLOTS 2000
hazard_ptr* slots;
pthread_mutex_t* muts;
void * hazard_worker(void * hp) {
  hazard_t * h = hp;
  for(int i = 0; i < NUM_OPS; i++) {
    int ptr_off = (int)stasis_util_random64(NUM_SLOTS);
    void * p = hazard_ref(h, 0, &slots[ptr_off]);
    if(p != NULL) {
      assert(*(int*)p == ptr_off);
    }
    hazard_release(h, 0);
    pthread_mutex_lock(&muts[ptr_off]);
    if(slots[ptr_off] != 0) {
      void* freeme = (void*)slots[ptr_off];
      slots[ptr_off] = 0;
      hazard_free(h, freeme);
    }
    pthread_mutex_unlock(&muts[ptr_off]);
  }
  return 0;
}
START_TEST(hazard_loadTest) {
  hazard_t * h = hazard_init(1, 1, 2, hazard_finalize, 0);
  slots = stasis_malloc(NUM_SLOTS, hazard_ptr);
  muts = stasis_malloc(NUM_SLOTS, pthread_mutex_t);
  for(int i = 0; i < NUM_SLOTS; i++) {
    slots[i] = (hazard_ptr) stasis_malloc(1, int);
    *(int*)slots[i] = i;
    pthread_mutex_init(&muts[i],0);
  }
  pthread_t * threads = stasis_malloc(NUM_THREADS, pthread_t);
  for(int i = 0; i < NUM_THREADS; i++) {
    pthread_create(&threads[i], 0, hazard_worker, h);
  }
  for(int i = 0; i < NUM_THREADS; i++) {
    pthread_join(threads[i], 0);
  }
  for(int i = 0; i < NUM_SLOTS; i++) {
    void * p = hazard_ref(h, 0, &slots[i]);
    if(p != NULL) {
      printf("found slot\n");
      slots[i] = 0;
      hazard_free(h, p);
    }
  }
  hazard_release(h, 0);
  free(threads);
  free(slots);
  free(muts);
  hazard_deinit(h);
} END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("hazard");
  /* Begin a new test */
  TCase *tc = tcase_create("hazard");
  tcase_set_timeout(tc, 0); // disable timeouts

  /* Sub tests are added, one per line, here */
  tcase_add_test(tc, hazard_smokeTest);
  tcase_add_test(tc, hazard_loadTest);

  /* --------------------------------------------- */

  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"


