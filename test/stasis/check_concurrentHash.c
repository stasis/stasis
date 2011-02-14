/*
 * check_concurrentHash.c
 *
 *  Created on: Oct 15, 2009
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

#define _GNU_SOURCE
#include "../check_includes.h"

#include <stasis/concurrentHash.h>

#include <stdio.h>
#include <time.h>
#include <limits.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>

#ifdef DBUG_TEST
extern int dbug_choice(int);
#endif

#define LOG_NAME   "check_lhtable.log"

#ifdef DBUG_TEST
#define NUM_OPS 4
#define NUM_ENTRIES 4
#define NUM_THREADS 2
#define THREAD_ENTRIES 2
#define HT_ENTRIES 16
#define myrandom(x) dbug_choice(x)
#else
#define NUM_OPS 10000000
#define NUM_ENTRIES 10000
#define NUM_THREADS 100
#define THREAD_ENTRIES ((NUM_ENTRIES/NUM_THREADS)-1)
#endif
hashtable_t * ht;

void * worker(void * arg) {
  int stride = *(int*) arg;

  pageid_t *data = malloc(sizeof(pageid_t) * THREAD_ENTRIES);

#ifdef DBUG_TEST
  for(int i = 1; i <= THREAD_ENTRIES; i++) {
    data[i-1] = -1 * (stride + (i * HT_ENTRIES));
  }
#else
  for(int i = 1; i <= THREAD_ENTRIES; i++) {
    data[i-1] = -1 * (stride + (i * NUM_THREADS));
  }
#endif
  for(int j = 0; j < NUM_OPS/*/ NUM_THREADS*/; j++) {

    int op = myrandom(2);

    int i = myrandom(THREAD_ENTRIES);

    pageid_t scratch = data[i];
    if(data[i] < 0) {
      scratch *= -1;
    }
    switch(op) {
    case 0: {
      void * ret;
      if(data[i] < 0) {
        ret = hashtable_insert(ht, scratch, &data[i]);
        assert(ret == NULL);
        data[i] *= -1;
      } else {
        ret = hashtable_remove(ht, scratch);
        assert(ret == &data[i]);
        data[i] *= -1;
      }
    } break;
    case 1: {
      void * ret = hashtable_lookup(ht, scratch);
      if(data[i] < 0) {
        assert(ret == NULL);
      } else {
        assert(ret == &data[i]);
      }
    } break;
    default:
      abort();
    }
  }
  free(data);
  return 0;
}

START_TEST(singleThreadHashTest) {
#ifdef DBUG_TEST
  ht = hashtable_init((pageid_t)HT_ENTRIES);
#else
  ht = hashtable_init((pageid_t)((double)THREAD_ENTRIES * 1.1));
#endif
  int i = 0;
  worker(&i);
  hashtable_deinit(ht);
} END_TEST

START_TEST(concurrentHashTest) {
#ifdef DBUG_TEST
  ht = hashtable_init((pageid_t)HT_ENTRIES);
#else
  ht = hashtable_init((pageid_t)((double)NUM_ENTRIES * 1.1));
#endif
  pthread_t workers[NUM_THREADS];
  for(int i = 0 ; i < NUM_THREADS; i++) {
    int * ip = malloc(sizeof(int));
    *ip = i;
    pthread_create(&workers[i], 0, worker, ip);
  }
  for(int i = 0 ; i < NUM_THREADS; i++) {
    pthread_join(workers[i],0);
  }
  hashtable_deinit(ht);
} END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("lhtable");
  /* Begin a new test */
  TCase *tc = tcase_create("lhtable");

  tcase_set_timeout(tc, 0); // disable timeouts

  srandom(43);

  /* Sub tests are added, one per line, here */
  tcase_add_test(tc, singleThreadHashTest);
#ifndef DBUG_TEST // TODO should run exactly one of these two tests under dbug.  Need good way to choose which one.
  tcase_add_test(tc, concurrentHashTest);
#endif
  /* --------------------------------------------- */

  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"

