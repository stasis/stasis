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

#include <lladd/transactional.h>
#include <pbl/pbl.h>
#include <stdlib.h>
#include <string.h>
#include <config.h>
#include <check.h>

#include <assert.h>

#include "../check_includes.h"

#include <lladd/multiplexer.h>

#include <sys/time.h>
#include <time.h>

#define LOG_NAME   "check_iterator.log"

/**
   @test 

*/




#define NUM_ENTRIES 10000
#define NUM_THREADS 100 
START_TEST(multiplexTest) {
  Tinit();
  int xid = Tbegin();
  recordid hash;
  lladdIterator_t * it = ThashGenericIterator(xid, hash);
  lladdFifoPool_t * fifoPool = fifoPool_ringBufferInit(NUM_THREADS, NUM_ENTRIES);
  lladdMultiplexer_t * mux = lladdMultiplexer_alloc(xid, it, &multiplexHashLogByKey, &fifoPool_getConsumerCRC32, fifoPool);
  
  // now, read from fifos, checking to see if everything is well.  (Need to spawn one thread per fifo.) 

  int i;
  
  /* threads have static thread sizes.  Ughh. */
  pthread_attr_t attr;
  pthread_attr_init(&attr);

  pthread_mutex_init(&mutex, NULL);
  pthread_cond_init(&never, NULL);

  pthread_attr_setstacksize (&attr, PTHREAD_STACK_MIN);
  pthread_attr_setschedpolicy(&attr, SCHED_FIFO);
  pthread_mutex_lock(&mutex);

  pthread_t * workers = malloc(sizeof(pthread_t) * fifoPool->fifoCount);

  for(i = 0 ; i < fifoPool->fifoCount; i++) {
    lladdConsumer_t * consumer = fifoPool->pool[i]->consumer;

    pthread_create(&workers[i], &attr, go, consumer);
    
  }
						    
  pthread_mutex_unlock(&mutex);
  
  for(i = 0; i < fifoPool->fifoCount; i++) {
    pthread_join(&workers[i], NULL);    
  }
} END_TEST


Suite * check_suite(void) {
  Suite *s = suite_create("multiplexer");
  /* Begin a new test */
  TCase *tc = tcase_create("multiplexer");

  tcase_set_timeout(tc, 0); // disable timeouts
  /* Sub tests are added, one per line, here */

  tcase_add_test(tc, multiplexTest);

  /* --------------------------------------------- */
  
  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
