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
#include <stasis/multiplexer.h>
#include <stasis/logger/logMemory.h>
#include <pbl/pbl.h>

#include <sys/time.h>
#include <time.h>

#include <assert.h>

#define LOG_NAME   "check_iterator.log"

#define NUM_BYTES_IN_FIFO 1000
#define NUM_INSERTS 100000
#define NUM_THREADS 500

lsn_t * array;

static pthread_mutex_t mutex;
static pthread_cond_t  never;


static void * go( void * arg) {
  lladdIterator_t * it = (lladdIterator_t *) arg;

  pthread_mutex_lock(&mutex);
  pthread_mutex_unlock(&mutex);

  int itRet = 0;
  while((itRet = Titerator_next(-1, it))) {
    byte * key, * value;
    int keySize, valueSize;

    keySize   = Titerator_key  (-1, it, &key);
    valueSize = Titerator_value(-1, it, &value);

    assert(keySize == sizeof(lsn_t));
    LogEntry * e = (LogEntry*)value;
    linearHash_remove_arg * arg = (linearHash_remove_arg*)stasis_log_entry_update_args_ptr(e);

    assert(arg->keySize == sizeof(lsn_t));
    assert(arg->valueSize == sizeof(char));

    lsn_t i = *(lsn_t*)(arg+1);
    array[i]++;
    assert(array[i] == 1);

    Titerator_tupleDone(-1, it);

  }
  Titerator_close(-1, it);
  return NULL;
}
static void * trygo( void * arg) {
  lladdIterator_t * it = (lladdIterator_t *) arg;

  pthread_mutex_lock(&mutex);
  pthread_mutex_unlock(&mutex);

  int itRet = 0;
  assert(it->type >= 0 && it->type < MAX_ITERATOR_TYPES);
  while((itRet = Titerator_tryNext(-1, it))) {
    assert(it->type >= 0 && it->type < MAX_ITERATOR_TYPES);

    byte * key, * value;
    int keySize, valueSize;

    keySize   = Titerator_key  (-1, it, &key);
    valueSize = Titerator_value(-1, it, &value);

    assert(keySize == sizeof(lsn_t));
    LogEntry * e = (LogEntry*)value;
    linearHash_remove_arg * arg = (linearHash_remove_arg*)stasis_log_entry_update_args_ptr(e);

    assert(arg->keySize == sizeof(lsn_t));
    assert(arg->valueSize == sizeof(char));

    lsn_t i = *(lsn_t*)(arg+1);
    array[i]++;
    assert(*(lsn_t*)key == i);
    assert(array[i] == 1);

    Titerator_tupleDone(-1, it);

  }
  return NULL;
}

static void * go2( void * arg) {
  lladdIterator_t * it = (lladdIterator_t *) arg;

  pthread_mutex_lock(&mutex);
  pthread_mutex_unlock(&mutex);

  int itRet = 0;
  while((itRet = Titerator_next(-1, it))) {

    lladdFifo_t ** dirtyFifo_ptr;
    lladdFifo_t *** bdirtyFifo_ptr = &dirtyFifo_ptr;
    lladdFifo_t *  dirtyFifo;
    int dirtyFifoSize = Titerator_value(-1, it, (byte**)bdirtyFifo_ptr);

    dirtyFifo = * dirtyFifo_ptr;

    assert(dirtyFifo->iterator->type >= 0 && dirtyFifo->iterator->type < MAX_ITERATOR_TYPES);

    Titerator_tupleDone(-1, it);

    assert(dirtyFifoSize == sizeof(lladdFifo_t *));
    assert(dirtyFifo->iterator->type >= 0 && dirtyFifo->iterator->type < MAX_ITERATOR_TYPES);

    trygo(dirtyFifo->iterator);
  }
  return NULL;
}

/**
   @test

*/


START_TEST(multiplexTest) {
  Tinit();
  int xid = Tbegin();

  recordid hash = ThashCreate(xid, sizeof(lsn_t), VARIABLE_LENGTH);
  linearHash_remove_arg * arg = malloc(sizeof(linearHash_remove_arg) + sizeof(lsn_t) + sizeof(char));
  arg->keySize = sizeof(lsn_t);
  arg->valueSize = sizeof(char);


  lsn_t i;

  array = (lsn_t*)calloc(NUM_INSERTS, sizeof(lsn_t));

  for(i = 0; i < NUM_INSERTS; i++) {

    (*(lsn_t*)(arg+1)) = i;
    LogEntry * e = allocUpdateLogEntry(-1, -1, OPERATION_LINEAR_HASH_INSERT, INVALID_PAGE,
                                       sizeof(linearHash_remove_arg) + sizeof(lsn_t) + sizeof(char));
    memcpy(stasis_log_entry_update_args_ptr(e), arg, sizeof(linearHash_remove_arg) + sizeof(lsn_t) + sizeof(char));

    ThashInsert(xid, hash, (byte*)&i, sizeof(lsn_t), (byte*)e, sizeofLogEntry(0, e));


    free(e);

  }

  free(arg);
  Tcommit(xid);

  lladdIterator_t * it = ThashGenericIterator(xid, hash);
  lladdFifo_t * dirtyFifos = logMemoryFifo((lsn_t)(((double)NUM_INSERTS) * 0.5), 0);   //  8 bytes of memory used per queued request.
  //lladdFifoPool_t * fifoPool = lladdFifoPool_ringBufferInit(NUM_THREADS, NUM_BYTES_IN_FIFO, NULL, dirtyFifos);
  lladdFifoPool_t * fifoPool = lladdFifoPool_pointerPoolInit(NUM_THREADS, NUM_BYTES_IN_FIFO/10, NULL, dirtyFifos);

  lladdMultiplexer_t * mux = lladdMultiplexer_alloc(xid, it,
						    &multiplexHashLogByKey,
						    fifoPool);


  // now, read from fifos, checking to see if everything is well.  (Need to spawn one thread per fifo.)


  /* threads have static stack sizes.  Ughh. */
  pthread_attr_t attr;
  pthread_attr_init(&attr);

  pthread_mutex_init(&mutex, NULL);
  pthread_cond_init(&never, NULL);

  pthread_attr_setstacksize (&attr, PTHREAD_STACK_MIN);
  // pthread_attr_setschedpolicy(&attr, SCHED_FIFO);
  pthread_mutex_lock(&mutex);

  lladdMultiplexer_start(mux, &attr);

  //  printf("->(%d)", fifoPool->fifoCount); fflush(stdout);

  pthread_t * workers = malloc(sizeof(pthread_t) * fifoPool->fifoCount);

  for(i = 0 ; i < fifoPool->fifoCount; i+=2) {
    //   lladdConsumer_t * consumer = fifoPool->pool[i]->consumer;
    lladdIterator_t * iterator = fifoPool->pool[i]->iterator;

    //    printf("%d ", i);

    pthread_create(&workers[i], &attr, go, iterator);
    pthread_create(&workers[i+1], &attr, go2, dirtyFifos->iterator);

  }
  //  printf("<-(%d)", fifoPool->fifoCount); fflush(stdout);

  // This thread runs down the dirty list, consuming idle fifos' contents in
  // case other threads are busy, or not interested.

  //  pthread_create(&cleaner, &attr, go2, dirtyFifos->iterator);

  pthread_mutex_unlock(&mutex);

  lladdMultiplexer_join(mux);

  for(i = 0; i < fifoPool->fifoCount; i+=2) {
    pthread_join(workers[i], NULL);
    pthread_join(workers[i+1], NULL);
  }

  for(i = 0; i < NUM_INSERTS; i++) {
    assert(array[i] == 1);
  }

  //  pthread_join(cleaner, NULL);

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
