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

#include <stasis/util/ringbuffer.h>
#include <stasis/util/random.h>

#include <assert.h>
#include <sys/time.h>
#include <time.h>

#define LOG_NAME   "check_concurrentRingbuffer.log"

/**
   @test

*/

#define NUM_ENTRIES 10000

START_TEST(ringBufferSmokeTest) {
  stasis_ringbuffer_t * ring = stasis_ringbuffer_init(12, 0);
  assert((void*)RING_VOLATILE == stasis_ringbuffer_nb_get_rd_buf(ring, 0, 1));
  lsn_t off = stasis_ringbuffer_nb_reserve_space(ring, 4*900);
  assert(off != RING_FULL && off != RING_TORN && off != RING_VOLATILE);
  void * buf = stasis_ringbuffer_get_wr_buf(ring, off, 4*900);

  lsn_t off2 = stasis_ringbuffer_nb_reserve_space(ring, 4*400);
  assert(off2 == RING_FULL);

  stasis_ringbuffer_advance_write_tail(ring, 4*300);
  const void * buf3 = stasis_ringbuffer_nb_get_rd_buf(ring, 0,4*300);
  assert(buf3 == buf);

  stasis_ringbuffer_advance_read_tail(ring, 4*300);

  lsn_t off4 = stasis_ringbuffer_nb_reserve_space(ring, 4*400);
  assert(off4 != RING_FULL && off4 != RING_TORN && off4 != RING_VOLATILE);

  // XXX stasis_ringbuffer_deinit(ring);
} END_TEST

#define PROD_CONS_SIZE (100L * 1024L * 1024L)
static void * consumerWorker(void * arg) {
  stasis_ringbuffer_t * ring = arg;
  lsn_t cursor = 0;
  while(cursor < PROD_CONS_SIZE) {
    lsn_t rnd_size = stasis_util_random64(2048);
    if(rnd_size + cursor > PROD_CONS_SIZE) { rnd_size = PROD_CONS_SIZE - cursor; }
    byte const * rd_buf = stasis_ringbuffer_get_rd_buf(ring, RING_NEXT, rnd_size);
    for(lsn_t i = 0; i < rnd_size; i++) {
  //    printf("R[%lld] (addr=%lld) val = %d (%d)\n", cursor+i, (long long)(rd_buf)+i, rd_buf[i], (cursor+i)%250);
      assert(rd_buf[i] == ((cursor + i)%250));
    }
    cursor += rnd_size;
    stasis_ringbuffer_advance_read_tail(ring, cursor);
  }
  return 0;
}
static void * producerWorker(void * arg) {
  stasis_ringbuffer_t * ring = arg;
  lsn_t cursor = 0;
  while(cursor < PROD_CONS_SIZE) {
    int rnd_size = stasis_util_random64(2048);
    if(rnd_size + cursor > PROD_CONS_SIZE) { rnd_size = PROD_CONS_SIZE - cursor; }
    lsn_t wr_off = stasis_ringbuffer_reserve_space(ring, rnd_size, 0);
    assert(wr_off == cursor);
    byte * wr_buf = stasis_ringbuffer_get_wr_buf(ring, wr_off, rnd_size);
    for(lsn_t i = 0; i < rnd_size; i++) {
      wr_buf[i] = (cursor + i)%250;
//      printf("W[%d] (addr=%lld) val = %d\n", cursor+i, (long long)(wr_buf)+i, wr_buf[i]);
    }
    cursor += rnd_size;
    stasis_ringbuffer_advance_write_tail(ring, cursor);
  }
  return 0;
}

START_TEST(ringBufferProducerConsumerTest) {
  stasis_ringbuffer_t * ring = stasis_ringbuffer_init(12, 0);
  pthread_t reader, writer;
  pthread_create(&reader, 0, consumerWorker, ring);
  pthread_create(&writer, 0, producerWorker, ring);
  pthread_join(reader, 0);
  pthread_join(writer, 0);
  // XXX stasis_ringbuffer_deinit(ring);
} END_TEST

#define NUM_READERS 20
#define NUM_WRITERS NUM_READERS
#define BYTES_PER_THREAD (10L * 1000L * 1000L)

typedef struct {
  pthread_mutex_t wr_mut;
  stasis_ringbuffer_t * ring;
} arg;
static void * concurrentReader(void * argp) {
  arg * a = argp;
  stasis_ringbuffer_t * ring = a->ring;
  lsn_t cursor = 0;
  lsn_t rd_handle;
  while(cursor < BYTES_PER_THREAD) {
    lsn_t rnd_size = 1+stasis_util_random64(2047/NUM_READERS);
    if(rnd_size + cursor > BYTES_PER_THREAD) { rnd_size = BYTES_PER_THREAD - cursor; }
    stasis_ringbuffer_consume_bytes(ring, &rnd_size, &rd_handle);

    byte const * rd_buf = stasis_ringbuffer_get_rd_buf(ring, rd_handle, rnd_size);

    for(lsn_t i = 0; i < rnd_size; i++) {
  //    printf("R[%lld] (addr=%lld) val = %d (%d)\n", cursor+i, (long long)(rd_buf)+i, rd_buf[i], (cursor+i)%250);
      assert(rd_buf[i] == ((rd_handle + i)%250));
    }
    cursor += rnd_size;
    stasis_ringbuffer_read_done(ring, &rd_handle);
  }
  return 0;
}
static void * concurrentWriter(void * argp) {
  arg * a = argp;
  stasis_ringbuffer_t * ring = a->ring;
  lsn_t cursor = 0;
  lsn_t wr_handle;
  while(cursor < BYTES_PER_THREAD) {
    int rnd_size = 1+stasis_util_random64(2047/NUM_WRITERS);
    if(rnd_size + cursor > BYTES_PER_THREAD) { rnd_size = BYTES_PER_THREAD- cursor; }
    stasis_ringbuffer_reserve_space(ring, rnd_size, &wr_handle);
    byte * wr_buf = stasis_ringbuffer_get_wr_buf(ring, wr_handle, rnd_size);
    for(lsn_t i = 0; i < rnd_size; i++) {
      wr_buf[i] = (wr_handle + i)%250;
//      printf("W[%d] (addr=%lld) val = %d\n", cursor+i, (long long)(wr_buf)+i, wr_buf[i]);
    }
    cursor += rnd_size;
    stasis_ringbuffer_write_done(ring, &wr_handle);
    stasis_ringbuffer_reading_writer_done(ring, &wr_handle);
  }
  return 0;
}
START_TEST(ringBufferConcurrentProducerConsumerTest) {
  arg a = {
      PTHREAD_MUTEX_INITIALIZER,
      stasis_ringbuffer_init(12, 0),
  };
  pthread_t readers[NUM_READERS];
  pthread_t writers[NUM_WRITERS];
  for(int i = 0; i < NUM_READERS; i++) {
    pthread_create(&readers[i], 0, concurrentReader, &a);
  }
  for(int i = 0; i < NUM_WRITERS; i++) {
    pthread_create(&writers[i], 0, concurrentWriter, &a);
  }
  for(int i = 0; i < NUM_READERS; i++) {
    pthread_join(readers[i], 0);
  }
  for(int i = 0; i < NUM_WRITERS; i++) {
    pthread_join(writers[i], 0);
  }

} END_TEST

Suite * check_suite(void) {
  Suite *s = suite_create("ringBuffer");
  /* Begin a new test */
  TCase *tc = tcase_create("ringBuffer");

  /* Sub tests are added, one per line, here */

  tcase_add_test(tc, ringBufferSmokeTest);
  tcase_add_test(tc, ringBufferProducerConsumerTest);
  tcase_add_test(tc, ringBufferConcurrentProducerConsumerTest);

  /* --------------------------------------------- */

  tcase_add_checked_fixture(tc, setup, teardown);


  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
