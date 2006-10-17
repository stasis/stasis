/*--- This software is copyrighted by the Regents of the University of
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
#include <lladd/io/handle.h>

#include <check.h>
#include <assert.h>

#include "../check_includes.h"

#define LOG_NAME   "check_io.log"

long myrandom(long x) {
  double xx = x;
  double r = random();
  double max = ((long)RAND_MAX)+1;
  max /= xx;
  return (long)((r/max));
}


void handle_smoketest(stasis_handle_t * h) { 

  const int one   = 0x11111111;
  const int two   = 0x22222222;
  const int three = 0x33333333;
  const int four  = 0x44444444;

  assert((!h->num_copies(h)) || (!h->num_copies_buffer(h)));

  assert(0 == h->start_position(h) ||
	 0 == h->end_position(h));

  assert(! h->write(h, 0, (byte*)&one, sizeof(int)));

  int one_read = 0;
  assert(! h->read(h, 0, (byte*)&one_read, sizeof(int)));
  assert(one_read == one);
  stasis_write_buffer_t * w = h->write_buffer(h, sizeof(int), sizeof(int));
  *((int*)(w->buf)) = two;
  w->h->release_write_buffer(w);
  one_read = 0;  
  stasis_read_buffer_t * r = h->read_buffer(h, 0, sizeof(int));
  one_read = *((int*)(r->buf));
  r->h->release_read_buffer(r);
  assert(one == one_read);

  int two_read = 0;
  assert(! h->read(h, sizeof(int), (byte*)&two_read, sizeof(int)));
  assert(two == two_read);
  
  lsn_t off;
  assert(! h->append(h, &off, (byte*)&three, sizeof(int)));
  
  w = h->append_buffer(h, sizeof(int));
  memcpy(w->buf, &four, sizeof(int));
  w->h->release_write_buffer(w);

  h->truncate_start(h, 2 * sizeof(int));
  
  int three_read = 0;
  int four_read = 0;
  
  assert(! h->read(h, 2*sizeof(int), (byte*)&three_read, sizeof(int)));

  r = h->read_buffer(h, 3*sizeof(int), sizeof(int));
  memcpy(&four_read, r->buf, sizeof(int));
  r->h->release_read_buffer(r);

  assert(three == three_read);
  assert(four == four_read);

}


typedef struct { 
  int * values;
  int count;
  stasis_handle_t * h;
} thread_arg;

#define VALUE_COUNT      100000
#define THREAD_COUNT   100
#define OPS_PER_THREAD   500000

lsn_t trunc_val;
pthread_mutex_t trunc_mut = PTHREAD_MUTEX_INITIALIZER;

void load_handle(thread_arg* t) {
  lsn_t * offsets = malloc(t->count * sizeof(lsn_t));
  
  stasis_handle_t * h = t->h;

  for(int i = 0; i < t->count; i++) { 
    offsets[i] = -1;
  }

  for(int i = 0; i < OPS_PER_THREAD; i++) { 
    int val = myrandom(t->count);
    
    if(offsets[val] == -1) { 
      // Need to write it somewhere.
      
      long choice = myrandom(4);
      
      switch(choice) { 
      case 0: {	  // overwrite old entry with write()
	long val2 = myrandom(t->count);
	offsets[val] = offsets[val2];
	offsets[val2] = -1;
	if(offsets[val] != -1) { 
	  int ret = h->write(h, offsets[val], (const byte*)&(t->values[val]), sizeof(int));
	  if(ret) {
	    assert(ret == EDOM);
	    offsets[val] = -1;
	    i--;
	  }
	} else { 
	  i--;
	}
      } break;
      case 1: {	  // overwrite old entry with write_buffer()
	long val2 = myrandom(t->count);
	offsets[val] = offsets[val2];
	offsets[val2] = -1;
	if(offsets[val] != -1) { 
	  stasis_write_buffer_t * w = h->write_buffer(h, offsets[val], sizeof(int));
	  if(!w->error) { 
	    *((int*)w->buf) = t->values[val];
	    assert(w->len == sizeof(int));
	    assert(w->off == offsets[val]);
	  } else { 
	    assert(w->error == EDOM);
	    offsets[val] = -1; 
	    i--;
	  }
	  w->h->release_write_buffer(w);
	} else {
	  i--; 
	}
      } break;
      case 2: {	  // append
	int ret = h->append(h, &(offsets[val]), (const byte*)&(t->values[val]), sizeof(int));
	assert(!ret);
      } break;
      case 3: {	  // append_buffer
	stasis_write_buffer_t * w = h->append_buffer(h, sizeof(int));
	if(!w->error) {
	  *((int*)w->buf) = t->values[val];
	  assert(w->len == sizeof(int));
	  offsets[val] = w->off;
	} else {
	  abort();
	}
	w->h->release_write_buffer(w);
      } break;
      default: {
	abort();
      }
      }
      
      int check;
      int ret = h->read(h, offsets[val], (byte*)&check, sizeof(int));
      if(!ret) { 
	assert(check == t->values[val]);
      }
      

    } else {
      // Read the value.
      
      long choice = myrandom(2);
      
      switch(choice) { 
      case 0: {   // read
	int j;
	int ret = h->read(h, offsets[val], (byte*)&j, sizeof(int));
	if(!ret) { 
	  assert(j == t->values[val]);
	} else {
	  assert(ret == EDOM);
	  assert(h->start_position(h) > offsets[val]);
	}
      } break;
      case 1: {   // read_buffer
	stasis_read_buffer_t * r = h->read_buffer(h, offsets[val], sizeof(int));
	if(!r->error) { 
	  assert(*(int*)(r->buf) == t->values[val]);
	  assert(r->len == sizeof(int));
	  r->h->release_read_buffer(r);
	} else {
	  assert(r->error == EDOM);
	  r->h->release_read_buffer(r);
	  assert(h->start_position(h) > offsets[val]);
	}
      } break;
      default:
	abort();
      };
      
    }

    // Truncate 1% of the time.
    if(!myrandom(100)) {     
      lsn_t pre_start = h->start_position(h);
      
      pthread_mutex_lock(&trunc_mut);
      lsn_t start = trunc_val;
      lsn_t stop   = start - 100 + myrandom(200);
      if(stop > trunc_val) { 
	trunc_val = stop;
      }
      pthread_mutex_unlock(&trunc_mut);

      assert(pre_start <= start);
      int ret = h->truncate_start(h, stop);
      if(!ret) { 
	lsn_t post_stop = h->start_position(h);
	assert(stop <= post_stop);
      }
    }
  }
  free(offsets);
}

void handle_sequentialtest(stasis_handle_t * h) { 
  time_t seed = time(0);
  printf("\nSeed = %ld\n", seed);
  srandom(seed);

  int * values = malloc(VALUE_COUNT * sizeof(int));

  for(int i = 0; i < VALUE_COUNT; i++) {
    values[i] = i;
  }
  trunc_val = 0;
  thread_arg arg = { values, VALUE_COUNT, h};
  load_handle(&arg);

  free(values);
}

void handle_concurrencytest(stasis_handle_t * h) { 
  int vc = myrandom(VALUE_COUNT) + 10;

  printf("Running concurrency test with %d values", vc); fflush(stdout);

  int * values = malloc(vc * sizeof(int));

  for(int i = 0; i < vc; i++) {
    values[i] = i;
  }
  
  thread_arg * args = malloc(THREAD_COUNT * sizeof(thread_arg));
  pthread_t * threads = malloc(THREAD_COUNT * sizeof(pthread_t));
  
  int val_per_thread = vc / THREAD_COUNT;
  trunc_val = 0;
  for(int i = 0; i < THREAD_COUNT; i++) {
    args[i].values = &(values[i * val_per_thread]);
    args[i].count  = val_per_thread;
    args[i].h = h;
    pthread_create(&threads[i], 0, (void*(*)(void*))load_handle, &args[i]);
  }

  for(int i = 0; i < THREAD_COUNT; i++) {
    pthread_join(threads[i], 0);
  }
  free(values);
  free(args);
  free(threads);
}
/**
   @test 
   Check the memory I/O handle.
*/
START_TEST(io_memoryTest) {
  stasis_handle_t * h;
  h = stasis_handle(open_memory)();
  //  h = stasis_handle(open_debug)(h);
  handle_smoketest(h);
  h->close(h);
  h = stasis_handle(open_memory)();
  //  h = stasis_handle(open_debug)(h);
  handle_sequentialtest(h);
  h->close(h);
  h = stasis_handle(open_memory)();
  //  h = stasis_handle(open_debug)(h);
  handle_concurrencytest(h);
  h->close(h);
} END_TEST


/** 
  Add suite declarations here
*/
Suite * check_suite(void) {
  Suite *s = suite_create("io");
  /* Begin a new test */
  TCase *tc = tcase_create("io_test");
  tcase_set_timeout(tc, 600); // ten minute timeout

  /* Sub tests are added, one per line, here */
  tcase_add_test(tc, io_memoryTest);
  /* --------------------------------------------- */
  tcase_add_checked_fixture(tc, setup, teardown);
  suite_add_tcase(s, tc);

  return s;
}

#include "../check_setup.h"
