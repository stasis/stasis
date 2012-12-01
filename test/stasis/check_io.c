/*--- This software is copyrighted by the Regents of the University
of
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

#include <stasis/io/handle.h>
#include <stasis/constants.h>
#include <stasis/flags.h>
#include <stasis/util/random.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define LOG_NAME   "check_io.log"

void handle_smoketest(stasis_handle_t * h) {

  const int one   = 0x11111111;
  const int two   = 0x22222222;

  assert((!h->num_copies(h)) || (!h->num_copies_buffer(h)));

  assert(! h->write(h, 0, (byte*)&one, sizeof(int)));

  int one_read = 0;
  int ret = h->read(h, 0, (byte*)&one_read, sizeof(int));
  assert(!ret);
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
  lsn_t * offsets = stasis_malloc(t->count, lsn_t);

  stasis_handle_t * h = t->h;

  for(int i = 0; i < t->count; i++) {
    offsets[i] = i * sizeof(int);
  }
  for(int i = 0; i < OPS_PER_THREAD; i++) {
    int val = stasis_util_random64(t->count);

    if(offsets[val] == -1) {
      // Need to write it somewhere.

      long choice = stasis_util_random64(2);

      switch(choice) {
      case 0: {	  // overwrite old entry with write()
        long val2 = stasis_util_random64(t->count);
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
        long val2 = stasis_util_random64(t->count);
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

      long choice = stasis_util_random64(2);

      switch(choice) {
      case 0: {   // read
	int j = -1;
	int ret = h->read(h, offsets[val], (byte*)&j, sizeof(int));
	if(!ret) {
	  assert(j == t->values[val]);
	} else {
	  assert(ret == EDOM);
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
	}
      } break;
      default:
	abort();
      };

    }
    // Force 1% of the time.
    if(!stasis_util_random64(100)) {
      h->force(h);
    }
  }
  free(offsets);
}

void handle_sequentialtest(stasis_handle_t * h) {
  time_t seed = time(0);
  printf("Seed = %ld\n", seed);
  srandom(seed);

  int * values = stasis_malloc(VALUE_COUNT, int);

  for(int i = 0; i < VALUE_COUNT; i++) {
    values[i] = i;
  }
  trunc_val = 0;
  thread_arg arg = { values, VALUE_COUNT, h};
  load_handle(&arg);

  free(values);
}

void handle_concurrencytest(stasis_handle_t * h) {
  int vc = stasis_util_random64(VALUE_COUNT) + 10;

  printf("Running concurrency test with %d values\n", vc); fflush(stdout);

  int * values = stasis_malloc(vc, int);

  for(int i = 0; i < vc; i++) {
    values[i] = i;
  }

  thread_arg * args = stasis_malloc(THREAD_COUNT, thread_arg);
  pthread_t * threads = stasis_malloc(THREAD_COUNT, pthread_t);
  stasis_handle_t ** handles = stasis_malloc(THREAD_COUNT / 2, stasis_handle_t*);

  int val_per_thread = vc / THREAD_COUNT;
  trunc_val = 0;
  for(int i = 0; i < THREAD_COUNT; i++) {
    args[i].values = &(values[i * val_per_thread]);
    args[i].count  = val_per_thread;
    if(!(i % 2)) {
      handles[i/2] = h->dup(h);
      h = handles[i/2];
    }
    args[i].h = h;
    pthread_create(&threads[i], 0, (void*(*)(void*))load_handle, &args[i]);
  }

  for(int i = 0; i < THREAD_COUNT; i++) {
    pthread_join(threads[i], 0);
  }
  for(int i = 0; i < THREAD_COUNT / 2; i++) {
    handles[i]->close(handles[i]);
  }
  free(values);
  free(args);
  free(threads);
  free(handles);
}
/**
   @test
   Check the memory I/O handle.
*/
START_TEST(io_memoryTest) {
  printf("io_memoryTest\n"); fflush(stdout);
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

START_TEST(io_fileTest) {
  printf("io_fileTest\n"); fflush(stdout);
  stasis_handle_t * h;
  h = stasis_handle(open_file)("logfile.txt", O_CREAT | O_RDWR, FILE_PERM);
  //  h = stasis_handle(open_debug)(h);
  handle_smoketest(h);
  h->close(h);

  remove("logfile.txt");

  h = stasis_handle(open_file)("logfile.txt", O_CREAT | O_RDWR, FILE_PERM);
  //h = stasis_handle(open_debug)(h);
  handle_sequentialtest(h);
  h->close(h);

  remove("logfile.txt");

  h = stasis_handle(open_file)("logfile.txt", O_CREAT | O_RDWR, FILE_PERM);
//  handle_concurrencytest(h);  // fails by design
  h->close(h);

  remove("logfile.txt");

} END_TEST

START_TEST(io_pfileTest) {
  printf("io_pfileTest\n"); fflush(stdout);

  stasis_handle_t * h;
  h = stasis_handle(open_pfile)("logfile.txt", O_CREAT | O_RDWR, FILE_PERM);
  //  h = stasis_handle(open_debug)(h);
  handle_smoketest(h);
  h->close(h);

  remove("logfile.txt");

  h = stasis_handle(open_pfile)("logfile.txt", O_CREAT | O_RDWR, FILE_PERM);
  //h = stasis_handle(open_debug)(h);
  handle_sequentialtest(h);
  h->close(h);

  remove("logfile.txt");

  h = stasis_handle(open_pfile)("logfile.txt", O_CREAT | O_RDWR, FILE_PERM);
  handle_concurrencytest(h);
  h->close(h);

  remove("logfile.txt");

} END_TEST

START_TEST(io_raid1pfileTest) {
  printf("io_raid1pfileTest\n"); fflush(stdout);

  const char * A = "vol1.txt";
  const char * B = "vol2.txt";

  remove(A);
  remove(B);

  stasis_handle_t *h, *a, *b;
  a = stasis_handle(open_pfile)(A, O_CREAT | O_RDWR, FILE_PERM);
  b = stasis_handle(open_pfile)(B, O_CREAT | O_RDWR, FILE_PERM);
  h = stasis_handle_open_raid1(a, b);

  //  h = stasis_handle(open_debug)(h);
  handle_smoketest(h);
  h->close(h);

  remove(A);
  remove(B);

  a = stasis_handle(open_pfile)(A, O_CREAT | O_RDWR, FILE_PERM);
  b = stasis_handle(open_pfile)(B, O_CREAT | O_RDWR, FILE_PERM);
  h = stasis_handle_open_raid1(a, b);

  //  h = stasis_handle(open_debug)(h);
  handle_sequentialtest(h);
  h->close(h);

  remove(A);
  remove(B);

  a = stasis_handle(open_pfile)(A, O_CREAT | O_RDWR, FILE_PERM);
  b = stasis_handle(open_pfile)(B, O_CREAT | O_RDWR, FILE_PERM);
  h = stasis_handle_open_raid1(a, b);

  //  h = stasis_handle(open_debug)(h);
  handle_concurrencytest(h);
  h->close(h);

  remove(A);
  remove(B);

} END_TEST
START_TEST(io_raid0pfileTest) {
  printf("io_raid0pfileTest\n"); fflush(stdout);
  uint32_t stripe_size = PAGE_SIZE;
  const char * A = "vol1.txt";
  const char * B = "vol2.txt";

  remove(A);
  remove(B);


  stasis_handle_t *h;
  stasis_handle_t *hp[2];

  hp[0] = stasis_handle(open_pfile)(A, O_CREAT | O_RDWR, FILE_PERM);
  hp[1] = stasis_handle(open_pfile)(B, O_CREAT | O_RDWR, FILE_PERM);
  h = stasis_handle_open_raid0(2, hp, stripe_size);

  //  h = stasis_handle(open_debug)(h);
  handle_smoketest(h);
  h->close(h);

  remove(A);
  remove(B);

  hp[0] = stasis_handle(open_pfile)(A, O_CREAT | O_RDWR, FILE_PERM);
  hp[1] = stasis_handle(open_pfile)(B, O_CREAT | O_RDWR, FILE_PERM);
  h = stasis_handle_open_raid0(2, hp, stripe_size);

  //  h = stasis_handle(open_debug)(h);
  handle_sequentialtest(h);
  h->close(h);

  remove(A);
  remove(B);

  hp[0] = stasis_handle(open_pfile)(A, O_CREAT | O_RDWR, FILE_PERM);
  hp[1] = stasis_handle(open_pfile)(B, O_CREAT | O_RDWR, FILE_PERM);
  h = stasis_handle_open_raid0(2, hp, stripe_size);

  //  h = stasis_handle(open_debug)(h);
  handle_concurrencytest(h);
  h->close(h);

  remove(A);
  remove(B);

} END_TEST
  /*
static stasis_handle_t * fast_factory(lsn_t off, lsn_t len, void * ignored) {
  stasis_handle_t * h = stasis_handle(open_memory)(off);
  //  h = stasis_handle(open_debug)(h);
  stasis_write_buffer_t * w = h->append_buffer(h, len);
  w->h->release_write_buffer(w);

  return h;

}
typedef struct sf_args {
  char * filename;
  int    openMode;
  int    filePerm;
} sf_args;

static stasis_handle_t * slow_factory_file(void * argsP) {
  sf_args * args = (sf_args*) argsP;
  return stasis_handle(open_file)(0, args->filename, args->openMode, args->filePerm);
}
static stasis_handle_t * slow_pfile_factory(void * argsP) {
  stasis_handle_t * h = argsP;
  return h;
}
static int slow_pfile_close(void * argsP) {
  stasis_handle_t * h = argsP;
  return h->close(h);
}

START_TEST(io_nonBlockingTest_file) {
  printf("io_nonBlockingTest\n"); fflush(stdout);
  stasis_handle_t * h;

  sf_args slow_args = {
    "logfile.txt",
    O_CREAT | O_RDWR,
    FILE_PERM
  };

  h = stasis_handle(open_non_blocking)(slow_factory_file, 0, &slow_args, 0,
				       fast_factory, 0,
				       5, 1024*1024, 100);
  //  h = stasis_handle(open_debug)(h);
  handle_smoketest(h);
  h->close(h);

  remove("logfile.txt");

  h = stasis_handle(open_non_blocking)(slow_factory_file, 0, &slow_args, 0,
				       fast_factory, 0,
				       5, 1024*1024, 100);
  //h = stasis_handle(open_debug)(h);
  handle_sequentialtest(h);
  h->close(h);

  remove("logfile.txt");

  h = stasis_handle(open_non_blocking)(slow_factory_file, 0, &slow_args, 0,
				       fast_factory, 0,
				       5, 1024 * 1024, 100);
  handle_concurrencytest(h);
  h->close(h);

  remove("logfile.txt");

} END_TEST


START_TEST(io_nonBlockingTest_pfile) {
  printf("io_nonBlockingTest\n"); fflush(stdout);
  stasis_handle_t * h;

  sf_args slow_args = {
    "logfile.txt",
    O_CREAT | O_RDWR,
    FILE_PERM
  };

  stasis_handle_t * pfile_singleton = slow_factory_file(&slow_args);

  h = stasis_handle(open_non_blocking)(slow_pfile_factory, slow_pfile_close, pfile_singleton, 0,
				       fast_factory, 0,
				       5, 1024*1024, 100);
  //  h = stasis_handle(open_debug)(h);
  handle_smoketest(h);
  h->close(h);

  remove("logfile.txt");

  pfile_singleton = slow_factory_file(&slow_args);
  h = stasis_handle(open_non_blocking)(slow_pfile_factory, slow_pfile_close, pfile_singleton, 0,
				       fast_factory, 0,
				       5, 1024*1024, 100);
  //h = stasis_handle(open_debug)(h);
  handle_sequentialtest(h);
  h->close(h);

  remove("logfile.txt");

  pfile_singleton = slow_factory_file(&slow_args);
  h = stasis_handle(open_non_blocking)(slow_pfile_factory, slow_pfile_close, pfile_singleton, 0,
				       fast_factory, 0,
				       5, 1024 * 1024, 100);
  handle_concurrencytest(h);
  h->close(h);

  remove("logfile.txt");

} END_TEST
*/

/**
  Add suite declarations here
*/
Suite * check_suite(void) {
  Suite *s = suite_create("io");
  /* Begin a new test */
  TCase *tc = tcase_create("io_test");
//  tcase_set_timeout(tc, 1800); // thirty minute timeout

  /* Sub tests are added, one per line, here */
  tcase_add_test(tc, io_memoryTest);
  tcase_add_test(tc, io_fileTest);
  tcase_add_test(tc, io_pfileTest);
  tcase_add_test(tc, io_raid1pfileTest);
  tcase_add_test(tc, io_raid0pfileTest);
  //tcase_add_test(tc, io_nonBlockingTest_file);
  //tcase_add_test(tc, io_nonBlockingTest_pfile);
  /* --------------------------------------------- */
  tcase_add_checked_fixture(tc, setup, teardown);
  suite_add_tcase(s, tc);

  return s;
}

#include "../check_setup.h"
