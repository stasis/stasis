#include <config.h>
#include <stasis/transactional.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


#include <stasis/bufferManager.h>
#include <stasis/bufferManager/legacy/legacyBufferManager.h>
#include <stasis/bufferManager/legacy/pageFile.h>
#include <stasis/bufferManager/bufferHash.h>
#include <stasis/bufferManager/concurrentBufferManager.h>
#include <stasis/truncation.h>
#include <stasis/logger/logger2.h>

#include <stasis/util/time.h>
#include <stasis/flags.h>

/*static stasis_handle_t * memory_factory(lsn_t off, lsn_t len, void * ignored) {
  stasis_handle_t * h = stasis_handle(open_memory)(off);
  //h = stasis_handle(open_debug)(h);
  stasis_write_buffer_t * w = h->append_buffer(h, len);
  w->h->release_write_buffer(w);
  return h;
}
typedef struct sf_args {
  char * filename;
  int    openMode;
  int    filePerm;
} sf_args;
static stasis_handle_t * traditional_file_factory(void * argsP) {
  sf_args * args = (sf_args*) argsP;
  stasis_handle_t * h =  stasis_handle(open_file)(0, args->filename, args->openMode, args->filePerm);
  //h = stasis_handle(open_debug)(h);
  return h;
}
*/

static inline long mb_to_page(long mb) {
  return (mb * 1024 * 1024) / PAGE_SIZE;
}
static inline long page_to_mb(long page) {
  return (page * PAGE_SIZE) / (1024 * 1024);
}
const char * usage = "./sequentialThroughput [--direct] [--mb mb] [--stake mb]\n  [--deprecatedBM|--deprecatedFH|--log_safe_writes|--log_memory|--log_file_pool|--nb|--file|--pfile|--nb_pfile|--nb_file] [--read]\n";

int main(int argc, char ** argv) {
  int direct = 0;
  int legacyBM = 0;
  int legacyFH = 0;
  int stake = 0;
  int log_mode = 0;
  int read_mode = 0;
  long page_count = mb_to_page(100);
  for(int i = 1; i < argc; i++) {
    if(!strcmp(argv[i], "--direct")) {
      direct = 1;
      stasis_buffer_manager_io_handle_flags |= O_DIRECT;
    } else if(!strcmp(argv[i], "--log_safe_writes")) {
      stasis_log_type = LOG_TO_FILE;
      log_mode = 1;
    } else if(!strcmp(argv[i], "--log_memory")) {
      stasis_log_type = LOG_TO_MEMORY;
      log_mode = 1;
    } else if(!strcmp(argv[i], "--log_file_pool")) {
      stasis_log_type = LOG_TO_DIR;
      log_mode = 1;
    } else if(!strcmp(argv[i], "--deprecatedBM")) {
      stasis_buffer_manager_factory = stasis_buffer_manager_deprecated_factory;
      legacyBM = 1;
    } else if(!strcmp(argv[i], "--deprecatedFH")) {
      stasis_page_handle_factory = stasis_page_handle_deprecated_factory;
      legacyFH = 1;
    } else if(!strcmp(argv[i], "--nb")) {
      printf("XXX unsupported at the moment!\n");
      legacyBM = 0;
      legacyFH = 0;
    } else if(!strcmp(argv[i], "--file")) {
      stasis_handle_file_factory = stasis_handle_open_file;
      legacyBM = 0;
      legacyFH = 0;
    } else if(!strcmp(argv[i], "--pfile")) {
      stasis_handle_file_factory = stasis_handle_open_pfile;
      legacyBM = 0;
      legacyFH = 0;
    } else if(!strcmp(argv[i], "--nb_pfile")) {
      stasis_non_blocking_handle_file_factory = stasis_handle_open_pfile;
    } else if(!strcmp(argv[i], "--nb_file")) {
      stasis_non_blocking_handle_file_factory = stasis_handle_open_file;
    } else if(!strcmp(argv[i], "--mb")) {
      i++;
      page_count = mb_to_page(atoll(argv[i]));
    } else if(!strcmp(argv[i], "--stake")) {
      i++;
      stake = mb_to_page(atoll(argv[i]));
    } else if(!strcmp(argv[i], "--read")) {
      read_mode = 1;
    } else if(!strcmp(argv[i], "--hint-sequential-writes")) {
      stasis_buffer_manager_hint_writes_are_sequential = 1;
      stasis_replacement_policy_concurrent_wrapper_exponential_backoff = 1;
    } else {
      printf("Unknown argument: %s\nUsage: %s\n", argv[i], usage);
      return 1;
    }
  }

  if(legacyFH && direct) {
    printf("--direct and --deprecatedFH are incompatible with each other\n");
    return 1;
  }

  struct timeval start;

  gettimeofday(&start,0);

  Tinit();

  if(read_mode) {
    if(log_mode) {
      printf("read mode for the log is unimplemented\n");
      abort();
    } else {
      for(long i = 0; i < page_count; i++) { 
	Page * p = loadPage(-1, i);
	releasePage(p);
      }
    }
  } else {
    if(log_mode) {
      lsn_t prevLSN = -1;
      byte * arg = calloc(PAGE_SIZE, 1);
      stasis_log_t * l = stasis_log();

      for(long i = 0; i < page_count; i++) {
	LogEntry * e = allocUpdateLogEntry(l, prevLSN, -1, OPERATION_NOOP,
					   0, PAGE_SIZE);
	l->write_entry(l, e);
	l->write_entry_done(l, e);
      }
      free(arg);
    } else {
      if(stake) {
	Page * p = loadPage(-1, stake);
	writelock(p->rwlatch,0);
	stasis_dirty_page_table_set_dirty(stasis_runtime_dirty_page_table(), p);
	unlock(p->rwlatch);
	releasePage(p);
      }

      for(long i =0; i < page_count; i++) {
	Page * p = loadUninitializedPage(-1, i);
	stasis_dirty_page_table_set_dirty(stasis_runtime_dirty_page_table(), p);
	releasePage(p);
      }
    }
  }
  Tdeinit();

  struct timeval stop;
  gettimeofday(&stop, 0);

  double elapsed = stasis_timeval_to_double(
                     stasis_subtract_timeval(stop,start));

  printf("Elasped = %f seconds, %s %ld mb, throughput %f MB/sec\n",
	 elapsed,
	 read_mode ? "read": "wrote",
	 page_to_mb(page_count),
	 ((double)page_to_mb(page_count)) / elapsed);
  return 0;
}
