#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <stasis/transactional.h>
#include <stasis/bufferManager.h>
#include <stasis/bufferManager/legacy/legacyBufferManager.h>
#include <stasis/truncation.h>
#include <stasis/logger/logger2.h>
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

const char * usage = "./sequentialThroughput [--direct] [--page_count mb] [--stake mb]\n  [--deprecatedBM|--deprecatedFH|--log_safe_writes|--log_memory|--nb|--file|--pfile|--nb_pfile|--nb_file]\n";

int main(int argc, char ** argv) {
  int direct = 0;
  int legacyBM = 0;
  int legacyFH = 0;
  int stake = 0;
  int log_mode = 0;
  long page_count = mb_to_page(100);

  for(int i = 1; i < argc; i++) {
    if(!strcmp(argv[i], "--direct")) {
      direct = 1;
      bufferManagerO_DIRECT = 1;
    } else if(!strcmp(argv[i], "--log_safe_writes")) {
      stasis_log_type = LOG_TO_FILE;
      log_mode = 1;
    } else if(!strcmp(argv[i], "--log_memory")) {
      stasis_log_type = LOG_TO_MEMORY;
      log_mode = 1;
    } else if(!strcmp(argv[i], "--deprecatedBM")) {
      stasis_buffer_manager_factory = stasis_buffer_manager_deprecated_factory;
      legacyBM = 1;
    } else if(!strcmp(argv[i], "--deprecatedFH")) {
      bufferManagerFileHandleType = BUFFER_MANAGER_FILE_HANDLE_DEPRECATED;
      legacyFH = 1;
    } else if(!strcmp(argv[i], "--nb")) {
      bufferManagerFileHandleType = BUFFER_MANAGER_FILE_HANDLE_NON_BLOCKING;
      legacyBM = 0;
      legacyFH = 0;
    } else if(!strcmp(argv[i], "--file")) {
      bufferManagerFileHandleType = BUFFER_MANAGER_FILE_HANDLE_FILE;
      legacyBM = 0;
      legacyFH = 0;
    } else if(!strcmp(argv[i], "--pfile")) {
      bufferManagerFileHandleType = BUFFER_MANAGER_FILE_HANDLE_PFILE;
      legacyBM = 0;
      legacyFH = 0;
    } else if(!strcmp(argv[i], "--nb_pfile")) {
      bufferManagerNonBlockingSlowHandleType = IO_HANDLE_PFILE;
    } else if(!strcmp(argv[i], "--nb_file")) {
      bufferManagerNonBlockingSlowHandleType = IO_HANDLE_FILE;
    } else if(!strcmp(argv[i], "--mb")) {
      i++;
      page_count = mb_to_page(atoll(argv[i]));
    } else if(!strcmp(argv[i], "--stake")) {
      i++;
      stake = mb_to_page(atoll(argv[i]));
    } else {
      printf("Unknown argument: %s\nUsage: %s\n", argv[i], usage);
      return 1;
    }
  }

  if(legacyFH && direct) {
    printf("--direct and --deprecatedFH are incompatible with each other\n");
    return 1;
  }

  Tinit();

  if(log_mode) {
    lsn_t prevLSN = -1;
    byte * arg = calloc(PAGE_SIZE, 1);
    stasis_log_t * l = stasis_log();

    LogEntry * e = allocUpdateLogEntry(l, prevLSN, -1, OPERATION_NOOP,
                                       0, PAGE_SIZE);
    memcpy(stasis_log_entry_update_args_ptr(e), arg, PAGE_SIZE);
    for(long i = 0; i < page_count; i++) {
      void * h;
      LogEntry * e2 = l->reserve_entry(l, sizeofLogEntry(l, e), &h);
      e->prevLSN = e->LSN;
      e->LSN = -1;
      memcpy(e2, e, sizeofLogEntry(l, e));
      l->write_entry_done(l, e2, h);
    }
    freeLogEntry(l, e);
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
      Page * p = loadPage(-1, i);
      writelock(p->rwlatch,0);
      stasis_dirty_page_table_set_dirty(stasis_runtime_dirty_page_table(), p);
      unlock(p->rwlatch);
      releasePage(p);
    }
  }
  Tdeinit();
  return 0;
}
