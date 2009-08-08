#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <stasis/transactional.h>
#include <stasis/truncation.h>

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

int main(int argc, char ** argv) {
  int direct = 0;
  int legacyBM = 0;
  int legacyFH = 0;

  long page_count = mb_to_page(100);

  for(int i = 1; i < argc; i++) {
    if(!strcmp(argv[i], "--direct")) {
      direct = 1;
      bufferManagerO_DIRECT = 1;
    } else if(!strcmp(argv[i], "--deprecatedBM")) {
      bufferManagerType = BUFFER_MANAGER_DEPRECATED_HASH;
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
    } else {
      printf("Unknown argument: %s\n", argv[i]);
      return 1;
    }
  }

  if(legacyFH && direct) {
    printf("--direct and --deprecatedFH are incompatible with each other\n");
    return 1;
  }

  Tinit();

  for(long i =0; i < page_count; i++) {
    Page * p = loadPage(-1, i);
    writelock(p->rwlatch,0);
    stasis_dirty_page_table_set_dirty(stasis_runtime_dirty_page_table(), p);
    unlock(p->rwlatch);
    releasePage(p);
  }

  Tdeinit();
  return 0;
}
