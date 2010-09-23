/*
 * handle.c
 *
 *  Created on: May 7, 2009
 *      Author: sears
 */
#include <config.h>

#include <stasis/common.h>
#include <stasis/constants.h>
#include <stasis/flags.h>
#include <stasis/io/handle.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <assert.h>
#include <stdio.h>

// @todo this factory stuff doesn't really belong here...
static stasis_handle_t * fast_factory(lsn_t off, lsn_t len, void * ignored) {
  stasis_handle_t * h = stasis_handle(open_memory)(off);
  //h = stasis_handle(open_debug)(h);
  stasis_write_buffer_t * w = h->append_buffer(h, len);
  w->h->release_write_buffer(w);
  return h;
}
typedef struct sf_args {
  const char * filename;
  int    openMode;
  int    filePerm;
} sf_args;
static stasis_handle_t * slow_file_factory(void * argsP) {
  sf_args * args = (sf_args*) argsP;
  stasis_handle_t * h =  stasis_handle(open_file)(0, args->filename, args->openMode, args->filePerm);
  //h = stasis_handle(open_debug)(h);
  return h;
}
static stasis_handle_t * slow_pfile_factory(void * argsP) {
  stasis_handle_t * h = argsP;
  return h;
}
static int nop_close(stasis_handle_t*h) { return 0; }

stasis_handle_t * stasis_handle(open)(const char * path) {
#ifndef HAVE_O_DIRECT
	if(bufferManagerO_DIRECT) {
	  printf("O_DIRECT not supported by this build; switching to conventional buffered I/O.\n");
	  bufferManagerO_DIRECT = 0;
	}
#endif
	int openMode;
	if(bufferManagerO_DIRECT) {
#ifdef HAVE_O_DIRECT
	  openMode = O_CREAT | O_RDWR | O_DIRECT;
#else
              printf("Can't happen\n");
              abort();
#endif
	} else {
	  openMode = O_CREAT | O_RDWR;
	}
	stasis_handle_t * ret;
	/// @todo remove hardcoding of buffer manager implementations in transactional2.c

	switch(bufferManagerFileHandleType) {
	  case BUFFER_MANAGER_FILE_HANDLE_NON_BLOCKING: {
		struct sf_args * slow_arg = malloc(sizeof(sf_args));
		slow_arg->filename = path;

	    slow_arg->openMode = openMode;

		slow_arg->filePerm = FILE_PERM;
		// Allow 4MB of outstanding writes.
		// @todo Where / how should we open storefile?
		int worker_thread_count = 1;
		if(bufferManagerNonBlockingSlowHandleType == IO_HANDLE_PFILE) {
		  //              printf("\nusing pread()/pwrite()\n");
		  stasis_handle_t * slow_pfile = stasis_handle_open_pfile(0, slow_arg->filename, slow_arg->openMode, slow_arg->filePerm);
		  int (*slow_close)(struct stasis_handle_t *) = slow_pfile->close;
		  slow_pfile->close = nop_close;
		  ret =
			  stasis_handle(open_non_blocking)(slow_pfile_factory, (int(*)(void*))slow_close, slow_pfile, 1, fast_factory,
						 NULL, worker_thread_count, PAGE_SIZE * 1024 , 1024);

		} else if(bufferManagerNonBlockingSlowHandleType == IO_HANDLE_FILE) {
		  ret =
			  stasis_handle(open_non_blocking)(slow_file_factory, 0, slow_arg, 0, fast_factory,
						 NULL, worker_thread_count, PAGE_SIZE * 1024, 1024);
        } else {
          printf("Unknown value for config option bufferManagerNonBlockingSlowHandleType\n");
          abort();
        }
      } break;
	  case BUFFER_MANAGER_FILE_HANDLE_FILE: {
	    ret = stasis_handle_open_file(0, path, openMode, FILE_PERM);
	  } break;
	  case BUFFER_MANAGER_FILE_HANDLE_PFILE: {
	    ret = stasis_handle_open_pfile(0, path, openMode, FILE_PERM);
	  } break;
	  case BUFFER_MANAGER_FILE_HANDLE_DEPRECATED: {
		assert(bufferManagerFileHandleType != BUFFER_MANAGER_FILE_HANDLE_DEPRECATED);
	  } break;
	  default: {
		printf("\nUnknown buffer manager filehandle type: %d\n",
			   bufferManagerFileHandleType);
		abort();
	  }
    }
	return ret;
}
