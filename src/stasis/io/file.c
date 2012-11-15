#include <config.h>
#include <stasis/common.h>
#include <fcntl.h>
#include <stasis/io/handle.h>

#include <stdio.h>
#include <assert.h>

/** @file */

typedef struct file_impl {
  pthread_mutex_t mut;
  lsn_t end_pos;
  int fd;
  int file_flags;
  int file_mode;
  char * filename;
} file_impl;

static int updateEOF(stasis_handle_t * h) {
  file_impl * impl = h->impl;
  off_t pos = lseek(impl->fd, 0, SEEK_END);
  if(pos == (off_t)-1) {
    return errno;
  } else {
    impl->end_pos = pos;
    return 0;
  }
}

static int file_num_copies(stasis_handle_t * h) { return 0; }
static int file_num_copies_buffer(stasis_handle_t * h) { return 0; }
static int file_force(stasis_handle_t *h);
static int file_force_range(stasis_handle_t *h, lsn_t start, lsn_t stop);

static int file_close(stasis_handle_t * h) {
  file_force(h);
  file_impl * impl = (file_impl*)h->impl;
  int fd = impl->fd;
  free(impl->filename);
  free(impl);
  free(h);
  int ret = close(fd);

  if(!ret) return 0;
  else     return errno;
}
static stasis_handle_t* file_dup(stasis_handle_t * h) {
  file_impl * impl = h->impl;
  return stasis_handle_open_file(impl->filename, impl->file_flags, impl->file_mode);
}
static void file_enable_sequential_optimizations(stasis_handle_t * h) {
  // TODO enable_sequential_optimizations is a no-op in file.c
}
static lsn_t file_end_position(stasis_handle_t *h) {
  file_impl * impl = (file_impl*)h->impl;
  pthread_mutex_lock(&(impl->mut));
  int error = updateEOF(h);
  int ret;
  if(error) {
    h->error = error;
    ret = -1;
  } else {
    ret = impl->end_pos;
  }
  pthread_mutex_unlock(&(impl->mut));
  return ret;
}

inline static int file_write_unlocked(stasis_handle_t * h, lsn_t off,
		     const byte * dat, lsn_t len) {
  file_impl * impl = (file_impl*)h->impl;
  int error = 0;

  // These should have been checked by the caller.
  assert(0 <= off);
  assert(impl->end_pos >= off+len);

  // @todo need a test harness that gets read(), write() and lseek() to misbehave.

  off_t lseek_offset = lseek(impl->fd, off, SEEK_SET);

  if(lseek_offset == (off_t)-1) {
    error = errno;
    if(error == EBADF || error == ESPIPE) {
      h->error = error;
      error = EBADF;
    }
  } else {
    ssize_t bytes_written = 0;

    // seek succeeded, so attempt to write.
    while(bytes_written < len) {
      ssize_t ret = write(impl->fd,
			  dat+bytes_written,
			  len-bytes_written);
      if(ret == -1) {
	if(errno == EAGAIN || errno == EINTR) {
	  // EAGAIN could be returned if the file handle was opened in non-blocking mode.
	  // we should probably warn instead of spinning.

	  // EINTR is returned if the write is interrupted by a signal.
	  // On Linux, it must have been interrupted before any bytes were written.
	  // On SVr4 it may have been interrupted at any point.

	  // Try again.

	  lseek_offset = lseek(impl->fd, off + bytes_written, SEEK_SET);
	  if(lseek_offset == (off_t)-1) {
	    error = errno;
	    if(error == EBADF || error == ESPIPE) {
	      h->error = error;
	      error = EBADF;
	    }
	    break;
	  }

	  ret = 0;

	} else {
	  // Need to set h->error if an unrecoverable error occured.
	  // The only unrecoverable error is EBADF.  (EINVAL could be
	  // the caller's fault if O_DIRECT is being used.  Otherwise,
	  // it is unrecoverable.
	  if(errno == EBADF) {
	    h->error = EBADF;
	  }
	  error = errno;
	  break;
	}
      }
      bytes_written += ret;
    }
  }

  return error;
}

inline static void print_eof_error(char * file, int line) {
  fprintf(stderr, "%s:%d Internal error: attempt to access negative offset, or beyond EOF.\n", file, line);
  fflush(stderr);
}

static int file_read(stasis_handle_t * h,
		    lsn_t off, byte * buf, lsn_t len) {
  file_impl * impl = (file_impl*)(h->impl);
  pthread_mutex_lock(&(impl->mut));
  int error = 0;
  if(off < 0) {
    error = EDOM;
  } else if(off + len > impl->end_pos) {
    error = updateEOF(h);
    if(!error && off + len > impl->end_pos) {
      error = EDOM;
    }
  }

  if(!error) {
    off_t lseek_offset = lseek(impl->fd, off, SEEK_SET);

    if(lseek_offset == (off_t)-1) {
      error = errno;
      if(error == EBADF || error == ESPIPE) {
	h->error = error;
	error = EBADF;
      } else if(error == EINVAL) {
	print_eof_error(__FILE__, __LINE__);
      }
    } else {
      ssize_t bytes_written = 0;
      // seek succeeded, so attempt to read.
      while(bytes_written < len) {
	ssize_t ret = read(impl->fd,
			   buf+bytes_written,
			   len-bytes_written);
	if(ret == -1) {
	  if(errno == EAGAIN || errno == EINTR) {
	    // EAGAIN could be returned if the file handle was opened in non-blocking mode.
	    // we should probably warn instead of spinning.

	    // EINTR is returned if the write is interrupted by a signal.
	    // On Linux, it must have been interrupted before any bytes were written.
	    // On SVr4 it may have been interrupted at any point.

	    // Try again.

	    lseek_offset = lseek(impl->fd, off + bytes_written, SEEK_SET);
	    if(lseek_offset == (off_t)-1) {
	      error = errno;
	      if(error == EBADF || error == ESPIPE) {
		h->error = error;
		error = EBADF;
	      } else if(error == EINVAL) {
		print_eof_error(__FILE__, __LINE__);
	      }
	      break;
	    }

	    ret = 0;

	  } else {
	    // Need to set h->error if an unrecoverable error occured.
	    // The only unrecoverable error is EBADF.  (EINVAL could be
	    // the caller's fault if O_DIRECT is being used.  Otherwise,
	    // it is unrecoverable.
	    if(errno == EBADF) {
	      h->error = EBADF;
	    }
	    error = errno;
	    break;
	  }
	} else if (ret == 0) {
	  // EOF (!)
	  print_eof_error(__FILE__, __LINE__);
	  error = EINVAL;
	  break;
	}
	bytes_written += ret;
      }
    }
  }
  pthread_mutex_unlock(&(impl->mut));
  return error;
}


static int file_write(stasis_handle_t *h, lsn_t off, const byte * dat, lsn_t len) {
  file_impl * impl = (file_impl*)(h->impl);
  pthread_mutex_lock(&(impl->mut));
  int error = 0;
  if(off < 0) {
    error = EDOM;
  }

  if(!error) {
    if(impl->end_pos < off+len){
      impl->end_pos = off+len;
    }
    error = file_write_unlocked(h, off, dat, len);
  }
  pthread_mutex_unlock(&(impl->mut));
  return error;
}

static stasis_write_buffer_t * file_write_buffer(stasis_handle_t * h,
						lsn_t off, lsn_t len) {
  // Allocate the handle
  stasis_write_buffer_t * ret = stasis_malloc(1, stasis_write_buffer_t);
  if(!ret) { return NULL; }

  file_impl * impl = (file_impl*)h->impl;
  int error = 0;

  pthread_mutex_lock(&(impl->mut));

  if(off < 0) {
    error = EDOM;
  }
  if(off + len > impl->end_pos) {
    impl->end_pos = off+len;
  }
  pthread_mutex_unlock(&(impl->mut));

  byte * buf;
  if(!error) {
    // Allocate the buffer
    buf = stasis_malloc(len, byte);
    if(!buf) {
      error = ENOMEM;
    }
  }

  if(error) {
    ret->h = h;
    ret->off = 0;
    ret->buf = 0;
    ret->len = 0;
    ret->impl = 0;
    ret->error = error;
  } else {
    ret->h = h;
    ret->off = off;
    ret->buf = buf;
    ret->len = len;
    ret->impl = 0;
    ret->error = 0;
  }

  return ret;
}

static int file_release_write_buffer(stasis_write_buffer_t * w) {

  file_impl * impl = (file_impl*)(w->h->impl);

  pthread_mutex_lock(&(impl->mut));
  int error = 0;
  if(impl->end_pos < w->off + w->len ||
     0 > w->off) {
    error = EDOM;
  }

  if(!error) {
    // Call write().
    error = file_write_unlocked(w->h, w->off, w->buf, w->len);
  }
  pthread_mutex_unlock(&(impl->mut));
  free(w->buf);
  free(w);
  return error;
}

static stasis_read_buffer_t * file_read_buffer(stasis_handle_t * h,
					      lsn_t off, lsn_t len) {
  stasis_read_buffer_t * ret = stasis_malloc(1, stasis_read_buffer_t);
  if(!ret) { return NULL; }

  byte * buf = stasis_malloc(len, byte);
  int error = 0;

  if(!buf) { error = ENOMEM; }

  if(!error) {
    error = file_read(h, off, buf, len);
  }

  if(error) {
    ret->h = h;
    ret->buf = 0;
    ret->off = 0;
    ret->len = 0;
    ret->impl = 0;
    ret->error = error;
    if(buf) { free(buf); }
  } else {
    ret->h = h;
    ret->buf = buf;
    ret->off = off;
    ret->len = len;
    ret->impl = 0;
    ret->error = 0;
  }
  return ret;
}
static int file_release_read_buffer(stasis_read_buffer_t * r) {
  free((void*)r->buf);
  free(r);
  return 0;
}
static int file_force(stasis_handle_t * h) {
  file_impl * impl = h->impl;

  if(!(impl->file_flags & O_SYNC)) {
    int fd = impl->fd;
#ifdef HAVE_FDATASYNC
    DEBUG("file_force() is calling fdatasync()\n");
    fdatasync(fd);
#else
    DEBUG("file_force() is calling fsync()\n");
    fsync(fd);
#endif
  } else {
    DEBUG("File was opened with O_SYNC.  file_force() is a no-op\n");
  }
  return 0;
}
static int file_async_force(stasis_handle_t *h) {
  file_impl * impl = h->impl;
  int ret = 0;
  if(!(impl->file_flags & O_SYNC)) {
    // not opened synchronously; we need to explicitly sync.
    pthread_mutex_lock(&impl->mut);
    int fd = impl->fd;
    pthread_mutex_unlock(&impl->mut);
    {
      static int warned = 0;
      if(!warned) {
                printf("Warning: There is a race condition between force_range() and "
                       " truncate() in file.c (This shouldn't matter in practice, "
                       "as the logger hasn't moved over to use file.c yet.\n");
                warned = 1;
      }
    }
    //#ifdef HAVE_F_SYNC_RANGE
#ifdef HAVE_SYNC_FILE_RANGE
    DEBUG("Calling sync_file_range\n");
    ret = sync_file_range(fd, 0, 0, SYNC_FILE_RANGE_WRITE);
    if(ret) {
      int error = errno;
      assert(ret == -1);
      // With the possible exceptions of ENOMEM and ENOSPACE, all of the sync
      // errors are unrecoverable.
      h->error = EBADF;
      ret = error;
    }
#else
#ifdef HAVE_FDATASYNC
    DEBUG("file_force_range() is calling fdatasync()\n");
    fdatasync(fd);
#else
    DEBUG("file_force_range() is calling fsync()\n");
    fsync(fd);
#endif
    ret = 0;
#endif
  }
  return ret;
}
static int file_force_range(stasis_handle_t *h, lsn_t start, lsn_t stop) {
  file_impl * impl = h->impl;
  int ret = 0;
  if(!(impl->file_flags & O_SYNC)) {
    // not opened synchronously; we need to explicitly sync.
    pthread_mutex_lock(&impl->mut);
    int fd = impl->fd;
    pthread_mutex_unlock(&impl->mut);
    {
      static int warned = 0;
      if(!warned) {
        printf("Warning: There is a race condition between force_range() and "
               " truncate() in file.c (This shouldn't matter in practice, "
               "as the logger hasn't moved over to use file.c yet.\n");
        warned = 1;
      }
    }
    //#ifdef HAVE_F_SYNC_RANGE
#ifdef HAVE_SYNC_FILE_RANGE
    if(!stop) stop = impl->end_pos;
    DEBUG("Calling sync_file_range\n");
    ret = sync_file_range(fd, start, (stop-start),
			      SYNC_FILE_RANGE_WAIT_BEFORE |
			      SYNC_FILE_RANGE_WRITE |
			      SYNC_FILE_RANGE_WAIT_AFTER);
    if(ret) {
      int error = errno;
      assert(ret == -1);
      // With the possible exceptions of ENOMEM and ENOSPACE, all of the sync
      // errors are unrecoverable.
      h->error = EBADF;
      ret = error;
    }
#else
#ifdef HAVE_FDATASYNC
    DEBUG("file_force_range() is calling fdatasync()\n");
    fdatasync(fd);
#else
    DEBUG("file_force_range() is calling fsync()\n");
    fsync(fd);
#endif
    ret = 0;
#endif
  }
  return ret;
}
static int file_fallocate(struct stasis_handle_t* h, lsn_t off, lsn_t len) {
  file_impl * impl = h->impl;
#ifdef HAVE_POSIX_FALLOCATE
  return posix_fallocate(impl->fd, off, len);
#else
  (void)impl;
  fprintf(stderr, "file.c: fallocate called, but not supported by this build.\n");
  return -1;
#endif
}

struct stasis_handle_t file_func = {
  .num_copies = file_num_copies,
  .num_copies_buffer = file_num_copies_buffer,
  .close = file_close,
  .dup = file_dup,
  .enable_sequential_optimizations = file_enable_sequential_optimizations,
  .end_position = file_end_position,
  .write = file_write,
  .write_buffer = file_write_buffer,
  .release_write_buffer = file_release_write_buffer,
  .read = file_read,
  .read_buffer = file_read_buffer,
  .release_read_buffer = file_release_read_buffer,
  .force = file_force,
  .async_force = file_async_force,
  .force_range = file_force_range,
  .fallocate = file_fallocate,
  .error = 0
};

stasis_handle_t * stasis_handle(open_file)(const char * filename, int flags, int mode) {
  stasis_handle_t * ret = stasis_malloc(1, stasis_handle_t);
  if(!ret) { return NULL; }
  *ret = file_func;

  file_impl * impl = stasis_malloc(1, file_impl);
  ret->impl = impl;
  pthread_mutex_init(&(impl->mut), 0);
  assert(sizeof(off_t) >= (64/8));
  impl->fd = open(filename, flags, mode);
  if(impl->fd == -1) {
    ret->error = errno;
  }
  impl->filename = strdup(filename);
  impl->file_flags = flags;
  impl->file_mode = mode;
  updateEOF(ret);
  return ret;
}
