#include <config.h>

#include <stasis/io/handle.h>
#include <stasis/util/histogram.h>

#include <fcntl.h>
#include <stdio.h>
#include <assert.h>

//#define PFILE_LATENCY_PROF

#ifdef PFILE_LATENCY_PROF
DECLARE_HISTOGRAM_64(read_hist)
DECLARE_HISTOGRAM_64(write_hist)
DECLARE_HISTOGRAM_64(force_hist)
DECLARE_HISTOGRAM_64(force_range_hist)
#define TICK(hist) stasis_histogram_tick(&hist)
#define TOCK(hist) stasis_histogram_tock(&hist)
#else
#define TICK(hist)
#define TOCK(hist)
#endif

/**
   @file

   Implementation of a file-backed io handle.  This implementation uses
   pread() and pwrite() to access the handle.

   The functions defined here implement interfaces documented in handle.h

   @see handle.h
*/

/**
   Per-handle information for pfile
*/
typedef struct pfile_impl {
  /**
    File descriptor
  */
  int fd;
  /**
    Flags passed into open
  */
  int file_flags;
  /**
    File creation mode
  */
  int file_mode;
  /**
    The name of the underlying file.
  */
  char *filename;
  /**
   * If 1, then do some extra stuff (like fadvise(WONTNEED) on sync).
   */
  int sequential;
} pfile_impl;

/**
   We can pass the caller's buffer directly into pread()/pwrite()
   without making any copies.
*/
static int pfile_num_copies(stasis_handle_t *h) { return 0; }
/**
   We have to call malloc(), but not memcpy().  Maybe this should return 1.
*/
static int pfile_num_copies_buffer(stasis_handle_t *h) { return 0; }

static int pfile_close(stasis_handle_t *h) {
  pfile_impl *impl = (pfile_impl*)h->impl;
  int fd = impl->fd;
  free((void*)impl->filename);
  free(impl);
  free(h);
  int ret = close(fd);
  if (!ret) return 0;
  else     return errno;
}

static stasis_handle_t * pfile_dup(stasis_handle_t *h) {
  pfile_impl *impl = h->impl;
  return stasis_handle_open_pfile(impl->filename, impl->file_flags, impl->file_mode);
}
static void pfile_enable_sequential_optimizations(stasis_handle_t *h) {
  pfile_impl *impl = h->impl;
  impl->sequential = 1;
  int err = posix_fadvise(impl->fd, 0, 0, POSIX_FADV_SEQUENTIAL);
  if(err) perror("Attempt to pass POSIX_FADV_SEQUENTIAL to kernel failed");
}
static lsn_t pfile_end_position(stasis_handle_t *h) {
  pfile_impl *impl = (pfile_impl*)h->impl;
  lsn_t ret = lseek(impl->fd, 0, SEEK_END);
  return ret;
}

inline static int pfile_write_unlocked(int fd, lsn_t off, const byte *dat,
                                       lsn_t len) {
  int error = 0;
  ssize_t bytes_written = 0;
  TICK(write_hist);
  while (bytes_written < len) {

    ssize_t count = pwrite(fd,
                           dat + bytes_written,
                           len - bytes_written,
                           off + bytes_written);

    if (count == -1) {
      if (errno == EAGAIN || errno == EINTR) {
        // @see file.c for an explanation; basically; we ignore these,
        // and try again.
        count = 0;
      } else {
        if (errno == EBADF) {
          error = EBADF;
        } else {
          error = errno;
        }
        break;
      }
    }
    bytes_written += count;
    if (bytes_written != len) {
      DEBUG("pwrite spinning\n");
    }
  }
  TOCK(write_hist);
  return error;
}

static int pfile_read(stasis_handle_t *h, lsn_t off, byte *buf, lsn_t len) {
  pfile_impl *impl = (pfile_impl*)(h->impl);
  int error = 0;

  if (off < 0) {
    error = EDOM;
  } else {
    ssize_t bytes_read = 0;
    TICK(read_hist);
    while (bytes_read < len) {
      ssize_t count = pread(impl->fd,
                            buf + bytes_read,
                            len - bytes_read,
                            off + bytes_read);
      if (count == -1) {
        if (errno == EAGAIN || errno == EINTR) {
          count = 0;
        } else {
          if (errno == EBADF) {
            h->error = EBADF;
          } else {
            int err = errno;
            // The other errors either involve memory bugs (EFAULT), logic bugs
            // (EISDIR, EFIFO, EOVERFLOW), or bad hardware (EIO), so print
            // something to console, and uncleanly crash.
            perror("pfile_read encountered an unknown error code.");
            fprintf(stderr, "pread() returned -1; errno is %d\n",err);
            abort();
          }
          error = errno;
          break;
        }
      } else if(count == 0) {
        // EOF
        if(bytes_read != 0) {
          fprintf(stderr, "short read at end of storefile.  Assuming that this is due to strange recovery scenario, and continuing.\n");
        }
        error = EDOM;
        break;
      } else {
        bytes_read += count;
        if (bytes_read != len) {
          DEBUG("pread spinning\n");
        }
      }
    }
    TOCK(read_hist);
    assert(error || bytes_read == len);
  }
  return error;
}

static int pfile_write(stasis_handle_t *h, lsn_t off, const byte *dat,
                       lsn_t len) {
  pfile_impl *impl = (pfile_impl*)(h->impl);
  int error = 0;
  if (off < 0) {
    error = EDOM;
  } else {
    error = pfile_write_unlocked(impl->fd, off, dat, len);
  }
  return error;
}

static stasis_write_buffer_t * pfile_write_buffer(stasis_handle_t *h,
                                                 lsn_t off, lsn_t len) {
  stasis_write_buffer_t *ret = malloc(sizeof(stasis_write_buffer_t));

  if (!ret) {
    h->error = ENOMEM;
    return NULL;
  }

  int error = 0;

  if (off < 0) {
    error = EDOM;
  }

  byte *buf;
  if (!error) {
    buf = malloc(len);
    if (!buf) { error = ENOMEM; }
  }
  if (error) {
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

static int pfile_release_write_buffer(stasis_write_buffer_t *w) {
  pfile_impl *impl = (pfile_impl*)(w->h->impl);

  int error = pfile_write_unlocked(impl->fd, w->off, w->buf,
                                 w->len);

  if (w->buf) {
    free(w->buf);
  }
  free(w);
  return error;
}

static stasis_read_buffer_t *pfile_read_buffer(stasis_handle_t *h,
					      lsn_t off, lsn_t len) {
  stasis_read_buffer_t *ret = malloc(sizeof(stasis_read_buffer_t));
  if (!ret) { return NULL; }

  byte *buf = malloc(len);
  int error = 0;

  if (!buf) { error = ENOMEM; }

  if (!error) {
    error = pfile_read(h, off, buf, len);
  }

  if (error) {
    ret->h = h;
    ret->buf = 0;
    ret->off = 0;
    ret->len = 0;
    ret->impl = 0;
    ret->error = error;
    if (buf) { free(buf); }
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

static int pfile_release_read_buffer(stasis_read_buffer_t *r) {
  if (r->buf) {
    free((void*)r->buf);
  }
  free(r);
  return 0;
}
static int pfile_force(stasis_handle_t *h) {
  TICK(force_hist);
  pfile_impl *impl = h->impl;
  if(!(impl->file_flags & O_SYNC)) {
#ifdef HAVE_FDATASYNC
    DEBUG("pfile_force() is calling fdatasync()\n");
    fdatasync(impl->fd);
#else
    DEBUG("pfile_force() is calling fsync()\n");
    fsync(impl->fd);
#endif
  } else {
    DEBUG("File was opened with O_SYNC.  pfile_force() is a no-op\n");
  }
  if(impl->sequential) {
    int err = posix_fadvise(impl->fd, 0, 0, POSIX_FADV_DONTNEED);
    if(err) perror("Attempt to pass POSIX_FADV_SEQUENTIAL to kernel failed");
  }
  TOCK(force_hist);
  return 0;
}
static int pfile_async_force(stasis_handle_t *h) {
  TICK(force_range_hist);
  pfile_impl * impl = h->impl;
#ifdef HAVE_SYNC_FILE_RANGE
  // stop of zero syncs to eof.
  DEBUG("pfile_force_range calling sync_file_range %lld %lld\n",
                 start, stop-start); fflush(stdout);
  int ret = sync_file_range(impl->fd, 0, 0, SYNC_FILE_RANGE_WAIT_BEFORE);
  ret |= sync_file_range(impl->fd, 0, 0, SYNC_FILE_RANGE_WRITE);
                                                     
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
  DEBUG("pfile_force_range() is calling fdatasync()\n");
  fdatasync(impl->fd);
#else
  DEBUG("pfile_force_range() is calling fsync()\n");
  fsync(impl->fd);
#endif
  int ret = 0;
#endif
  if(impl->sequential) {
    int err = posix_fadvise(impl->fd, 0, 0, POSIX_FADV_DONTNEED);
    if(err) perror("Attempt to pass POSIX_FADV_SEQUENTIAL (for a range of a file) to kernel failed");
  }
  TOCK(force_range_hist);
  return ret;
}
static int pfile_force_range(stasis_handle_t *h, lsn_t start, lsn_t stop) {
  TICK(force_range_hist);
  pfile_impl * impl = h->impl;
#ifdef HAVE_SYNC_FILE_RANGE
  // stop of zero syncs to eof.
  DEBUG("pfile_force_range calling sync_file_range %lld %lld\n",
	 start, stop-start); fflush(stdout);
  int ret = sync_file_range(impl->fd, start, stop-start,
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
  DEBUG("pfile_force_range() is calling fdatasync()\n");
  fdatasync(impl->fd);
#else
  DEBUG("pfile_force_range() is calling fsync()\n");
  fsync(impl->fd);
#endif
  int ret = 0;
#endif
  if(impl->sequential) {
    int err = posix_fadvise(impl->fd, start, stop-start, POSIX_FADV_DONTNEED);
    if(err) perror("Attempt to pass POSIX_FADV_SEQUENTIAL (for a range of a file) to kernel failed");
  }
  TOCK(force_range_hist);
  return ret;
}
static int pfile_fallocate(struct stasis_handle_t* h, lsn_t off, lsn_t len) {
  pfile_impl * impl = h->impl;
#ifdef HAVE_POSIX_FALLOCATE
  return posix_fallocate(impl->fd, off, len);
#else
  (void)impl;
  fprintf(stderr, "pfile.c: fallocate called, but not supported by this build.\n");
  return -1;
#endif  
}
static struct stasis_handle_t pfile_func = {
  .num_copies = pfile_num_copies,
  .num_copies_buffer = pfile_num_copies_buffer,
  .close = pfile_close,
  .dup = pfile_dup,
  .enable_sequential_optimizations = pfile_enable_sequential_optimizations,
  .end_position = pfile_end_position,
  .write = pfile_write,
  .write_buffer = pfile_write_buffer,
  .release_write_buffer = pfile_release_write_buffer,
  .read = pfile_read,
  .read_buffer = pfile_read_buffer,
  .release_read_buffer = pfile_release_read_buffer,
  .force = pfile_force,
  .async_force = pfile_async_force,
  .force_range = pfile_force_range,
  .fallocate = pfile_fallocate,
  .error = 0
};

stasis_handle_t *stasis_handle(open_pfile)(const char *filename,
                                           int flags, int mode) {
  stasis_handle_t *ret = malloc(sizeof(stasis_handle_t));
  if (!ret) { return NULL; }
  *ret = pfile_func;

  pfile_impl *impl = malloc(sizeof(pfile_impl));
  if (!impl) { free(ret); return NULL; }

  ret->impl = impl;
  impl->fd = open(filename, flags, mode);
  assert(sizeof(off_t) >= (64/8));
  if (impl->fd == -1) {
    ret->error = errno;
  }

  impl->filename = strdup(filename);
  impl->file_flags = flags;
  impl->file_mode = mode;
  impl->sequential = 0;
  assert(!ret->error);
  return ret;
}
