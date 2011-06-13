#include <config.h>
#include <stasis/common.h>
#include <dirent.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <stasis/flags.h>

#include <stasis/util/ringbuffer.h>
#include <stasis/util/crc32.h>
#include <stasis/util/latches.h>
#include <stasis/logger/filePool.h>

#include <stdio.h>
#include <assert.h>

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

/**
   Latch order: ringbuffer range, chunk state
 */
typedef struct {
  const char * dirname;

  int live_count;
  int dead_count;

  lsn_t target_chunk_size;

  /**
     An array of filenames that contain live log data, lowest LSN first.
  */
  char ** live_filenames;
  /**
     The offset of each log segment.  The LSN is of an internal log
     entry placed at file offset zero.
   */
  lsn_t * live_offsets;

  /**
     An array of filenames that do not contain live data.  These
     filenames end in '~', and may be safely unlinked whether or not
     stasis is running.  Reusing these files saves FS allocation
     overhead, and avoids fragmentation over time.
   */
  char ** dead_filenames;

  /**
     An array of read-only file descriptors.  If an entry is -1,
     then the file is not open.  Offsets match those of
     live_filenames.
   */
  int * ro_fd;

  int filemode;
  int fileperm;
  char softcommit;

  pthread_t write_thread;
  pthread_t write_thread2;
  stasis_ringbuffer_t * ring;
  /** Need this because the min aggregate in the ringbuffer doesn't
   * want to malloc keys, but needs to maintain some sort of state
   * for each log operation.
   */
  pthread_key_t handle_key;

  pthread_mutex_t mut;
} stasis_log_file_pool_state;

enum file_type {
  UNKNOWN = 0,
  LIVE,
  DEAD
};
/**
 * No latching required.  Does not touch shared state.
 */
enum file_type stasis_log_file_pool_file_type(const struct dirent* file, lsn_t *lsn) {
  const char* name = file->d_name;

  // According to the linux readdir manpage, all apps have to deal
  // with getting unexpected DT_UNKNOWN values.
  // Optimistically assume that it's a file, and (possibly)
  // end up with a bizarre error later.

  // TODO call lstat on DT_UNKNOWN
  if(!(DT_REG == file->d_type
     ||DT_LNK == file->d_type
     ||DT_UNKNOWN == file->d_type)) {
    return UNKNOWN;
  }
  off_t base_len = strlen(stasis_log_chunk_name);
  if(strncmp(name, stasis_log_chunk_name, base_len)) {
    return UNKNOWN;
  }
  name+=base_len;
  char * nameend;
  *lsn = strtoull(name,&nameend,10);
  if(nameend-name == stasis_log_file_pool_lsn_chars) {
    return
      (nameend[0] == '\0') ? LIVE :
      (nameend[0] == '~' && nameend[1] == '\0')  ? DEAD :
      UNKNOWN;
  } else {
    return UNKNOWN;
  }
}
/**
 * No latching required.  Does not touch shared state.
 */
int stasis_log_file_pool_file_filter(const struct dirent* file) {
  lsn_t junk;
  if(UNKNOWN != stasis_log_file_pool_file_type(file, &junk)) {
    return 1;
  } else {
    if(file->d_name[0] != '.') {
      printf("Unknown file in log dir: %s\n", file->d_name);
    }
    return 0;
  }
}
/**
 * No latching required.  Only touches immutable shared state.
 */
char * stasis_log_file_pool_build_filename(stasis_log_file_pool_state * fp,
					lsn_t start_lsn) {
  int name_len = strlen(stasis_log_chunk_name);
  char * first = malloc(name_len + stasis_log_file_pool_lsn_chars + 1);
  strcpy(first, stasis_log_chunk_name);
  sprintf(first+name_len, "%020lld", start_lsn);
  printf("Name is %s\n", first);
  return first;
}

/**
 *  Same API as pread(), except it never performs a short read.
 *
 *  No latching required.
 */
static ssize_t mypread(int fd, byte * buf, size_t sz, off_t off) {
  size_t rem = sz;
  while(rem) {
    DEBUG("pread(%d, %lld, %lld, %lld)\n", fd, (long long)(intptr_t)buf, (long long)rem, (long long)off);
    size_t ret = pread(fd, buf, rem, off);
    if(ret == -1) {
      perror("Error reading from log.");
      abort();
      return -1;
    }
    if(ret == 0)  { return 0; }
    off += ret;
    buf += ret;
    rem  -= ret;
  }
  return sz;
}
/**
 * No-op; no latching.
 */
static void stasis_log_file_pool_set_truncation(stasis_log_t* log, stasis_truncation_t *trunc) {
  // ths module does not support hard limits on its size, so this is a no-op
}
/**
 * Unsupported, as this log format does not introduce internal entries.
 */
lsn_t stasis_log_file_pool_sizeof_internal_entry(stasis_log_t * log, const LogEntry *e) {
  abort();
}
// No latching requried.
char * build_path(const char * dir, const char * file) {
  char * full_name = malloc(strlen(file) + 1 + strlen(dir) + 1);
  full_name[0] = 0;
  strcat(full_name, dir);
  strcat(full_name, "/");
  strcat(full_name, file);
  return full_name;
}

/**
 * Does not perform latching.  Modifies fp->live_filenames, and fp->ro, which must
 * not concurrently change.
 */
void stasis_log_file_pool_chunk_open(stasis_log_file_pool_state * fp, int chunk) {
  char* full_name = malloc(strlen(fp->dirname) + 1 + strlen(fp->live_filenames[chunk]) + 1);
  full_name[0] = 0;
  strcat(full_name, fp->dirname);
  strcat(full_name, "/");
  strcat(full_name, fp->live_filenames[chunk]);

  fp->ro_fd[chunk] = open(full_name, fp->filemode, fp->fileperm);
  free(full_name);
}
/**
 * Does no latching.  Relies on stability of fp->live_offsets and fp->live_count.
 */
static int get_chunk_from_offset(stasis_log_t * log, lsn_t lsn) {
  stasis_log_file_pool_state * fp = log->impl;
  int chunk = -1;
  if(fp->live_offsets[fp->live_count-1] <= lsn && (fp->live_offsets[fp->live_count-1] + fp->target_chunk_size) > lsn) {
    return fp->live_count - 1;
  }
  for(int i = 0; i < fp->live_count; i++) {
    if(fp->live_offsets[i] > lsn) { chunk = i - 1; break; }
  }
  return chunk;
}
/**
 * Does no latching.  Modifies all mutable fields of fp.
 */
int stasis_log_file_pool_append_chunk(stasis_log_t * log, off_t new_offset) {
  stasis_log_file_pool_state * fp = log->impl;
  char * old_file = 0;
  char * new_file = stasis_log_file_pool_build_filename(fp, new_offset);
  char * new_path = build_path(fp->dirname, new_file);
  fp->live_filenames = realloc(fp->live_filenames, sizeof(char*) * (fp->live_count+1));
  fp->live_offsets   = realloc(fp->live_offsets,   sizeof(lsn_t) * (fp->live_count+1));
  fp->ro_fd          = realloc(fp->ro_fd,          sizeof(int)   * (fp->live_count+1));
  fp->live_filenames[fp->live_count] = new_file;
  fp->live_offsets[fp->live_count] = new_offset;
  if(fp->dead_count) {
    old_file = fp->dead_filenames[fp->dead_count-1];
    fp->dead_count--;
    char * old_path = build_path(fp->dirname, old_file);
    int err = rename(old_path, new_path);
    if(err) {
      assert(err == -1);
      perror("Could not rename old log file.");
      abort();
    }
    free(old_path);
  }
  fp->ro_fd[fp->live_count] = open(new_path, fp->filemode, fp->fileperm);

  free(new_path);
  fp->live_count++;
  return fp->live_count-1;
}
/**
 * Appends a new chunk to the log if necessary.  Holds mut while checking and
 * appending chunk.  (After reserving space in the ringbuffer).
 */
LogEntry * stasis_log_file_pool_reserve_entry(stasis_log_t * log, size_t szs) {
  uint32_t sz = szs;
  stasis_log_file_pool_state * fp = log->impl;
  lsn_t * handle = pthread_getspecific(fp->handle_key);
  if(!handle) { handle = malloc(sizeof(*handle)); pthread_setspecific(fp->handle_key, handle); }

  uint64_t framed_size = sz+sizeof(uint32_t)+sizeof(uint32_t);
  lsn_t off  = stasis_ringbuffer_reserve_space(fp->ring, framed_size, handle);

  pthread_mutex_lock(&fp->mut);
  int endchunk = get_chunk_from_offset(log, off + sz + 2*sizeof(uint32_t));
  //if(chunk >= fp->live_count || (off + (sz+2*sizeof(uint32_t)) > fp->live_offsets[chunk] + fp->target_chunk_size)) {
  if(endchunk == -1) { //>= fp->live_count) {
    stasis_log_file_pool_append_chunk(log, off);
    int chunk = get_chunk_from_offset(log, off);
    assert(chunk == fp->live_count-1);
  }
  lsn_t barrier = fp->live_offsets[fp->live_count - 1]-1;
  pthread_mutex_unlock(&fp->mut);

  if(barrier < off) {
    stasis_ringbuffer_flush(fp->ring, barrier);
  }

  byte * buf = stasis_ringbuffer_get_wr_buf(fp->ring, off, framed_size);

  memcpy(buf,            &sz, sizeof(uint32_t));
  LogEntry * e = (LogEntry*)(buf + (2 * sizeof(uint32_t)));
  e->LSN = off;

  return e;
}
/**
 * Does no latching, but does call ringbuffer.
 */
int stasis_log_file_pool_write_entry_done(stasis_log_t * log, LogEntry * e) {
  stasis_log_file_pool_state * fp = log->impl;
  lsn_t * handle = pthread_getspecific(fp->handle_key);
  assert(handle);
  byte * buf = (byte*)e;
  lsn_t sz = sizeofLogEntry(log, e);

  assert(*(((uint32_t*)buf)-2)==sz);

  // TODO figure out how to move this computation into background threads.
  // One possibility is to enqueue the size, len on some other ringbuffer, and
  // have workers dequeue entries to checksum.  It's not clear it would be worth
  // the extra synchronization overhead...
  *(((uint32_t*)buf)-1) = stasis_crc32(buf, sz, (uint32_t)-1);
  stasis_ringbuffer_write_done(fp->ring, handle);
  return 0;
}
/**
 * Does no latching.  (no-op)
 */
int stasis_log_file_pool_write_entry(stasis_log_t * log, LogEntry * e) {
  // no-op; the entry is written into the ringbuffer in place.
  return 0;
}
/**
 * Does no latching.  No shared state, except for fd, which is
 * protected from being closed by truncation.
 */
const LogEntry* stasis_log_file_pool_chunk_read_entry(stasis_log_file_pool_state * fp, int fd, lsn_t file_offset, lsn_t lsn, uint32_t * len) {
  int err;
  if(sizeof(*len) != (err = mypread(fd, (byte*)len, sizeof(*len), lsn-file_offset))) {
    if(err == 0) { DEBUG(stderr, "EOF reading len from log\n"); return 0; }
    abort();
  }
  if(*len == 0) { DEBUG(stderr, "Reached end of log\n"); return 0; }

  // Force bytes containing body of log entry to disk.
  if(fp->ring) {  // if not, then we're in startup, and don't need to flush.
    if(stasis_ringbuffer_get_write_frontier(fp->ring) > lsn) {
      stasis_ringbuffer_flush(fp->ring, lsn+sizeof(uint32_t)+*len);
    } else {
      // there is a ringbuffer, and the read is past the eof.
      // this should only happen at the end of recovery's forward
      // scans.
      return 0;
    }
  }

  byte * buf = malloc(*len + sizeof(uint32_t));
  if(!buf) {
    fprintf(stderr, "Couldn't alloc memory for log entry of size %lld.  "
           "This could be due to corruption at the end of the log.  Conservatively bailing out.",
           (long long)*len);
    abort();
  }

  if((*len)+sizeof(uint32_t) != (err = mypread(fd, (byte*)buf, (*len) + sizeof(uint32_t), sizeof(*len)+ lsn - file_offset))) {
    if(err == 0) { fprintf(stderr, "EOF reading payload from log.  Assuming unclean shutdown and continuing.\n"); free(buf); return 0; }
    abort();
  }
  uint32_t logged_crc = *(uint32_t*)(buf);
  uint32_t calc_crc = (uint32_t)stasis_crc32(buf+sizeof(uint32_t), *len, (uint32_t)-1);
  if(logged_crc != calc_crc) {
    // crc does not match
    fprintf(stderr, "CRC mismatch reading from log.  LSN %lld Got %d, Expected %d", lsn, calc_crc, logged_crc);
    free(buf);
    return 0;
  } else {
    return (const LogEntry*)(buf+sizeof(uint32_t));
  }
}
/**
 * Does no latching.  No shared state, except for fd, which is
 * protected by the ringbuffer.
 */
int stasis_log_file_pool_chunk_write_buffer(int fd, const byte * buf, size_t sz, lsn_t file_offset, lsn_t lsn) {
  size_t rem = sz;
  while(rem) {
    ssize_t ret = pwrite(fd, buf, rem, lsn-file_offset);
    if(ret == -1) { fprintf(stderr, "fd is %d\n", fd); perror("error writing to log"); return 0; }
    assert(ret != 0);
    rem -= ret;
    buf += ret;
    lsn += ret;
  }
  return 1;
}
const LogEntry* stasis_log_file_pool_read_entry(struct stasis_log_t* log, lsn_t lsn) {
  stasis_log_file_pool_state * fp = log->impl;
  if(fp->ring) {
    // Force bytes containing length of log entry to disk.
    if(stasis_ringbuffer_get_write_frontier(fp->ring) > lsn) {
      stasis_ringbuffer_flush(fp->ring, lsn+sizeof(uint32_t));
    } else {
      // end of log
      return 0;
    }
  } // else, we haven't finished initialization, so there are no bytes to flush.
  const LogEntry * e;
  pthread_mutex_lock(&fp->mut);
  int chunk = get_chunk_from_offset(log, lsn);
  if(chunk == -1) {
    pthread_mutex_unlock(&fp->mut);
    e = NULL;
  } else {
    // Need to hold mutex while accessing array (since the array could
    // be realloced()), but the values we read from it cannot change
    // while the fd is open.  Callers are not  allowed to truncate the
    // log past the LSN of outstanding reads, so our fd and associated
    // offset will be stable throughout this call..
    int fd    = fp->ro_fd[chunk];
    lsn_t off = fp->live_offsets[chunk];
    pthread_mutex_unlock(&fp->mut);
    // Be sure not to hold a mutex while hitting disk.
    uint32_t len;
    e = stasis_log_file_pool_chunk_read_entry(fp, fd, off, lsn, &len);
    if(e) { assert(sizeofLogEntry(log, e) == len); }
  }
  return e;
}
/**
 * Does no latching.  No shared state.
 */
void stasis_log_file_pool_read_entry_done(struct stasis_log_t *log, const LogEntry *e) {
  free((void*)((byte*)e-sizeof(uint32_t)));
}
/**
 * Does no latching.  No shared state.
 */
lsn_t stasis_log_file_pool_next_entry(struct stasis_log_t* log, const LogEntry * e) {
  return e->LSN + sizeofLogEntry(log, e) + sizeof(uint32_t) + sizeof(uint32_t);
}
/**
 * Does no latching.  Relies on ringbuffer for synchronization.
 */
lsn_t stasis_log_file_pool_first_unstable_lsn(struct stasis_log_t* log, stasis_log_force_mode_t mode) {
  // TODO this ignores mode...
  stasis_log_file_pool_state * fp = log->impl;
  return stasis_ringbuffer_get_read_tail(fp->ring);
}
/**
 * Does no latching.  Relies on ringbuffer for synchronization.
 */
lsn_t stasis_log_file_pool_first_pending_lsn(struct stasis_log_t* log) {
  stasis_log_file_pool_state * fp = log->impl;
  return stasis_ringbuffer_get_write_tail(fp->ring);
}
/**
 * Does no latching.  Relies on ringbuffer for synchronization.
 */
void stasis_log_file_pool_force_tail(struct stasis_log_t* log, stasis_log_force_mode_t mode) {
  stasis_log_file_pool_state * fp = log->impl;
  stasis_ringbuffer_flush(fp->ring, stasis_ringbuffer_get_write_frontier(fp->ring));
}
/**
 * Does no latching.  Relies on ringbuffer for synchronization.
 */
lsn_t stasis_log_file_pool_next_available_lsn(stasis_log_t *log) {
  stasis_log_file_pool_state * fp = log->impl;
  return stasis_ringbuffer_get_write_frontier(fp->ring);//nextAvailableLSN;
}
/**
 * Modifies all fields of fp.  Holds latches.
 */
int stasis_log_file_pool_truncate(struct stasis_log_t* log, lsn_t lsn) {
  stasis_log_file_pool_state * fp = log->impl;
  pthread_mutex_lock(&fp->mut);
  int chunk = get_chunk_from_offset(log, lsn);
  int dead_offset = fp->dead_count;
  fp->dead_filenames = realloc(fp->dead_filenames, sizeof(char**) * (dead_offset + chunk));
  for(int i = 0; i < chunk; i++) {
    fp->dead_filenames[dead_offset + i] = malloc(strlen(fp->live_filenames[i]) + 2);
    fp->dead_filenames[dead_offset + i][0] = 0;
    strcat(fp->dead_filenames[dead_offset + i], fp->live_filenames[i]);
    strcat(fp->dead_filenames[dead_offset + i], "~");
    char * old = build_path(fp->dirname, fp->live_filenames[i]);
    char * new = build_path(fp->dirname, fp->dead_filenames[dead_offset + i]);
    // TODO This is the only place where we hold the latch while going to disk.
    // Rename should be fast, but we're placing a lot of faith in the filesystem.
    int err = rename(old, new);
    if(err) {
      assert(err == -1);
      perror("could not rename file");
      abort();
    }
    close(fp->ro_fd[i]);
    free(old);
    free(new);
  }
  fp->dead_count += chunk;
  for(int i = 0; i < (fp->live_count - chunk); i++) {
    fp->live_filenames[i] = fp->live_filenames[chunk+i];
    fp->live_offsets[i] = fp->live_offsets[chunk+i];
    fp->ro_fd[i] = fp->ro_fd[chunk+i];
  }
  fp->live_count = fp->live_count - chunk;
  pthread_mutex_unlock(&fp->mut);
  return 0;
}
/**
 * Grabs mut so that it can safely read fp->live_offsets[0].
 */
lsn_t stasis_log_file_pool_truncation_point(struct stasis_log_t* log) {
  stasis_log_file_pool_state * fp = log->impl;
  pthread_mutex_lock(&fp->mut);
  lsn_t ret = fp->live_offsets[0];
  pthread_mutex_unlock(&fp->mut);
  return ret;
}
/**
 * Does no latching. (Thin wrapper atop thread safe methods.)
 */
lsn_t stasis_log_file_pool_chunk_scrub_to_eof(stasis_log_t * log, int fd, lsn_t file_off) {
  lsn_t cur_off = file_off;
  const LogEntry * e;
  uint32_t len;
  while((e = stasis_log_file_pool_chunk_read_entry(log->impl, fd, file_off, cur_off, &len))) {
    cur_off = log->next_entry(log, e);
    log->read_entry_done(log, e);
  }
  return cur_off;
}
/**
 * Does no latching.  Only one thread may call this method at a time, and the
 * first thing it does is shut down the writeback thread.
 */
int stasis_log_file_pool_close(stasis_log_t * log) {
  stasis_log_file_pool_state * fp = log->impl;

  log->force_tail(log, 0); /// xxx use real constant for wal mode..

  stasis_ringbuffer_shutdown(fp->ring);

  pthread_join(fp->write_thread, 0);
//  pthread_join(fp->write_thread2, 0);

  stasis_ringbuffer_free(fp->ring);

  for(int i = 0; i < fp->live_count; i++) {
  close(fp->ro_fd[i]);
    free(fp->live_filenames[i]);
  }
  for(int i = 0; i < fp->dead_count; i++) {
    free(fp->dead_filenames[i]);
  }
  free((void*)fp->dirname);
  free(fp->ro_fd);
  free(fp->live_offsets);
  free(fp->live_filenames);
  free(fp->dead_filenames);
  free(fp);
  free(log);
  return 0;
}

void * stasis_log_file_pool_writeback_worker(void * arg) {
  stasis_log_t * log = arg;
  stasis_log_file_pool_state * fp = log->impl;

  lsn_t handle;
  lsn_t off;
  while(1) {
    lsn_t len = 16*1024*1024;
    off = stasis_ringbuffer_consume_bytes(fp->ring, &len, &handle);
    const byte * buf = stasis_ringbuffer_get_rd_buf(fp->ring, off, len);

    if(off == RING_CLOSED) break;
    pthread_mutex_lock(&fp->mut);
    int chunk = get_chunk_from_offset(log, off);
    int endchunk = get_chunk_from_offset(log, off + len);
    // build vector of write operations.
    int* fds = malloc(sizeof(int) * (1 + endchunk-chunk));
    lsn_t* file_offs = malloc(sizeof(lsn_t) * (1 + endchunk-chunk));
    for(int c = chunk; c <= endchunk; c++) {
      fds[c-chunk]       = fp->ro_fd[c];
      file_offs[c-chunk] = fp->live_offsets[c];
    }
    assert(endchunk != -1);
    pthread_mutex_unlock(&fp->mut);
    lsn_t bytes_written = 0;
    for(int c = chunk; c <= endchunk; c++){
      lsn_t write_len;
      if(c < endchunk) {
        write_len = fp->live_offsets[c+1] - (off+bytes_written);
      } else {
        write_len = len - bytes_written;
      }
      int succ = stasis_log_file_pool_chunk_write_buffer(fds[c-chunk], buf+bytes_written, write_len, file_offs[c-chunk], off+bytes_written);
      assert(succ);
      bytes_written += write_len;

      if(c < endchunk) {
        uint32_t zero = 0;
        // Close current log file.  This might increase its length
        // by a byte or so, but that's the filesystem's problem.
        succ = stasis_log_file_pool_chunk_write_buffer(
            fds[c-chunk],
            (const byte*)&zero,
            sizeof(zero),
            file_offs[c-chunk],
            off+bytes_written);  // don't count this write toward bytes written, since it's not logically part of the log.
        assert(succ);
      }
    }
    free(file_offs);
    free(fds);
    stasis_ringbuffer_read_done(fp->ring, &handle);
  }
  return 0;
}
/**
 * Does no latching.  No shared state.
 */
void key_destr(void * key) { free(key); }
/**
 * Does no latching.  No shared state.
 */
int filesort(const struct dirent ** a, const struct dirent ** b) {
  int ret = strcmp((*a)->d_name, (*b)->d_name);
  DEBUG("%d = %s <=> %s\n", ret, (*a)->d_name, (*b)->d_name);
  return ret;
}
/**
 * Does no latching.  Implicitly holds latch on log until it returns it.
 * Spawns writeback thread immediately before returning.
 */
stasis_log_t* stasis_log_file_pool_open(const char* dirname, int filemode, int fileperm) {
  struct dirent **namelist;
  stasis_log_file_pool_state* fp = malloc(sizeof(*fp));
  stasis_log_t * ret = malloc(sizeof(*ret));

  static const stasis_log_t proto = {
    stasis_log_file_pool_set_truncation,
    stasis_log_file_pool_sizeof_internal_entry,
    stasis_log_file_pool_write_entry,
    stasis_log_file_pool_reserve_entry,
    stasis_log_file_pool_write_entry_done,
    stasis_log_file_pool_read_entry,
    stasis_log_file_pool_read_entry_done,
    stasis_log_file_pool_next_entry,
    stasis_log_file_pool_first_unstable_lsn,
    stasis_log_file_pool_first_pending_lsn,
    stasis_log_file_pool_next_available_lsn,
    stasis_log_file_pool_force_tail,
    stasis_log_file_pool_truncate,
    stasis_log_file_pool_truncation_point,
    stasis_log_file_pool_close,
    0,//stasis_log_file_pool_is_durable,
  };
  memcpy(ret, &proto, sizeof(proto));
  ret->impl = fp;

  fp->dirname = strdup(dirname);
  pthread_mutex_init(&fp->mut, 0);
  struct stat st;
  while(stat(dirname, &st)) {
    if(errno == ENOENT) {
      if(mkdir(dirname, filemode | 0711)) {
        perror("Error creating stasis log directory");
        return 0;
      }
    } else {
      perror("Couldn't stat stasis log directory");
      return 0;
    }
    char * file_name = stasis_log_file_pool_build_filename(fp, 1);
    char * full_name = build_path(fp->dirname, file_name);
    int fd = creat(full_name, stasis_log_file_permissions);
    free(file_name);
    free(full_name);
    if(fd == -1) { perror("Could not creat() initial log file."); abort(); }
    close(fd);
  }
  if(!S_ISDIR(st.st_mode)) {
    printf("Stasis log directory %s exists and is not a directory!\n", dirname);
    return 0;
  }

  int n = scandir(dirname, &namelist, stasis_log_file_pool_file_filter, filesort);

  if(n < 0) {
    perror("couldn't scan log directory");
    free(fp);
    return 0;
  }

  fp->live_filenames = 0;
  fp->live_offsets = 0;
  fp->dead_filenames = 0;
  fp->ro_fd = 0;
  fp->live_count = 0;
  fp->dead_count = 0;

  fp->target_chunk_size = 64 * 1024 * 1024;

  fp->filemode = filemode | O_DSYNC;  /// XXX should not hard-code O_SYNC.
  fp->fileperm = fileperm;
  fp->softcommit = !(filemode & O_DSYNC);

  off_t current_target = 0;
  for(int i = 0; i < n; i++) {
    lsn_t lsn;

    switch(stasis_log_file_pool_file_type(namelist[i],&lsn)) {
    case UNKNOWN: {

      abort(); // bug in scandir?!?  Should have been filtered out...

    } break;
    case LIVE: {

      printf("Live file %s\n", namelist[i]->d_name);

      fp->live_filenames = realloc(fp->live_filenames,
                                   (fp->live_count+1) * sizeof(*fp->live_filenames));
      fp->live_offsets   = realloc(fp->live_offsets,
                                   (fp->live_count+1) * sizeof(*fp->live_offsets));
      fp->ro_fd          = realloc(fp->ro_fd,
                                   (fp->live_count+1) * sizeof(*fp->ro_fd));

      fp->live_filenames[fp->live_count] = strdup(namelist[i]->d_name);
      fp->live_offsets[fp->live_count]   = lsn;
      /*fo->ro_fd=*/ stasis_log_file_pool_chunk_open(fp, fp->live_count);

      // XXX check to see if this file contains valid data.  If not, then we crashed after creating this file, but before syncing the previous one.
      assert(lsn <= current_target || !current_target);
      char * full_name = build_path(fp->dirname, fp->live_filenames[fp->live_count]);
      if(!stat(full_name, &st)) {
        current_target = st.st_size + fp->live_offsets[fp->live_count];
      } else {
        perror("Could not stat file.");
      }
      free(full_name);
      (fp->live_count)++;
          ;
    } break;
    case DEAD: {

      fp->dead_filenames = realloc(fp->dead_filenames, sizeof(char**)*fp->dead_count+1);
      fp->dead_filenames[fp->dead_count] = strdup(namelist[i]->d_name);
      (fp->dead_count)++;

    } break;
    }

    free(namelist[i]);
  }
  free(namelist);

  lsn_t next_lsn;
  if(!fp->live_count) {
    next_lsn = 1;
    stasis_log_file_pool_append_chunk(ret, 1);
  } else {

    printf("Current log segment appears to be %s.  Scanning for next available LSN\n", fp->live_filenames[fp->live_count-1]);

    fp->ring = 0; // scrub calls read, which tries to flush the ringbuffer.  passing null disables the flush.

    next_lsn = stasis_log_file_pool_chunk_scrub_to_eof(ret, fp->ro_fd[fp->live_count-1], fp->live_offsets[fp->live_count-1]);

    printf("Scan returned %lld\n", (long long)next_lsn);
  }
  // The previous segment must have been forced to disk before we created the current one, so we're good to go.

  fp->ring = stasis_ringbuffer_init(26, next_lsn); // 64mb buffer
  pthread_key_create(&fp->handle_key, key_destr);

  pthread_create(&fp->write_thread, 0, stasis_log_file_pool_writeback_worker, ret);
//  pthread_create(&fp->write_thread2, 0, stasis_log_file_pool_writeback_worker, ret);

  return ret;
}

