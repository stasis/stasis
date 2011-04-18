#include <config.h>
#include <stasis/common.h>
#include <dirent.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <stasis/flags.h>

#include <stasis/util/ringbuffer.h>
#include <stasis/crc32.h>
#include <stasis/latches.h>
#include <stasis/logger/filePool.h>

#include <stdio.h>
#include <assert.h>
/**
   @see stasis_log_safe_writes_state for more documentation;
        identically named fields serve analagous purposes.

   Latch order: write_mutex, read_mutex, state_latch
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

//  lsn_t nextAvailableLSN;

  /**
     A file handle positioned at the current end of log.
   */
//  FILE * fp;

  int filemode;
  int fileperm;
  char softcommit;
  /**
     These are always the lsn of the first entry that might not be stable.
   */
  lsn_t flushedLSN_wal;
  lsn_t flushedLSN_commit;
  lsn_t flushedLSN_internal;

  pthread_t write_thread;

  pthread_mutex_t write_mutex;
  pthread_mutex_t read_mutex;

/**
     Held whenever manipulating state in this struct (with the
     exception of the file handles, which are protected by read and
     write mutex).
   */
  rwl* state_latch;

  stasis_ringbuffer_t * ring;
  /** Need this because the min aggregate in the ringbuffer doesn't
   * want to malloc keys, but needs to maintain some sort of state
   * for each log operation.
   */
  pthread_key_t handle_key;

  unsigned int crc;
} stasis_log_file_pool_state;

enum file_type {
  UNKNOWN = 0,
  LIVE,
  DEAD
};

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
  off_t base_len = strlen(stasis_log_dir_name);
  if(strncmp(name, stasis_log_dir_name, base_len)) {
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

char * stasis_log_file_pool_build_filename(stasis_log_file_pool_state * fp,
					lsn_t start_lsn) {
  int name_len = strlen(stasis_log_dir_name);
  char * first = malloc(name_len + stasis_log_file_pool_lsn_chars + 1);
  strcpy(first, stasis_log_dir_name);
  sprintf(first+name_len, "%020lld", start_lsn);
  printf("Name is %s\n", first);
  char * full_name = malloc(strlen(fp->dirname) + 1 + strlen(first) + 1);
  full_name[0] = 0;
  strcat(full_name, fp->dirname);
  strcat(full_name, "/");
  strcat(full_name, first);
  free(first);
  return full_name;
}

// Same API as pread(), except it never performs a short read.
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

lsn_t stasis_log_file_pool_sizeof_internal_entry(stasis_log_t * log, const LogEntry *e) {
  abort();
}

void stasis_log_file_pool_chunk_open(stasis_log_file_pool_state * fp, int chunk) {
  char* full_name = malloc(strlen(fp->dirname) + 1 + strlen(fp->live_filenames[chunk]) + 1);
  full_name[0] = 0;
  strcat(full_name, fp->dirname);
  strcat(full_name, "/");
  strcat(full_name, fp->live_filenames[chunk]);

  fp->ro_fd[chunk] = open(full_name, fp->filemode, fp->fileperm);
}
int stasis_log_file_pool_append_chunk(stasis_log_t * log, off_t new_offset) {
  stasis_log_file_pool_state * fp = log->impl;

  char * old_file = 0;
  if(fp->dead_count) {
    old_file = fp->dead_filenames[fp->dead_count-1];
    fp->dead_count--;
  }

  char * new_name = stasis_log_file_pool_build_filename(fp, new_offset);
  fp->live_filenames = realloc(fp->live_filenames, sizeof(char*) * (fp->live_count+1));
  fp->live_offsets   = realloc(fp->live_offsets,   sizeof(lsn_t) * (fp->live_count+1));
  fp->ro_fd          = realloc(fp->ro_fd,          sizeof(int)   * (fp->live_count+1));
  fp->live_filenames[fp->live_count] = new_name;
  fp->live_offsets[fp->live_count] = new_offset;
  if(old_file) {
    int err = rename(old_file, new_name);
    if(err) {
      assert(err == -1);
      perror("Could not rename old log file.");
      abort();
    }
  } else {
    fp->ro_fd[fp->live_count] = open(new_name, fp->filemode, fp->fileperm);
  }
  fp->live_count++;
  return fp->live_count-1;
}
static int get_chunk_from_offset(stasis_log_t * log, lsn_t lsn) {
  stasis_log_file_pool_state * fp = log->impl;
  int chunk = -1;
  if(fp->live_offsets[fp->live_count-1] <= lsn && fp->live_offsets[fp->live_count-1] + fp->target_chunk_size > lsn) {
    return fp->live_count - 1;
  }
  for(int i = 0; i < fp->live_count; i++) {
    if(fp->live_offsets[i] > lsn) { chunk = i - 1; break; }
  }
  return chunk;
}

LogEntry * stasis_log_file_pool_reserve_entry(stasis_log_t * log, size_t szs) {
  uint32_t sz = szs;
  stasis_log_file_pool_state * fp = log->impl;
  lsn_t * handle = pthread_getspecific(fp->handle_key);
  if(!handle) { handle = malloc(sizeof(lsn_t)); pthread_setspecific(fp->handle_key, handle); }

  uint64_t framed_size = sz+sizeof(uint32_t)+sizeof(uint32_t);
  lsn_t off  = stasis_ringbuffer_reserve_space(fp->ring, framed_size, handle);
  byte * buf = stasis_ringbuffer_get_wr_buf(fp->ring, off, framed_size);

  memcpy(buf,            &sz, sizeof(uint32_t));
  LogEntry * e = (LogEntry*)(buf + (2 * sizeof(uint32_t)));
  static lsn_t last_off = 0;
  assert(off > last_off);
  last_off = off;
  e->LSN = off;

  if(off + (sz+2*sizeof(uint32_t)) > fp->live_offsets[get_chunk_from_offset(log, off)] + fp->target_chunk_size) {
    stasis_log_file_pool_append_chunk(log, off);
  }
  return e;
}

int stasis_log_file_pool_write_entry_done(stasis_log_t * log, LogEntry * e) {
  stasis_log_file_pool_state * fp = log->impl;
  lsn_t * handle = pthread_getspecific(fp->handle_key);

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
int stasis_log_file_pool_write_entry(stasis_log_t * log, LogEntry * e) {
  lsn_t sz = sizeofLogEntry(log, e);
  LogEntry * buf = stasis_log_file_pool_reserve_entry(log, sz);
  e->LSN = buf->LSN;
  memcpy(buf, e,   sz);
  stasis_log_file_pool_write_entry_done(log, buf);
  return 0;
}
const LogEntry* stasis_log_file_pool_chunk_read_entry(int fd, lsn_t file_offset, lsn_t lsn, uint32_t * len) {
  int err;
  if(sizeof(*len) != (err = mypread(fd, (byte*)len, sizeof(*len), lsn-file_offset))) {
    if(err == 0) { fprintf(stderr, "EOF reading len from log\n"); return 0; }
    abort();
  }
  if(*len == 0) { fprintf(stderr, "Reached end of log\n"); return 0; }
  byte * buf = malloc(*len + sizeof(uint32_t));
  if(!buf) {
    fprintf(stderr, "Couldn't alloc memory for log entry of size %lld.  "
           "This could be due to corruption at the end of the log.  Conservatively bailing out.",
           (long long)*len);
    abort();
  }

  if((*len)+sizeof(uint32_t) != (err = mypread(fd, (byte*)buf, (*len) + sizeof(uint32_t), sizeof(*len)+ lsn - file_offset))) {
    if(err == 0) { fprintf(stderr, "EOF reading payload from log\n"); abort(); return 0; }
    abort();
  }
  uint32_t logged_crc = *(uint32_t*)(buf);
  uint32_t calc_crc = (uint32_t)stasis_crc32(buf+sizeof(uint32_t), *len, (uint32_t)-1);
  if(logged_crc != calc_crc) {
    // crc does not match
    fprintf(stderr, "CRC mismatch reading from log.  LSN %lld Got %d, Expected %d", lsn, calc_crc, logged_crc);
    abort();
    free(buf);
    return 0;
  } else {
    return (const LogEntry*)(buf+sizeof(uint32_t));
  }
}
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
  // XXX if in current segment, need to force log before read, or perhaps look in buffer.
  stasis_log_file_pool_state * fp = log->impl;
  int chunk = get_chunk_from_offset(log, lsn);
  if(chunk == -1) { return NULL; }
  uint32_t len;
  const LogEntry * e = stasis_log_file_pool_chunk_read_entry(fp->ro_fd[chunk], fp->live_offsets[chunk], lsn, &len);
  if(e) { assert(sizeofLogEntry(log, e) == len); }
  return e;
}
void stasis_log_file_pool_read_entry_done(struct stasis_log_t *log, const LogEntry *e) {
  free((void*)((byte*)e-sizeof(uint32_t)));
}
lsn_t stasis_log_file_pool_next_entry(struct stasis_log_t* log, const LogEntry * e) {
  return e->LSN + sizeofLogEntry(log, e) + sizeof(uint32_t) + sizeof(uint32_t);
}

lsn_t stasis_log_file_pool_next_available_lsn(stasis_log_t *log) {
  stasis_log_file_pool_state * fp = log->impl;
  /// XXX latching
  return stasis_ringbuffer_current_write_tail(fp->ring);//nextAvailableLSN;
}
lsn_t stasis_log_file_pool_chunk_scrub_to_eof(stasis_log_t * log, int fd, lsn_t file_off) {
  lsn_t cur_off = file_off;
  const LogEntry * e;
  uint32_t len;
  while((e = stasis_log_file_pool_chunk_read_entry(fd, file_off, cur_off, &len))) {
    cur_off = log->next_entry(log, e);
    log->read_entry_done(log, e);
  }
  return cur_off;
}

int stasis_log_file_pool_close(stasis_log_t * log) {
  stasis_log_file_pool_state * fp = log->impl;

  stasis_ringbuffer_shutdown(fp->ring);

  pthread_join(fp->write_thread, 0);

  // XXX need to force log to disk here.
  for(int i = 0; i < fp->live_count; i++) {
    if(fp->ro_fd[i] != -1) {
      close(fp->ro_fd[i]);
    }
    free(fp->live_filenames[i]);
  }
  for(int i = 0; i < fp->dead_count; i++) {
    free(fp->dead_filenames[i]);
  }
  free(fp->ro_fd);
  free(fp->live_filenames);
  free(fp->dead_filenames);
  free(fp);
  free(log);
  return 0;
}

void * stasis_log_file_pool_writeback_worker(void * arg) {
  stasis_log_t * log = arg;
  stasis_log_file_pool_state * fp = log->impl;

  lsn_t handle, off, next_chunk_off, chunk_len, remaining_len;
  while(1) {
    lsn_t len = 4*1024*1024;
    off = stasis_ringbuffer_consume_bytes(fp->ring, &len, &handle);
    if(off == RING_CLOSED) break;
    int chunk = get_chunk_from_offset(log, off);
    assert(chunk != -1); // chunks are created on insertion into the ring buffer...
    if(fp->live_count > chunk+1) {
      next_chunk_off = fp->live_offsets[chunk+1];
      chunk_len = next_chunk_off - off;
      if(chunk_len > len) {
        chunk_len = len;
        remaining_len = 0;
      } else {
        remaining_len = len - chunk_len;
      }
    } else {
      chunk_len = len;
      remaining_len = 0;
    }
    const byte * buf = stasis_ringbuffer_get_rd_buf(fp->ring, off, len);
    int succ = stasis_log_file_pool_chunk_write_buffer(fp->ro_fd[chunk], buf, chunk_len, fp->live_offsets[chunk], off);
    if(!succ) {
      fprintf(stderr, "A: chunk is %d cnk offset is %lld offset =s %lld cnk name is %s\n", chunk, fp->live_offsets[chunk], off, fp->live_filenames[chunk]);
      assert(succ);
    }
    if(remaining_len) {
      uint32_t zero = 0;
      // Close current log file.  This might increase its length
      // by a byte or so, but that's the filesystem's problem.
      succ = stasis_log_file_pool_chunk_write_buffer(
          fp->ro_fd[chunk],
          (const byte*)&zero,
          sizeof(zero),
          fp->live_offsets[chunk],
          off+chunk_len);
      if(!succ) {
        fprintf(stderr, "B: chunk is %d cnk offset is %lld offset =s %lld cnk name is %s\n", chunk, fp->live_offsets[chunk], off, fp->live_filenames[chunk]);
        assert(succ);
      }
      succ = stasis_log_file_pool_chunk_write_buffer(
          fp->ro_fd[chunk+1],
          buf + chunk_len,
          remaining_len,
          fp->live_offsets[chunk+1],
          off + chunk_len);
      if(!succ) {
        fprintf(stderr, "C: chunk is %d cnk offset is %lld offset =s %lld cnk name is %s\n", chunk, fp->live_offsets[chunk], off, fp->live_filenames[chunk]);
        assert(succ);
      }
    }
    stasis_ringbuffer_read_done(fp->ring, &off);
  }
  return 0;
}

void key_destr(void * key) { free(key); }

int filesort(const struct dirent ** a, const struct dirent ** b) {
  int ret = strcmp((*a)->d_name, (*b)->d_name);
  DEBUG("%d = %s <=> %s\n", ret, (*a)->d_name, (*b)->d_name);
  return ret;
}

stasis_log_t* stasis_log_file_pool_open(const char* dirname, int filemode, int fileperm) {
  struct dirent **namelist;
  stasis_log_file_pool_state* fp = malloc(sizeof(*fp));
  stasis_log_t * ret = malloc(sizeof(*ret));

  static const stasis_log_t proto = {
    0,//stasis_log_file_pool_set_truncation,
    stasis_log_file_pool_sizeof_internal_entry,
    stasis_log_file_pool_write_entry,
    stasis_log_file_pool_reserve_entry,
    stasis_log_file_pool_write_entry_done,
    stasis_log_file_pool_read_entry,
    stasis_log_file_pool_read_entry_done,
    stasis_log_file_pool_next_entry,
    0,//stasis_log_file_pool_first_unstable_lsn,
    0,//stasis_log_file_pool_first_pending_lsn,
    stasis_log_file_pool_next_available_lsn,
    0,//stasis_log_file_pool_force_tail,
    0,//stasis_log_file_pool_truncate,
    0,//stasis_log_file_pool_truncation_point,
    stasis_log_file_pool_close,
    0,//stasis_log_file_pool_is_durable,
  };
  memcpy(ret, &proto, sizeof(proto));
  ret->impl = fp;

  fp->dirname = strdup(dirname);

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
    char * full_name = stasis_log_file_pool_build_filename(fp, 1);
    int fd = creat(full_name, stasis_log_file_permissions);
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

  fp->target_chunk_size = 16 * 1024 * 1024;

  fp->filemode = filemode;
  fp->fileperm = fileperm;
  fp->softcommit = !(filemode & O_SYNC);

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

      assert(lsn <= current_target || !current_target);
      char * full_name = malloc(strlen(fp->live_filenames[fp->live_count]) + 1 + strlen(fp->dirname) + 1);
      full_name[0] = 0;
      strcat(full_name, fp->dirname);
      strcat(full_name, "/");
      strcat(full_name, fp->live_filenames[fp->live_count]);
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

      fp->dead_filenames = realloc(fp->dead_filenames, fp->dead_count+1);
      fp->dead_filenames[fp->dead_count] = strdup(namelist[i]->d_name);
      (fp->dead_count)++;

    } break;
    }

    free(namelist[i]);

    //fp->nextAvailableLSN = yyy();

    //fp->fp = xxx(); // need to scan last log segment for valid entries,
                    // if fail, position at eof on second to last.
                    // if no log segments, create new + open

                    // XXX check each log segment's CRCs at startup?

    pthread_mutex_init(&fp->write_mutex,0);
    pthread_mutex_init(&fp->read_mutex,0);
    fp->state_latch = initlock();

//    fp->buffer = calloc(stasis_log_file_write_buffer_size, sizeof(char));
//    setbuffer(fp->fp, fp->buffer, stasis_log_file_write_buffer_size);


  }

  free(namelist);

  printf("Current log segment appears to be %s.  Scanning for next available LSN\n", fp->live_filenames[fp->live_count-1]);

  lsn_t next_lsn = stasis_log_file_pool_chunk_scrub_to_eof(ret, fp->ro_fd[fp->live_count-1], fp->live_offsets[fp->live_count-1]);

  printf("Scan returned %lld\n", (long long)next_lsn);

  // The previous segment must have been forced to disk before we created the current one, so we're good to go.

  fp->ring = stasis_ringbuffer_init(24, next_lsn); // 16mb buffer
  pthread_key_create(&fp->handle_key, key_destr);
  fp->flushedLSN_wal = next_lsn;
  fp->flushedLSN_commit = next_lsn;
  fp->flushedLSN_internal = next_lsn;

  pthread_create(&fp->write_thread, 0, stasis_log_file_pool_writeback_worker, ret);

  return ret;
}

void stasis_log_file_pool_delete(const char* dirname) {

}
