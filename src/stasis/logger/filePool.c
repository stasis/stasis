#define _SVID_SOURCE
#define _BSD_SOURCE
#include <dirent.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <pthread.h>

#include <stasis/common.h>
#include <stasis/latches.h>
#include <stasis/logger/filePool.h>
/**
   @see stasis_log_safe_writes_state for more documentation;
        identically named fields serve analagous purposes.

   Latch order: write_mutex, read_mutex, state_latch
 */
typedef struct {
  const char * dirname;

  int live_count;
  int dead_count;

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
     An array of read-only file descriptors.  If an entry is zero,
     then the file is not open.  Offsets match those of
     live_filenames.
   */
  int * ro_fd;

  lsn_t nextAvailableLSN;

  /**
     A file handle positioned at the current end of log.
   */
  FILE * fp;

  int filemode;
  int fileperm;
  char softcommit;
  /**
     These are always the lsn of the first entry that might not be stable.
   */
  lsn_t flushedLSN_wal;
  lsn_t flushedLSN_commit;
  lsn_t flushedLSN_internal;

  pthread_mutex_t write_mutex;
  pthread_mutex_t read_mutex;
  /**
     Held whenever manipulating state in this struct (with the
     execption of the file handles, which are protected by read and
     write mutex).
   */
  rwl* state_latch;

  char * buffer;

  unsigned int crc;
} stasis_log_file_pool_state;

enum file_type {
  UNKNOWN = 0,
  LIVE,
  DEAD
};

enum file_type stasis_log_file_pool_file_type(const struct dirent* file, lsn_t *lsn) {
  const char* name = file->d_name;

  if(DT_REG != file->d_type && DT_LNK != file->d_type) {
    return UNKNOWN;
  }
  off_t base_len = strlen(stasis_log_dir_name);
  if(strncmp(name, stasis_log_dir_name, base_len)) {
    return UNKNOWN;
  }
  name+=base_len;
  char * nameend;
  *lsn = strtoull(name,&nameend,10);
  if(nameend-name == stasis_log_dir_name_lsn_chars) {
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
    printf("Unknown file in log dir: %s\n", file->d_name);
    return 0;
  }
}

stasis_log_t* stasis_log_file_pool_open(const char* dirname, int filemode, int fileperm) {
  struct dirent **namelist;
  stasis_log_file_pool_state* fp = malloc(sizeof(*fp));

  struct stat st;
  while(stat(dirname, &st)) {
    if(errno == ENOENT) {
      if(mkdir(dirname, filemode | 0111)) {
        perror("Error creating stasis log directory");
        return 0;
      }
    } else {
      perror("Couldn't stat stasis log directory");
      return 0;
    }
  }
  if(!S_ISDIR(st.st_mode)) {
    printf("Stasis log directory %s exists and is not a directory!\n", dirname);
    return 0;
  }

  int n = scandir(dirname, &namelist, stasis_log_file_pool_file_filter, alphasort);

  if(n < 0) {
    perror("couldn't scan log directory");
    free(fp);
    return 0;
  }

  fp->live_filenames = 0;
  fp->live_offsets = 0;
  fp->dead_filenames = 0;
  fp->live_count = 0;
  fp->dead_count = 0;
  off_t current_target = 0;
  for(int i = 0; i < n; i++) {
    lsn_t lsn;

    switch(stasis_log_file_pool_file_type(namelist[i],&lsn)) {
    case UNKNOWN: {

      abort(); // bug in scandir?!?  Should have been filtered out...

    } break;
    case LIVE: {

      fp->live_filenames = realloc(fp->live_filenames,
                                   (fp->live_count+1) * sizeof(char));
      fp->live_offsets   = realloc(fp->live_offsets,
                                   (fp->live_count+1) * sizeof(*fp->live_offsets));
      fp->ro_fd          = realloc(fp->ro_fd,
                                   (fp->live_count+1) * sizeof(*fp->ro_fd));

      fp->live_filenames[fp->live_count] = strdup(namelist[i]->d_name);
      fp->live_offsets[fp->live_count]   = lsn;
      fp->ro_fd[fp->live_count]          = 0;

      assert(lsn == current_target || !current_target);
      (fp->live_count)++;
      if(stat(fp->live_filenames[fp->live_count], &st)) {
        current_target = st.st_size + fp->live_offsets[fp->live_count];
      }

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
                    // if fail, positition at eof on second to last.
                    // if no log segments, create new + open

                    // XXX check each log segment's CRCs at startup?

    fp->filemode = filemode;
    fp->fileperm = fileperm;
    fp->softcommit = !(filemode & O_SYNC);

    fp->flushedLSN_wal = fp->nextAvailableLSN;
    fp->flushedLSN_commit = fp->nextAvailableLSN;
    fp->flushedLSN_internal = fp->nextAvailableLSN;

    pthread_mutex_init(&fp->write_mutex,0);
    pthread_mutex_init(&fp->read_mutex,0);
    fp->state_latch = initlock();

    fp->buffer = calloc(stasis_log_file_write_buffer_size, sizeof(char));
    setbuffer(fp->fp, fp->buffer, stasis_log_file_write_buffer_size);


  }
  free(namelist);
}

void stasis_log_file_pool_delete(const char* dirname) {

}
