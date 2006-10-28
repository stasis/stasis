#include <lladd/transactional.h>
#define stasis_handle(x) stasis_handle_##x

/**
   Error handling:
   
   read, write, append, open, release_read_buffer and
   release_write_buffer return 0 on success, and an error code
   otherwise.  read_buffer() and write_buffer() return error codes via
   the error field of the handles they produce.

   An error that occurs while writing to the handle leaves the region
   that was being written in an undefined state.

   Errors in num_copies, num_copies_buffer, start_position, and end_position
   are always unrecoverable, and return -1.

   close returns 0 on success, or an error code otherwise.  close
   always frees the handle that was passed into it, regardless of
   whether an error occurred.

   Here are the meanings of the various error codes:
   
   EDOM   off is less than the beginning of the file (possibly due to truncation).
   EBADF  an unrecoverable error occurred; the handle is no longer vaild.  The error
          that caused this one is stored in the handle's error field.

   Handle implementations may return return other errors as appropriate.

   Offset:
   
   Negative offsets are reserved for implementation-specific purposes.

 */
typedef struct stasis_handle_t { 
  /** @return the number of in-memory copies made when the caller
      provides the buffer */
  int (*num_copies)(struct stasis_handle_t * h);
  /** @return the number of in-memory copies made when the handle
      provides the buffer */
  int (*num_copies_buffer)(struct stasis_handle_t * h);
  
  int (*close)(struct stasis_handle_t *);
  
  /** The offset of the handle's first byte */
  lsn_t (*start_position)(struct stasis_handle_t * h);
  /** The offset of the byte after the end of the handle's data. */
  lsn_t (*end_position)(struct stasis_handle_t * h);
  
  struct stasis_write_buffer_t * (*write_buffer)(struct stasis_handle_t * h, 
					  lsn_t off, lsn_t len);
  struct stasis_write_buffer_t * (*append_buffer)(struct stasis_handle_t * h, 
					   lsn_t len);
  int (*release_write_buffer)(struct stasis_write_buffer_t * w);

  struct stasis_read_buffer_t * (*read_buffer)(struct stasis_handle_t * h,
					lsn_t offset, lsn_t length);
  int (*release_read_buffer)(struct stasis_read_buffer_t * r);
  
  int (*write)(struct stasis_handle_t * h, lsn_t off, 
	       const byte * dat, lsn_t len);
  int (*append)(struct stasis_handle_t * h, lsn_t * off, const byte * dat, lsn_t len);

  int (*read)(struct stasis_handle_t * h, 
	      lsn_t offset, byte * buf, lsn_t length);
  
  int (*truncate_start)(struct stasis_handle_t * h, lsn_t new_start);

  int error;

  void * impl;

} stasis_handle_t;

typedef struct stasis_write_buffer_t {
  stasis_handle_t * h;
  lsn_t off;
  byte * buf;
  lsn_t len;
  void * impl;
  int error;
} stasis_write_buffer_t;

typedef struct stasis_read_buffer_t {
  stasis_handle_t * h;
  const byte * buf;
  lsn_t off;
  lsn_t len;
  void * impl;
  int error;
} stasis_read_buffer_t;

stasis_handle_t * stasis_handle(open_memory)(lsn_t start_offset);
stasis_handle_t * stasis_handle(open_file)(lsn_t start_offset, char * path, int flags, int mode);
stasis_handle_t * stasis_handle(open_non_blocking)(stasis_handle_t * (*slow_factory)(void * arg),
						   void * slow_factory_arg,
						   stasis_handle_t * (*fast_factory)(lsn_t off, lsn_t len, void * arg),
						   void * fast_factory_arg,
						   int worker_thread_count,
						   lsn_t buffer_size);
stasis_handle_t * stasis_handle(open_verifying)(stasis_handle_t * h);
stasis_handle_t * stasis_handle(open_debug)(stasis_handle_t * h);

