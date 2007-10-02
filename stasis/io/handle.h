#include <stasis/transactional.h>

#ifndef IO_HANDLE_H
#define IO_HANDLE_H


/**
   stasis_handle() is a macro that prepends a unique prefix to the its
   argument's function name.  It's used to cope with namespace
   collisions

   @todo Do away with macros like this one.
*/
#define stasis_handle(x) stasis_handle_##x

/**

   @file 

   Interface for I/O handle implementations.

   This interface is designed to provide some extra features needed by
   the buffer manager and the log, and to hide the operating system's
   I/O interface from the rest of Stasis.

   Handles are composable, and some features, such as log truncation,
   non-blocking writes are not implemented by all file handle
   implementations, and are instead supported by wrapping a file
   handle that performs raw I/O with one that adds extra
   functionality.

   This file describes the minimum concurrency guarantees provided by
   handle implementations.  See the handle implementations'
   documentation for more information about concurrency.

   Each handle defines two sets of methods that read, write and append
   to the file.  The first set (read(), write() and append()) take a
   buffer that is allocated by the caller.  The second set
   (read_buffer(), write_buffer() and append_buffer()) use memory that
   is managed by the handle.  Callers must explicitly release these
   buffers by calling release_read_buffer() or release_write_buffer().

   Finally, handles support truncation from the <i>beginning</i> of
   the file, which is needed by the log manager.  The off parameters
   passed into functions are relative to the original start of the
   file.  Negative file offsets are reserved for
   implementation-specific purposes.



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

   EDOM   off is less than the beginning of the file (possibly due to
          truncation).

   EBADF  an unrecoverable error occurred; the handle is no longer vaild.  The
          error that caused this one is stored in the handle's error field.

   Handle implementations may return return other errors as appropriate.


   @todo rename *_buffer() functions to get_*_buffer()

 */

/**
   This struct contains the function pointers that define handle
   implementations.  Implementations of the handle interface should
   instantiate this struct, and set each function pointer accordingly.
   The contents of the "impl" pointer is implementation defined.
*/
typedef struct stasis_handle_t {
  /** Some handle implementations maintain their own internal buffers,
      and must use memcpy in order to read or write from their
      caller's buffers.  The num_copies* functions provide perfomance
      hints to the caller so that the more efficient set of methods
      can be used.

      @return the number of in-memory copies made when the caller
      provides the buffer, or some other proxy for performance (higher
      values are slower)

  */
  int (*num_copies)(struct stasis_handle_t * h);

  /** @see num_copies() */
  int (*num_copies_buffer)(struct stasis_handle_t * h);

  /** Close this handle, and release any associated resources. */
  int (*close)(struct stasis_handle_t *);

  /** The offset of the handle's first byte */
  lsn_t (*start_position)(struct stasis_handle_t * h);

  /** The offset of the byte after the end of the handle's data. */
  lsn_t (*end_position)(struct stasis_handle_t * h);

  /** Obtain a write buffer.

      The behavior of calls that attempt to access this region before
      release_write_buffer() returns is undefined.

      @param h   The handle
      @param off The offset of the first byte in the write buffer.
      @param len The length, in bytes, of the write buffer.
  */
  struct stasis_write_buffer_t * (*write_buffer)(struct stasis_handle_t * h,
					  lsn_t off, lsn_t len);
  /**
     Increase the size of the file, and obtain a write buffer at the
     beginning of the new space.

     The behavior of calls that attempt to access this region before
     release_write_buffer() returns is undefined.  Calls to append
     that are made before the buffer is released are legal, and will
     append data starting at the new end of file.

     @param h   The handle
     @param len The length, in bytes, of the write buffer.
  */
  struct stasis_write_buffer_t * (*append_buffer)(struct stasis_handle_t * h,
					   lsn_t len);
  /**
     Release a write buffer and associated resources.
  */
  int (*release_write_buffer)(struct stasis_write_buffer_t * w);
  /**
     Read a region of the file.  Attempts to modify the region that is
     being read will have undefined behavior until release_read_buffer
     returns.

     The behavior of calls that attempt to write to this region before
     release_read_buffer() returns is undefined.

     @param h   The handle
     @param off The offset of the first byte in the read buffer.
     @param len The length, in bytes, of the read buffer.

  */
  struct stasis_read_buffer_t * (*read_buffer)(struct stasis_handle_t * h,
					lsn_t offset, lsn_t length);
  /**
     Release a read buffer and associated resources.
  */
  int (*release_read_buffer)(struct stasis_read_buffer_t * r);
  /**
     Write data to the handle from memory managed by the caller.  Once
     write returns, the handle will reflect the update.

     @param h The handle
     @param off The position of the first byte to be written
     @param dat A buffer containin the data to be written
     @param len The number of bytes to be written
  */
  int (*write)(struct stasis_handle_t * h, lsn_t off,
	       const byte * dat, lsn_t len);
  /**
     Append data to the end of the file.  Once append returns, future
     calls to the handle will reflect the update.

     @param h The handle
     @param off The position of the first byte to be written
     @param dat A buffer containin the data to be written
     @param len The number of bytes to be written
  */
  int (*append)(struct stasis_handle_t * h, lsn_t * off, const byte * dat,
                lsn_t len);
  /**
     Read data from the file.  The region may be safely written to
     once read returns.

     @param h The handle
     @param off The position of the first byte to be written
     @param dat A buffer containin the data to be written
     @param len The number of bytes to be written
  */
  int (*read)(struct stasis_handle_t * h,
	      lsn_t off, byte * buf, lsn_t len);
  /**
     Truncate bytes from the beginning of the file.  This is needed by
     the log manager.

     @param h The handle

     @param new_start The offest of the first byte in the handle that
            must be preserved.  Bytes before this point may or may not
            be retained after this function returns.
  */
  int (*truncate_start)(struct stasis_handle_t * h, lsn_t new_start);
  /**
     The handle's error flag; this passes errors to the caller when
     they can't be returned directly.
  */
  int error;
  /**
     Reserved for implementation specific data.
  */
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
  lsn_t off;
  const byte * buf;
  lsn_t len;
  void * impl;
  int error;
} stasis_read_buffer_t;

/**
   Open a handle that is backed by RAM

   @param start_offset The logical offset of the first byte in the handle
*/
stasis_handle_t * stasis_handle(open_memory)(lsn_t start_offset);
/**
   Open a handle that is backed by a file.  This handle uses the unix
   read(),write() I/O interfaces.  Due to limitations in read() and
   write(), it must hold a mutex during system calls, and therefore
   cannot perform concurrent I/O.

   Attempts to read or write to a region that is already being written
   to have undefined behavior, but non-overlapping regions can be
   concurrently accessed.

   @param start_offset The logical offset of the first byte in the handle
   @param path The name of the file to be opened.
   @param mode Flags to be passed to open()
*/
stasis_handle_t * stasis_handle(open_file)
    (lsn_t start_offset, char * path, int flags, int mode);
/**
   Open a handle that is backed by a file.  This handle uses pread()
   and pwrite().  It never holds a mutex while perfoming I/O.

   Attempts to read or write to a region that is already being written
   to have undefined behavior, but non-overlapping regions can be
   concurrently accessed.

   @param start_offset The logical offset of the first byte in the handle
   @param path The name of the file to be opened.
   @param mode Flags to be passed to open().
*/
stasis_handle_t * stasis_handle(open_pfile)
    (lsn_t start_offset, char * path, int flags, int mode);
/**
   Given a factory for creating "fast" and "slow" handles, provide a
   handle that never makes callers wait for write requests to
   complete.  ("Never" is a strong word; callers will begin to block
   if the supply of write buffers is depleted.)

   Attempts to read or write to a region that is already being written
   to are undefined, but non-overlapping regions can be concurrently
   accessed.

   @param slow_factory A callback function that returns a handle with
                       offset zero.  These handles will be accessed
                       concurrently, but do not need to support
                       concurrent writes, or reads from regions that
                       are being written to.  For performance reasons,
                       handles that cannot exploit concurrency should
                       probably be allocated from a pool (@see
                       open_file), while a single truely concurrent
                       handle (@see open_pfile) should suffice.

   @param slow_factory_arg A pointer to data that will be passed into
                           slow_factory.

   @param fast_factory A callback function that returns a handle with
                       a given offest and length.  The handle need not
                       support persistant storage, and is used as
                       write buffer space.  Typically, fast handles
                       will be allocated out of a pool.

   @param fast_factory_arg A pointer to data that will be passed into
                           fast_factory.

   @param worker_thread_count This many workers will be spawned in
                              order to service this handle

   @param buffers_size The maximum number of outstanding bytes to
                       buffer before blocking.

   @param max_writes The maximum number of outstanding writes to allow
                     before blocking.
*/
stasis_handle_t * stasis_handle(open_non_blocking)
    (stasis_handle_t * (*slow_factory)(void * arg), void * slow_factory_arg,
     stasis_handle_t * (*fast_factory)(lsn_t off, lsn_t len, void * arg),
     void * fast_factory_arg, int worker_thread_count, lsn_t buffer_size,
     int max_writes);
/**
   @todo implement open_verifying in handle.h
*/
stasis_handle_t * stasis_handle(open_verifying)(stasis_handle_t * h);
/**
   Writes each action performed (and return values) to standard out.

   @param h All handle operations will be forwarded to h.
*/
stasis_handle_t * stasis_handle(open_debug)(stasis_handle_t * h);

#endif
