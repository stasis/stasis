#ifndef _STASIS_FLAGS_H__
#define _STASIS_FLAGS_H__
#include <stasis/bufferManager.h>
#include <stasis/logger/logger2.h>
#include <stasis/dirtyPageTable.h>
/**
    This is the type of buffer manager that is being used.

    Before Stasis is intialized it will be set to a default value.
    It may be changed before Tinit() is called, or overridden at
    compile time by defining USE_BUFFER_MANAGER.

    (eg: gcc ... -DUSE_BUFFER_MANAGER=BUFFER_MANAGER_FOO)

    @see constants.h for a list of recognized buffer manager implementations.
         (The constants are named BUFFER_MANAGER_*)

 */
extern stasis_buffer_manager_t* (*stasis_buffer_manager_factory)(stasis_log_t*, stasis_dirty_page_table_t*);

extern pageid_t stasis_buffer_manager_size;
/**
   This determines which type of file handle the buffer manager will use.

   It defaults to BUFFER_MANAGER_FILE_HANDLE_NON_BLOCKING for a
   non-blocking handle.  It can be overridden at compile time by defining
   BUFFER_MANAGER_FILE_HANDLE_TYPE.

   @see constants.h for potential values.  (The constants are named
   BUFFER_MANAGER_FILE_HANDLE_*)
*/
extern int bufferManagerFileHandleType;
/**
   Determines which type of slow handle non_blocking will use for the
   buffer manager.  Override at compile time by defining
   BUFFER_MANAGER_NON_BLOCKING_SLOW_TYPE.

   @see constants.h: Constants named IO_HANDLE_* are potential values.
*/
extern int bufferManagerNonBlockingSlowHandleType;
/**
   If true, the buffer manager will use O_DIRECT.  Set at compile time by
   defining BUFFER_MANAGER_O_DIRECT.
*/
extern int bufferManagerO_DIRECT;
/**
   If true, Stasis will suppress warnings regarding unclean shutdowns.
   This is use to prevent spurious warnings during unit testing, and
   must be set after Tinit() is called.  (Tinit() resets this value to
   false).

   (This should not be set by applications, or during compilation.)
 */
extern int stasis_suppress_unclean_shutdown_warnings;

/*
   Truncation options
*/
/**
   If true, Stasis will spawn a background thread that periodically
   truncates the log.
 */
extern int stasis_truncation_automatic;

/**
    This is the log implementation that is being used.

    Before Stasis is initialized it will be set to a default value.
    It may be changed before Tinit() is called by assigning to it.
    The default can be overridden at compile time by defining
    USE_LOGGER.

    (eg: gcc ... -DSTASIS_LOG_TYPE=LOG_TO_FOO)

    @see constants.h for a list of recognized log implementations.
         (The constants are named LOG_TO_*)
	@todo rename LOG_TO_* constants to STASIS_LOG_TYPE_*

*/
extern int stasis_log_type;

extern size_t stasis_log_in_memory_max_entries;

extern char * stasis_log_file_name;
extern int    stasis_log_file_mode;
extern int    stasis_log_file_permissions;
extern int    stasis_log_softcommit;

extern char * stasis_store_file_name;

/**
 * Number of prefetch threads to create at startup.  Zero disables the threads,
 * which currently causes the pages to be read synchronously.
 */
extern int stasis_buffer_manager_hash_prefetch_count;

extern const char * stasis_log_dir_name;
/**
   Maximum number of log chunks that will be created by file pool.
   This number is treated as a hint.
 */
extern int   stasis_log_file_pool_chunk_count_target;
/**
   Minimum size of each completed log chunk.  This number is treated
   as a hint.
 */
extern lsn_t stasis_log_file_pool_chunk_min_size;
/**
   Number of characters in log file names devoted to storing the LSN.
 */
extern const int    stasis_log_dir_name_lsn_chars;
/**
   Number of bytes that stasis' log may buffer before writeback.
 */
extern lsn_t stasis_log_file_write_buffer_size;
/**
   Set to 1 if segment based recovery is enabled.  This disables some
   optimizations that assume all operations are page based.

   @todo Stasis' segment implementation is a work in progress; therefore this is set to zero by default.
 */
extern int stasis_segments_enabled;
#endif
