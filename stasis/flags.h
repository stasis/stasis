#ifndef _STASIS_FLAGS_H__
#define _STASIS_FLAGS_H__
#include <stasis/bufferManager.h>
#include <stasis/logger/logger2.h>
#include <stasis/dirtyPageTable.h>
#include <stasis/io/handle.h>
#include <stasis/pageHandle.h>

BEGIN_C_DECLS

/**
    This is the type of log that is being used.

 */
extern stasis_log_t* (*stasis_log_factory)(void);
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
 * The number of pages that must be dirty for the writeback thread to
 * initiate writeback.
 */
extern pageid_t stasis_dirty_page_count_soft_limit;
/**
 * If there are fewer than this many dirty pages, then the writeback thread
 * will go to sleep.
 */
extern pageid_t stasis_dirty_page_low_water_mark;
/**
 * The number of pages that must be dirty for application threads to block on
 * (or initiate) writeback.  Since this number causes backpressure on the
 * threads that are dirtying pages, dirty pages will never occupy more than
 * this number of pages.
 */
extern pageid_t stasis_dirty_page_count_hard_limit;
/**
 * The number of pages that should be written back to Linux's file cache
 * at a time.  We do not force after each quantum, but instead may hint to
 * Linux that it should treat the set as a group.  Also, any latchesh held
 * by writeback are released at least this often.
 */
extern pageid_t stasis_dirty_page_table_flush_quantum;

/**
   If this is true, then the only thread that will perform writeback is the
   buffer manager writeback thread.  It turns out that splitting sequential
   writes across many threads leads to a 90-95% reduction in write throughput.

   Otherwise (the default) application threads will help write back dirty pages
   so that we can get good concurrency on our writes.
 */
extern int stasis_buffer_manager_hint_writes_are_sequential;
/**
   If this is true, then disable some optimizations associated with sequential
   write mode.  This will needlessly burn CPU by inserting dirty pages into
   the LRU.  In sequential write mode, these dirty pages will cause populateTLS
   to loop excessively, excercising all sorts of extremely rare thread
   synchronization schedules.
 */
extern int stasis_buffer_manager_debug_stress_latching;
/**
   This determines which page handle infrastructure buffer manager will use.
   Page handles sit between the buffer manager and io handles.  Their main
   purpose is to observe the buffer manager's I/O operations, and coordinate
   with the dirty page table, log, and per-page state machines as necessary.

   It defaults to stasis_page_handle_default_factory.  It can be overridden
   at compile time by defining STASIS_PAGE_HANDLE_FACTORY.
*/
extern stasis_page_handle_t* (*stasis_page_handle_factory)(stasis_log_t*, stasis_dirty_page_table_t*);
/**
   This determines which Stasis handle factory will be called by the default
   page_handle_factory.  It defaults to a method that simply calls a standard
   open method exported by the file-based handles, and passes in sane defaults
   for file permissions, file names and so on.

   @see stasis_handle_file_factory to change which underlying file API will
        back the handle.
 */
extern stasis_handle_t* (*stasis_handle_factory)();
/**
   This factory is invoked by the default stasis_handle_factory, and takes
   additional file system parameters as arguments.

   Valid options: stasis_handle_open_file(), stasis_handle_open_pfile(), and stasis_handle_non_blocking_factory.
 */
extern stasis_handle_t* (*stasis_handle_file_factory)(const char* filename, int open_mode, int creat_perms);
/**
 * The default stripe size for Stasis' user space raid0 implementation.
 *
 * This must be a multiple of PAGE_SIZE.
 */
extern uint32_t stasis_handle_raid0_stripe_size;
extern char ** stasis_handle_raid0_filenames;
/**
   The factory that non_blocking handles will use for slow handles.  (Only
   used if stasis_buffer_manager_io_handle_default_factory is set to
   stasis_non_blocking_factory.)

*/
extern stasis_handle_t* (*stasis_non_blocking_handle_file_factory)(const char* filename, int open_mode, int creat_perms);

/**
   If true, the buffer manager will use O_DIRECT, O_SYNC and so on (Mandatory
   flags such as O_RDWR will be set regardless).  Set at compile time by
   defining STASIS_BUFFER_MANAGER_IO_HANDLE_FLAGS.
*/
extern int stasis_buffer_manager_io_handle_flags;
/**
   How should stasis grow the page file?  Valid options are:

   * STASIS_BUFFER_MANAGER_PREALLOCATE_DISABLED, which avoids any explicit preallocation.  (This can cause terrible fragmentation on filesystems that support large files.), 
   * STASIS_BUFFER_MANAGER_PREALLCOATE_LEGACY, which placed dirty zero filled pages in the buffer cache.  (This can cause double writes if the extended region does not fit in RAM, and unnecessarily evicts stuff.)
   * STASIS_BUFFER_MANAGER_PREALLOCATE_DEFAULT, the recommended mode, which currently calls posix_fallocate().  This is not supported by old versions of Linux, so we attempt to fallback on the legacy mode at compile time.
*/
extern int stasis_buffer_manager_preallocate_mode;

/**
   The default replacement policy.

   Valid values are STASIS_REPLACEMENT_POLICY_THREADSAFE_LRU,
   STASIS_REPLACEMENT_POLICY_CONCURRENT_LRU and STASIS_REPLACEMENT_POLICY_CLOCK
 */
extern int stasis_replacement_policy;
/**
   If true, then concurrent LRU will use exponential backoff when it
   has trouble finding a page to evict.  If false, it will perform a
   busy wait.

   This definitely should be set to true when
   stasis_buffer_manager_hint_sequential_writes is true.  Otherwise
   the only way that page writeback will be able to apply backpressure
   to application threads will be to cause busy waits in concurrent
   LRU.
 */
extern int stasis_replacement_policy_concurrent_wrapper_exponential_backoff;
/**
   If true, then concurrent LRU will round the number of buckets up to
   the next power of two, and use bit masks instead of mod when
   assigning pages to buckets.
 */
extern int stasis_replacement_policy_concurrent_wrapper_power_of_two_buckets;
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
extern int    stasis_log_dir_permissions;
extern int    stasis_log_softcommit;

extern char * stasis_store_file_name;
extern char * stasis_store_file_1_name;
extern char * stasis_store_file_2_name;


/**
 * Number of prefetch threads to create at startup.  Zero disables the threads,
 * which currently causes the pages to be read synchronously.
 */
extern int stasis_buffer_manager_hash_prefetch_count;

extern const char * stasis_log_dir_name;
extern const char * stasis_log_chunk_name;
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
extern const int    stasis_log_file_pool_lsn_chars;
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

END_C_DECLS

#endif
