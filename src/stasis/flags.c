#include <config.h> //for O_NOATIME
#include <stasis/common.h>
#include <stasis/flags.h>
#include <stasis/constants.h>

#include <stasis/transactional.h>
#include <stasis/pageHandle.h>
#include <stasis/bufferManager/bufferHash.h>
#include <stasis/bufferManager/concurrentBufferManager.h>
#include <stasis/bufferManager/pageArray.h>
#include <stasis/bufferManager/legacy/legacyBufferManager.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#ifdef STASIS_LOG_FACTORY
stasis_log_t * (*stasis_log_factory)(void) = STASIS_LOG_FACTORY;
#else
stasis_log_t * (*stasis_log_factory)(void) = stasis_log_default_factory;
#endif

#ifdef STASIS_BUFFER_MANAGER_FACTORY
stasis_buffer_manager_t * (*stasis_buffer_manager_factory)(stasis_log_t*, stasis_dirty_page_table_t*) = STASIS_BUFFER_MANAGER_FACTORY;
#else
stasis_buffer_manager_t * (*stasis_buffer_manager_factory)(stasis_log_t*, stasis_dirty_page_table_t*) = stasis_buffer_manager_concurrent_hash_factory;
#endif

int stasis_buffer_manager_io_handle_flags =
#ifdef STASIS_BUFFER_MANAGER_IO_HANDLE_FLAGS
  STASIS_BUFFER_MANAGER_IO_HANDLE_FLAGS;
#else
  O_NOATIME;
#endif

int stasis_buffer_manager_preallocate_mode =
#ifdef STASIS_BUFFER_MANAGER_PREALLOCATE_MODE
  STASIS_BUFFER_MANAGER_PREALLOCATE_MODE;
#else
#ifdef HAVE_POSIX_FALLOCATE
  STASIS_BUFFER_MANAGER_PREALLOCATE_DEFAULT;
#else
  STASIS_BUFFER_MANAGER_PREALLOCATE_LEGACY;
#endif
#endif

#ifdef STASIS_BUFFER_MANAGER_SIZE
pageid_t stasis_buffer_manager_size = STASIS_BUFFER_MANAGER_SIZE;
#else // STASIS_BUFFER_MANAGER_SIZE
#ifdef MAX_BUFFER_SIZE
pageid_t stasis_buffer_manager_size = MAX_BUFFER_SIZE;
#else // MAX_BUFFER_SIZE
pageid_t stasis_buffer_manager_size = 83107840 / PAGE_SIZE; // ~ 82MB
#endif // MAX_BUFFER_SIZE
#endif // STASIS_BUFFER_MANAGER_SIZE

pageid_t stasis_dirty_page_count_soft_limit=
#ifdef STASIS_DIRTY_PAGE_count_SOFT_LIMIT
  STASIS_DIRTY_PAGE_COUNT_SOFT_LIMIT;
#else
  16 * 1024 * 1024 / PAGE_SIZE;
#endif
pageid_t stasis_dirty_page_low_water_mark =
#ifdef STASIS_DIRTY_PAGE_LOW_WATER_MARK
  STASIS_DIRTY_PAGE_LOW_WATER_MARK;
#else
  (8 * 1024 * 1024) / PAGE_SIZE;
#endif
pageid_t stasis_dirty_page_count_hard_limit =
#ifdef STASIS_DIRTY_PAGE_COUNT_HARD_LIMIT
  STASIS_DIRTY_PAGE_COUNT_HARD_LIMIT;
#else
  (48 * 1024 * 1024) / PAGE_SIZE;
#endif

pageid_t stasis_dirty_page_table_flush_quantum =
#ifdef STASIS_DIRTY_PAGE_TABLE_FLUSH_QUANTUM
  STASIS_DIRTY_PAGE_TABLE_FLUSH_QUANTUM;
#else
  (16 * 1024 * 1024) / PAGE_SIZE;
#endif

stasis_page_handle_t* (*stasis_page_handle_factory)(stasis_log_t*, stasis_dirty_page_table_t*) =
#ifdef STASIS_PAGE_HANDLE_FACTORY
  STASIS_PAGE_HANDLE_FACTORY;
#else
  stasis_page_handle_default_factory;
#endif
stasis_handle_t* (*stasis_handle_factory)() =
#ifdef STASIS_HANDLE_FACTORY
  STASIS_HANDLE_FACTORY;
#else
  stasis_handle_default_factory;
#endif
stasis_handle_t* (*stasis_handle_file_factory)(const char* filename, int open_mode, int creat_perms) =
#ifdef STASIS_FILE_HANDLE_FACTORY
  STASIS_FILE_HANDLE_FACTORY
#else
  stasis_handle_open_pfile;
#endif

stasis_handle_t* (*stasis_non_blocking_handle_file_factory)(const char* filename, int open_mode, int creat_perms) =
#ifdef STASIS_NON_BLOCKING_HANDLE_FILE_FACTORY
  STASIS_NON_BLOCKING_HANDLE_FILE_FACTORY
#else
  stasis_handle_open_pfile;
#endif

#ifdef STASIS_BUFFER_MANAGER_HINT_WRITES_ARE_SEQUENTIAL
int stasis_buffer_manager_hint_writes_are_sequential = STASIS_BUFFER_MANAGER_HINT_WRITES_ARE_SEQUENTIAL;
#else
int stasis_buffer_manager_hint_writes_are_sequential = 0;
#endif

#ifdef STASIS_BUFFER_MANAGER_DEBUG_STRESS_LATCHING
int stasis_buffer_manager_debug_stress_latching = STASIS_BUFFER_MANAGER_DEBUG_STRESS_LATCHING;
#else
int stasis_buffer_manager_debug_stress_latching = 0;
#endif

#ifdef STASIS_REPLACEMENT_POLICY
int stasis_replacement_policy = STASIS_REPLACEMENT_POLICY;
#else
int stasis_replacement_policy = STASIS_REPLACEMENT_POLICY_CLOCK;
#endif

#ifdef STASIS_REPLACEMENT_POLICY_CONCURRENT_WRAPPER_EXPONENTIAL_BACKOFF
int stasis_replacement_policy_concurrent_wrapper_exponential_backoff = STASIS_REPLACEMENT_POLICY_CONCURRENT_WRAPPER_EXPONENTIAL_BACKOFF;
#else
int stasis_replacement_policy_concurrent_wrapper_exponential_backoff = 0;
#endif

#ifdef STASIS_REPLACEMENT_POLICY_CONCURRENT_WRAPPER_POWER_OF_TWO_BUCKETS
int stasis_replacement_policy_concurrent_wrapper_power_of_two_buckets = STASIS_REPLACEMENT_POLICY_CONCURRENT_WRAPPER_POWER_OF_TWO_BUCKETS;
#else
int stasis_replacement_policy_concurrent_wrapper_power_of_two_buckets = 0;
#endif

#ifdef STASIS_SUPPRESS_UNCLEAN_SHUTDOWN_WARNINGS
#error stasis_suppress_unclean_shutdown_warnings cannot be set at compile time.
#endif
int stasis_suppress_unclean_shutdown_warnings = 0;

#ifdef STASIS_TRUNCATION_AUTOMATIC
int stasis_truncation_automatic = STASIS_TRUNCATION_AUTOMATIC;
#else
int stasis_truncation_automatic = 1;
#endif

#ifdef STASIS_LOG_TYPE
int stasis_log_type = STASIS_LOG_TYPE;
#else
int stasis_log_type = LOG_TO_DIR;
#endif

#ifdef STASIS_LOG_IN_MEMORY_MAX_ENTRIES
size_t stasis_log_in_memory_max_entries = STASIS_LOG_IN_MEMORY_MAX_ENTRIES;
#else
size_t stasis_log_in_memory_max_entries = 0;  // unlimited
#endif

#ifdef STASIS_LOG_FILE_NAME
char * stasis_log_file_name = STASIS_LOG_FILE_NAME;
#else
char * stasis_log_file_name = "logfile.txt";
#endif

#ifdef STASIS_STORE_FILE_NAME
char * stasis_store_file_name = STASIS_STORE_FILE_NAME;
#else
char * stasis_store_file_name = "storefile.txt";
#endif

#ifdef STASIS_STORE_FILE_1_NAME
char * stasis_store_file_1_name = STASIS_STORE_FILE_1_NAME;
#else
char * stasis_store_file_1_name = "storefile1.txt";
#endif

#ifdef STASIS_STORE_FILE_2_NAME
char * stasis_store_file_2_name = STASIS_STORE_FILE_2_NAME;
#else
char * stasis_store_file_2_name = "storefile2.txt";
#endif

#ifdef STASIS_BUFFER_MANAGER_HASH_PREFETCH_COUNT
int stasis_buffer_manager_hash_prefetch_count = STASIS_BUFFER_MANAGER_HASH_PREFETCH_COUNT;
#else
int stasis_buffer_manager_hash_prefetch_count = 2;
#endif

#ifdef STASIS_LOG_FILE_MODE
int stasis_log_file_mode = STASIS_LOG_FILE_MODE;
#else
int stasis_log_file_mode = (O_CREAT | O_RDWR);
#endif

#ifdef STASIS_LOG_FILE_PERMISSIONS
int stasis_log_file_permissions = STASIS_LOG_FILE_PERMISSIONS;
#else
int stasis_log_file_permissions = (S_IRUSR | S_IWUSR | S_IRGRP|
                                   S_IWGRP | S_IROTH | S_IWOTH);
#endif

#ifdef STASIS_LOG_DIR_PERMISSSIONS
int stasis_log_dir_permissions = STASIS_LOG_DIR_PERMISSIONS;

#else
int stasis_log_dir_permissions = (S_IRUSR | S_IWUSR | S_IXUSR |
                                  S_IRGRP | S_IWGRP | S_IXGRP |
                                  S_IROTH | S_IWOTH | S_IXOTH );
#endif

#ifdef STASIS_LOG_SOFTCOMMIT
int stasis_log_softcommit = STASIS_LOG_SOFTCOMMIT;
#else
int stasis_log_softcommit = 0;
#endif

#ifdef STASIS_LOG_DIR_NAME
const char* stasis_log_dir_name = STASIS_LOG_DIR_NAME;
#else
const char* stasis_log_dir_name = "stasis_log";
#endif

#ifdef STASIS_LOG_CHUNK_NAME
const char* stasis_log_chunk_name = STASIS_LOG_CHUNK_NAME;
#else
const char* stasis_log_chunk_name = "log-chunk-";
#endif

#ifdef STASIS_LOG_FILE_POOL_LSN_CHARS
#error 2 ^ 64 is 20 chars in base ten, so there is no reason to redefine STASIS_LOG_FILE_POOL_LSN_CHARS
#endif //STASIS_LOG_DIR_LSN_CHARS
const int stasis_log_file_pool_lsn_chars = 20;

#ifdef STASIS_LOG_FILE_WRITE_BUFFER_SIZE
lsn_t stasis_log_file_write_buffer_size = STASIS_LOG_FILE_WRITE_BUFFER_SIZE;
#else
lsn_t stasis_log_file_write_buffer_size = 1024 * 1024;
#endif
#ifdef STASIS_SEGMENTS_ENABLED
int stasis_segments_enabled = STASIS_SEGMENTS_ENABLED;
#else
int stasis_segments_enabled = 0;
#endif
