#include <stasis/common.h>
#include <stasis/flags.h>
#include <stasis/constants.h>

#include <stasis/transactional.h>
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

#ifdef STASIS_BUFFER_MANAGER_SIZE
pageid_t stasis_buffer_manager_size = STASIS_BUFFER_MANAGER_SIZE;
#else // STASIS_BUFFER_MANAGER_SIZE
#ifdef MAX_BUFFER_SIZE
pageid_t stasis_buffer_manager_size = MAX_BUFFER_SIZE;
#else // MAX_BUFFER_SIZE
pageid_t stasis_buffer_manager_size = 20029; // ~ 82MB
#endif // MAX_BUFFER_SIZE
#endif // STASIS_BUFFER_MANAGER_SIZE

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

#ifdef BUFFER_MANAGER_O_DIRECT
int bufferManagerO_DIRECT = BUFFER_MANAGER_O_DIRECT;
#else
int bufferManagerO_DIRECT = 0;
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

#ifdef BUFFER_MANAGER_FILE_HANDLE_TYPE
int bufferManagerFileHandleType = BUFFER_MANAGER_FILE_HANDLE_TYPE;
#else
int bufferManagerFileHandleType = BUFFER_MANAGER_FILE_HANDLE_PFILE;
#endif

#ifdef BUFFER_MANAGER_SLOW_HANDLE_TYPE
int bufferManagerNonBlockingSlowHandleType
                              = BUFFER_MANAGER_NON_BLOCKING_SLOW_HANDLE_TYPE;
#else
int bufferManagerNonBlockingSlowHandleType = IO_HANDLE_PFILE;
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
int stasis_log_type = LOG_TO_FILE;
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

#ifdef STASIS_LOG_SOFTCOMMIT
int stasis_log_softcommit = STASIS_LOG_SOFTCOMMIT;
#else
int stasis_log_softcommit = 0;
#endif

#ifdef STASIS_LOG_DIR
const char* stasis_log_dir_name = STASIS_LOG_DIR;
#else
const char* stasis_log_dir_name = "stasis_log";
#endif

#ifdef STASIS_LOG_DIR_LSN_CHARS
#error 2 ^ 64 is 20 chars in base ten, so there is no reason to redefine STASIS_LOG_DIR_LSN_CHARS
#endif //STASIS_LOG_DIR_LSN_CHARS
const int stasis_log_dir_name_lsn_chars = 20;

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
