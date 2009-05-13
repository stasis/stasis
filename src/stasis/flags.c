#include <stasis/common.h>
#include <stasis/flags.h>
#include <stasis/constants.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#ifdef BUFFER_MANAGER_TYPE
int bufferManagerType = BUFFER_MANAGER_TYPE;
#else
int bufferManagerType = BUFFER_MANAGER_HASH;
#endif

#ifdef BUFFER_MANAGER_O_DIRECT
int bufferManagerO_DIRECT = BUFFER_MANAGER_O_DIRECT;
#else
int bufferManagerO_DIRECT = 0;
#endif

#ifdef BUFFER_MANAGER_FILE_HANDLE_TYPE
int bufferManagerFileHandleType = BUFFER_MANAGER_FILE_HANDLE_TYPE
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
#error stasus_suppress_unclean_shutdown_warnings cannot be set at compile time.
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

#ifdef STASIS_LOG_FILE_MODE
int stasis_log_file_mode = STASIS_LOG_FILE_MODE;
#else
int stasis_log_file_mode = (O_CREAT | O_RDWR | O_SYNC);
#endif

#ifdef STASIS_LOG_FILE_PERMISSIONS
int stasis_log_file_permissions = STASIS_LOG_FILE_PERMISSIONS;
#else
int stasis_log_file_permissions = (S_IRUSR | S_IWUSR | S_IRGRP|
                                   S_IWGRP | S_IROTH | S_IWOTH);
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
