#include <stasis/common.h>
#include <stasis/flags.h>
#include <stasis/constants.h>

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
