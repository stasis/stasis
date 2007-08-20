#include <stasis/common.h>
#include <stasis/flags.h>
#include <stasis/constants.h>

#ifdef BUFFER_MANAGER_TYPE
int bufferManagerType = BUFFER_MANAGER_TYPE;
#else
int bufferManagerType = BUFFER_MANAGER_HASH;
#endif

#ifdef BUFFER_MANAGER_O_DIRECT
int bufferManagerO_DIRECT = 1;
#else
int bufferManagerO_DIRECT = 0;
#endif

#ifdef BUFFER_MANAGER_FILE_HANDLE_TYPE
int bufferManagerFileHandleType = BUFFER_MANAGER_FILE_HANDLE_TYPE
#else
int bufferManagerFileHandleType = BUFFER_MANAGER_FILE_HANDLE_NON_BLOCKING;
#endif

#ifdef BUFFER_MANAGER_SLOW_HANDLE_TYPE
int bufferManagerNonBlockingSlowHandleType
                              = BUFFER_MANAGER_NON_BLOCKING_SLOW_HANDLE_TYPE;
#else
int bufferManagerNonBlockingSlowHandleType = IO_HANDLE_PFILE;
#endif
