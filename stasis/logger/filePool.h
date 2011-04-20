#ifndef __STASIS_LOG_FILE_POOL_H
#define __STASIS_LOG_FILE_POOL_H

#include <stasis/common.h>
#include <stasis/logger/logger2.h>

BEGIN_C_DECLS

stasis_log_t* stasis_log_file_pool_open(const char* dirname, int filemode, int fileperm);

END_C_DECLS
#endif
