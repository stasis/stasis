#ifndef __INMEMORYLOG
#define __INMEMORYLOG

#include <stasis/logger/logger2.h>
/**
 * Allocate a new non-persistent Stasis log.
 */
stasis_log_t* stasis_log_impl_in_memory_open();

#endif
