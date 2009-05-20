
#ifndef __PAGE_FILE_H
#define __PAGE_FILE_H

#include <stasis/pageHandle.h>
#include <stasis/logger/logger2.h>

stasis_page_handle_t* openPageFile(stasis_log_t * log, stasis_dirty_page_table_t * dirtyPages);

#endif /* __PAGE_FILE_H */
