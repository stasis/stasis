
#ifndef __PAGE_FILE_H
#define __PAGE_FILE_H

#include <stasis/pageHandle.h>
#include <stasis/logger/logger2.h>
BEGIN_C_DECLS
stasis_page_handle_t* openPageFile(stasis_log_t * log, stasis_dirty_page_table_t * dirtyPages);
stasis_page_handle_t* stasis_page_handle_deprecated_factory(stasis_log_t *log, stasis_dirty_page_table_t *dpt);
END_C_DECLS
#endif /* __PAGE_FILE_H */
