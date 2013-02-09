#ifndef STASIS_PAGE_ARRAY_H
#define STASIS_PAGE_ARRAY_H
#include <stasis/bufferManager.h>
BEGIN_C_DECLS
stasis_buffer_manager_t* stasis_buffer_manager_mem_array_open();
stasis_buffer_manager_t* stasis_buffer_manager_mem_array_factory(stasis_log_t * log, stasis_dirty_page_table_t *dpt);
END_C_DECLS
#endif // STASIS_PAGE_ARRAY_H
