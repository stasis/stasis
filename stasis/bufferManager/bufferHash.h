#ifndef STASIS_BUFFERMANAGER_BUFFERHASH_H
#define STASIS_BUFFERMANAGER_BUFFERHASH_H
#include <stasis/common.h>
BEGIN_C_DECLS
#include <stasis/bufferManager.h>
#include <stasis/pageHandle.h>
stasis_buffer_manager_t* stasis_buffer_manager_hash_open(stasis_page_handle_t *ph, stasis_log_t *log, stasis_dirty_page_table_t *dpt);
stasis_buffer_manager_t* stasis_buffer_manager_hash_factory(stasis_log_t *log, stasis_dirty_page_table_t *dpt);
END_C_DECLS
#endif //STASIS_BUFFERMANAGER_BUFFERHASH_H
