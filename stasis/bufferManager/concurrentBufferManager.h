/*
 * concurrentBufferManager.h
 *
 *  Created on: Nov 23, 2009
 *      Author: sears
 */

#ifndef CONCURRENTBUFFERMANAGER_H_
#define CONCURRENTBUFFERMANAGER_H_
#include <stasis/bufferManager.h>
BEGIN_C_DECLS
stasis_buffer_manager_t* stasis_buffer_manager_concurrent_hash_factory(stasis_log_t *log, stasis_dirty_page_table_t *dpt);
stasis_buffer_manager_t* stasis_buffer_manager_concurrent_hash_open(stasis_page_handle_t * h, stasis_log_t * log, stasis_dirty_page_table_t * dpt);
END_C_DECLS
#endif /* CONCURRENTBUFFERMANAGER_H_ */
