/*
 * concurrentBufferManager.h
 *
 *  Created on: Nov 23, 2009
 *      Author: sears
 */

#ifndef CONCURRENTBUFFERMANAGER_H_
#define CONCURRENTBUFFERMANAGER_H_
#include <stasis/bufferManager.h>
stasis_buffer_manager_t* stasis_buffer_manager_concurrent_hash_factory(stasis_log_t *log, stasis_dirty_page_table_t *dpt);
#endif /* CONCURRENTBUFFERMANAGER_H_ */
