/*
 * slab.h
 *
 *  Created on: Nov 12, 2010
 *      Author: sears
 */

#ifndef SLAB_H_
#define SLAB_H_

#include <stasis/common.h>

BEGIN_C_DECLS

typedef struct stasis_util_slab_t stasis_util_slab_t;
stasis_util_slab_t * stasis_util_slab_create(uint32_t obj_sz, uint32_t block_sz);
void stasis_util_slab_ref(stasis_util_slab_t * slab);
void stasis_util_slab_destroy(stasis_util_slab_t * slab);
void* stasis_util_slab_malloc(stasis_util_slab_t * slab);
void stasis_util_slab_free(stasis_util_slab_t * slab, void * ptr);

END_C_DECLS

#endif /* SLAB_H_ */
