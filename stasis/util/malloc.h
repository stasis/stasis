/*
 * malloc.h
 *
 *  Created on: Nov 14, 2012
 *      Author: sears
 */

#ifndef MALLOC_H_
#define MALLOC_H_

#include <stasis/common.h>

#define stasis_alloca(cnt, typ) ((typ*)alloca((cnt)*sizeof(typ)))
#define stasis_alloc(typ) ((typ*)malloc(sizeof(typ)))
#define stasis_malloc(cnt, typ) ((typ*)malloc((cnt)*sizeof(typ)))
#define stasis_malloc_trailing_array(typ, array_sz) ((typ*)malloc(sizeof(typ)+(array_sz)))
#define stasis_calloc(cnt, typ) ((typ*)calloc((cnt),sizeof(typ)))
#define stasis_calloc_trailing_array(typ, array_sz) ((typ*)calloc(sizeof(typ)+(array_sz),1))
#define stasis_realloc(ptr, cnt, typ) ((typ*)realloc(ptr, (cnt)*sizeof(typ)))
#define stasis_free(ptr) free(ptr)

#endif /* MALLOC_H_ */
