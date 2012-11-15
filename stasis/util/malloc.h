/*
 * malloc.h
 *
 *  Created on: Nov 14, 2012
 *      Author: sears
 */

#ifndef MALLOC_H_
#define MALLOC_H_

#include <stasis/common.h>

#define stasis_malloc(cnt, typ) ((typ*)malloc((cnt)*sizeof(typ)))
#define stasis_calloc(cnt, typ) ((typ*)calloc((cnt),sizeof(typ)))
#define stasis_realloc(ptr, cnt, typ) ((typ*)realloc(ptr, (cnt)*sizeof(typ)))
#define stasis_free(ptr) free(ptr)

#endif /* MALLOC_H_ */
