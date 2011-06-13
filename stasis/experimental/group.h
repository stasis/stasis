/*
 * group.h
 *
 *  Created on: Jun 30, 2009
 *      Author: sears
 */

#ifndef GROUP_H_
#define GROUP_H_

#include<stasis/iterator.h>

typedef struct stasis_group_t {
  int (*put)(struct stasis_group_t * impl, byte* key, size_t keylen, byte* val, size_t vallen);
  lladdIterator_t * (*done)(struct stasis_group_t *impl);
  void* impl;
} stasis_group_t;

stasis_group_t * TlogStructuredGroup(int xid, size_t runlen);
void stasis_log_structured_group_init();
#endif /* GROUP_H_ */
