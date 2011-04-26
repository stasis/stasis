/*
 * bTree.h
 *
 *  Created on: Jul 29, 2009
 *      Author: sears
 */

#ifndef BTREE_H_
#define BTREE_H_

#include <stasis/operations.h>

typedef int(*stasis_comparator_t)(const void*, size_t, const void*, size_t, void*);
typedef int16_t stasis_comparator_id_t;

void BtreeDeinit(void);
void BtreeInit();
recordid TbtreeCreate(int xid, stasis_comparator_id_t cmp_id);
int TbtreeLookup(int xid, recordid rid, void * cmp_arg, byte * key, size_t keySize, byte ** value, size_t* valueSize);
int TbtreeInsert(int xid, recordid rid, void *cmp_arg, byte *key, size_t keySize, byte *value, size_t valueSize);

#endif /* BTREE_H_ */
