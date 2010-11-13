/**
 * @file Simple O(n) multiset implementation, with aggregates.
 *
 * This file contains a simple array-backed multiset implementation.  It
 * should perform well for sets that contain up to a few dozen lsn's, and
 * terribly above that (most of the operations are O(n)).
 *
 *  Created on: Nov 12, 2010
 *      Author: sears
 */
#ifndef MULTISET_H_
#define MULTISET_H_
#include <stasis/common.h>

typedef struct stasis_util_multiset_t stasis_util_multiset_t;

stasis_util_multiset_t * stasis_util_multiset_create();
void stasis_util_multiset_destroy(stasis_util_multiset_t * set);
/**
 *  Increase the number of occurences of an item in the set by 1.
 */
void stasis_util_multiset_insert(stasis_util_multiset_t * set, lsn_t item);
/**
 *  Decrease the number of occurences of an item in the set by 1.
 *
 *  @return 1 if the item existed, zero otherwise.
 */
int stasis_util_multiset_remove(stasis_util_multiset_t * set, lsn_t item);
/**
 * @return the lowest lsn_t in the set; LSN_MAX if the set is empty.
 */
lsn_t stasis_util_multiset_min(const stasis_util_multiset_t * set);

#endif /* MULTISET_H_ */
