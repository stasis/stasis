/*
 * concurrentHash.h
 *
 *  Created on: Oct 15, 2009
 *      Author: sears
 */

#ifndef CONCURRENTHASH_H_
#define CONCURRENTHASH_H_
#include <stasis/common.h>

typedef struct hashtable_t hashtable_t;

hashtable_t * hashtable_init(pageid_t size, int tracknum);
void hashtable_deinit(hashtable_t * ht);
void * hashtable_insert(hashtable_t *ht, pageid_t p, void * val);
/** Atomically insert a value if the key was not already defined
 * @return NULL if val was inserted
 */
void * hashtable_test_and_set(hashtable_t *ht, pageid_t p, void * val);
void * hashtable_lookup(hashtable_t *ht, pageid_t p);
void * hashtable_remove(hashtable_t *ht, pageid_t p);

#endif /* CONCURRENTHASH_H_ */
