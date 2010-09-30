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
typedef struct bucket_t bucket_t;

typedef struct hashtable_bucket_handle_t {
  bucket_t * b1;
  pageid_t key;
  pageid_t idx;
  void * ret;
} hashtable_bucket_handle_t;

hashtable_t * hashtable_init(pageid_t size);
void hashtable_deinit(hashtable_t * ht);
void * hashtable_insert(hashtable_t *ht, pageid_t p, void * val);
/** Atomically insert a value if the key was not already defined
 * @return NULL if val was inserted
 */
void * hashtable_test_and_set(hashtable_t *ht, pageid_t p, void * val);
void * hashtable_lookup(hashtable_t *ht, pageid_t p);
void * hashtable_remove(hashtable_t *ht, pageid_t p);


void * hashtable_test_and_set_lock(hashtable_t *ht, pageid_t p, void * val, hashtable_bucket_handle_t *h);
void * hashtable_lookup_lock(hashtable_t *ht, pageid_t p, hashtable_bucket_handle_t *h);
void * hashtable_remove_begin(hashtable_t *ht, pageid_t p, hashtable_bucket_handle_t *h);
void   hashtable_remove_finish(hashtable_t *ht, hashtable_bucket_handle_t *h);
void   hashtable_remove_cancel(hashtable_t *ht, hashtable_bucket_handle_t *h);

/**
 * @return -0 if key not found, 1 if the key exists, >1 if the hashtable is corrupt, and the key appears multiple times..
 */
int hashtable_debug_number_of_key_copies(hashtable_t *ht, pageid_t pageied);

void hashtable_unlock(hashtable_bucket_handle_t *h);

#endif /* CONCURRENTHASH_H_ */
