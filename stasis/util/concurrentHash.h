/**
 * concurrentHash.h
 *
 * @file A concurrent, fixed-size hashtable that allows users to obtain latches
 *       on its keys.
 *
 * Operations against this hashtable proceed in two phases.  In the first phase,
 * the bucket that contains (or will contain) the requested key is located.  At
 * this point, the implementation optionally returns control to the caller,
 * which may examine the bucket, and decide to complete or cancel the operation.
 *
 * Of course, like any other mutex, bucket latches allow you to write code that
 * will deadlock.  Initiating an operation against a hashtable while holding a
 * latch on one of its buckets is unsafe, and will lead to deadlocks and other
 * bad behavior.
 *
 * Notes:
 *
 * It would be trivial to implement an insert_begin, _finish, and _remove, but
 * the need for such things has never come up.  (See hashtable_test_and_set instead)
 *
 *  Created on: Oct 15, 2009
 *      Author: sears
 */

#ifndef CONCURRENTHASH_H_
#define CONCURRENTHASH_H_
#include <stasis/common.h>

BEGIN_C_DECLS

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

/** Be sure to call this immediately after calling an methods whose names end in "_lock()" */
void hashtable_unlock(hashtable_bucket_handle_t *h);

/**
 * @return -0 if key not found, 1 if the key exists, >1 if the hashtable is corrupt, and the key appears multiple times..
 */
int hashtable_debug_number_of_key_copies(hashtable_t *ht, pageid_t pageied);

END_C_DECLS

#endif /* CONCURRENTHASH_H_ */
