#include <string.h>
#include "hash.h"
#include "debug.h"
#include "xmem.h"


/* Hash item. */
struct hash_item {
    struct hash_item *next;
    unsigned long key;
    unsigned long val;
};


void
hash_init (hash_table table, int nbuckexp)
{
    int bucket_max = (int) ((unsigned long) 1 << nbuckexp);

    memset (table, 0, bucket_max * sizeof (struct hash_item *));
}

void
hash_close (hash_table table, int nbuckexp)
{
    int bucket_max = (int) ((unsigned long) 1 << nbuckexp);
    struct hash_item *bucket, *next;
    int i;

    for (i = 0; i < bucket_max; i++) {
	for (bucket = table[i]; bucket; bucket = next) {
	    next = bucket->next;
	    XFREE (XMEM_HASH, bucket);
	}
	table[i] = NULL;
    }
}

unsigned long
hash_lookup (hash_table table, int nbuckexp, unsigned long key)
{
    int bucket_index = (int) (key & (((unsigned long) 1 << nbuckexp) - 1));
    struct hash_item *bucket;
    unsigned long val;

    debug ("tracing bucket %d for key %lu (%p)",
	   bucket_index, key, (void *) key);

    for (bucket = table[bucket_index]; bucket; bucket = bucket->next)
	if (bucket->key == key) {
	    val = bucket->val;
	    debug ("found %lu->%lu (%p->%p)",
		   key, val, (void *) key, (void *) val);
	    return val;
	}

    debug ("not found");

    return 0;
}

void
hash_insert (hash_table table, int nbuckexp,
	     unsigned long key, unsigned long val)
{
    int bucket_index = (int) (key & (((unsigned long) 1 << nbuckexp) - 1));
    struct hash_item *new;

    debug ("inserting %lu->%lu (%p->%p) to bucket %d",
	   key, val, (void *) key, (void *) val, bucket_index);

    new = (struct hash_item *) XMALLOC (XMEM_HASH, sizeof (struct hash_item));
    new->key = key;
    new->val = val;
    new->next = table[bucket_index];
    table[bucket_index] = new;
}

unsigned long
hash_delete (hash_table table, int nbuckexp, unsigned long key)
{
    int bucket_index = (int) (key & (((unsigned long) 1 << nbuckexp) - 1));
    struct hash_item *bucket, *prev;
    unsigned long val;

    debug ("tracing bucket %d for key %lu (%p)",
	   bucket_index, key, (void *) key);

    for (prev = NULL, bucket = table[bucket_index]; bucket;
	 prev = bucket, bucket = bucket->next)
	if (bucket->key == key) {
	    val = bucket->val;
	    debug ("deleting %lu->%lu (%p->%p)",
		   key, val, (void *) key, (void *) val);

	    if (prev)
		prev->next = bucket->next;
	    else
		table[bucket_index] = bucket->next;

	    XFREE (XMEM_HASH, bucket);
	    return val;
	}

    debug ("not found");

    return 0;
}
