#include <string.h>
#include "debug.h"
#include "xmem.h"


struct hash_item {
    struct hash_item *next;
    unsigned long key;
    unsigned long val;
};

struct hash_table {
    int bucket_max;
    unsigned long bucket_mask;
    struct hash_item **table;
};


struct hash_table *
hash_new (int nbuckexp)
{
    int bucket_max = (int) ((unsigned long) 1 << nbuckexp);
    unsigned long bucket_mask = (unsigned long) bucket_max - 1;
    struct hash_table *h;

    h = (struct hash_table *) XMALLOC (sizeof (struct hash_table));
    if (h)
	h->table =
	    (struct hash_item **) XMALLOC (sizeof (struct hash_item *) * bucket_max);
    if (! (h && h->table)) {
	if (h)
	    XFREE (h);
	return NULL;
    }

    h->bucket_max = bucket_max;
    h->bucket_mask = bucket_mask;
    memset (h->table, 0, sizeof (struct hash_item *) * bucket_max);

    return h;
}

void
hash_free (struct hash_table *h)
{
    int bucket_max = h->bucket_max;
    struct hash_item *bucket, *next;
    int i;

    for (i = 0; i < bucket_max; i++) {
	for (bucket = h->table[i]; bucket; bucket = next) {
	    next = bucket->next;
	    XFREE (bucket);
	}
    }
    XFREE (h->table);
    XFREE (h);
}

unsigned long
hash_lookup (struct hash_table *h, unsigned long key)
{
    unsigned long bucket_mask = h->bucket_mask;
    int bucket_index = (int) (key & bucket_mask);
    struct hash_item *bucket;
    unsigned long val;

    debug ("tracing bucket %d for key %lu (%p)",
	   bucket_index, key, (void *) key);

    for (bucket = h->table[bucket_index]; bucket; bucket = bucket->next)
	if (bucket->key == key) {
	    val = bucket->val;
	    debug ("found %lu->%lu (%p->%p)",
		   key, val, (void *) key, (void *) val);
	    return val;
	}

    debug ("not found");

    return 0;
}

int
hash_insert (struct hash_table *h, unsigned long key, unsigned long val)
{
    unsigned long bucket_mask = h->bucket_mask;
    int bucket_index = (int) (key & bucket_mask);
    struct hash_item *new;

    new = (struct hash_item *) XMALLOC (sizeof (struct hash_item));
    if (! new) {
	debug ("allocation failed");
	return -1;
    }

    debug ("inserting %lu->%lu (%p->%p) to bucket %d",
	   key, val, (void *) key, (void *) val, bucket_index);

    new->key = key;
    new->val = val;
    new->next = h->table[bucket_index];
    h->table[bucket_index] = new;

    return 0;
}

unsigned long
hash_delete (struct hash_table *h, unsigned long key)
{
    unsigned long bucket_mask = h->bucket_mask;
    int bucket_index = (int) (key & bucket_mask);
    struct hash_item *bucket, *prev;
    unsigned long val;

    debug ("tracing bucket %d for key %lu (%p)",
	   bucket_index, key, (void *) key);

    for (prev = NULL, bucket = h->table[bucket_index]; bucket;
	 prev = bucket, bucket = bucket->next)
	if (bucket->key == key) {
	    val = bucket->val;
	    debug ("deleting %lu->%lu (%p->%p)",
		   key, val, (void *) key, (void *) val);

	    if (prev)
		prev->next = bucket->next;
	    else
		h->table[bucket_index] = bucket->next;

	    XFREE (bucket);
	    return val;
	}

    debug ("not found");

    return 0;
}
