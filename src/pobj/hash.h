#ifndef __HASH_H
#define __HASH_H

struct hash_table;

struct hash_table *hash_new (int);
void hash_free (struct hash_table *);
unsigned long hash_lookup (struct hash_table *, unsigned long);
void hash_insert (struct hash_table *, unsigned long, unsigned long);
unsigned long hash_delete (struct hash_table *, unsigned long);

#endif /* __HASH_H */

