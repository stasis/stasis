#ifndef __HASH_H
#define __HASH_H

struct hash;

struct hash *hash_new (int);
void hash_free (struct hash *);
unsigned long hash_lookup (struct hash *, unsigned long);
int hash_insert (struct hash *, unsigned long, unsigned long);
unsigned long hash_delete (struct hash *, unsigned long);

#endif /* __HASH_H */

