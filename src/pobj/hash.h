#ifndef __HASH_H
#define __HASH_H


/* Hash table type and declaration macro. */
typedef struct hash_item **hash_table;
#define HASH_DECLARE(name,nbuckexp)  struct hash_item *name[1 << nbuckexp]


/* Prototypes. */
void hash_init (hash_table, int);
void hash_close (hash_table, int);
unsigned long hash_lookup (hash_table, int, unsigned long);
void hash_insert (hash_table, int, unsigned long, unsigned long);
unsigned long hash_delete (hash_table, int, unsigned long);

#endif /* __HASH_H */

