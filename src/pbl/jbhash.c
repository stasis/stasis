/*
 * A durable, recoverable hashtable
 * Based on Peter Graf's pblhash, <http://mission.base.com/peter/source/>
 * Jim Blomo <jim@xcf.berkeley.edu>
 * $Id$
 */

#include <assert.h>
#include <stdio.h>
#include <memory.h>
#include <stdlib.h>

#include <lladd/common.h>
#include <pbl/jbhash.h>

const recordid ZERO_RECORDID = {0,0,0};

jbHashTable_t* jbHtCreate(int xid, int size) {

	jbHashTable_t *ht;
	ht = (jbHashTable_t*)malloc(sizeof(jbHashTable_t));

	if( ht ) {
	        recordid * hm = malloc(sizeof(recordid) * size);
		if(hm) {
		  memset(hm, 0, sizeof(recordid)*size);
		  
		  ht->size  = size;
		  ht->store = Talloc(xid, sizeof(jbHashTable_t));
		  ht->hashmap_record = Talloc(xid, sizeof(recordid) * size);
		  ht->hashmap = NULL; /* Always should be NULL in the store, so that we know if we need to read it in */
		  Tset(xid, ht->store, ht);
		  ht->hashmap = hm;
		  Tset(xid, ht->hashmap_record, ht->hashmap);
		  ht->iterIndex = 0;
		  ht->iterData = NULL;

		  return ht;
		} else {
		  free(ht);
		  return NULL;
		}
	}

	return NULL;
}

int jbHtValid(int xid, jbHashTable_t *ht) {

	int ret;
	jbHashTable_t *test = (jbHashTable_t*)malloc(sizeof(jbHashTable_t));
	Tread(xid, ht->store, test);
	ret = ( test->store.size == ht->store.size
			&& test->store.slot == ht->store.slot
			&& test->store.page == ht->store.page );
	/* TODO:  Check hashmap_record? */
	free(test);

	return ret;
}

/**
 * Hash function generator, taken directly from pblhash
 */
static int hash( const unsigned char * key, size_t keylen, int size ) {
    int ret = 104729;

    for( ; keylen-- > 0; key++ )
    {
        if( *key )
        {
            ret *= *key + keylen;
            ret %= size;
        }
    }

    return( ret % size );
}

/** Should be called the first time ht->hashmap is accessed by a library function.  
    Checks to see if the hashmap record has been read in, reads it if necessary, and then
    returns a pointer to it. */ 
static recordid* _getHashMap(int xid, jbHashTable_t *ht) {
  if(! ht->hashmap) {
    ht->hashmap = malloc(sizeof(recordid) * ht->size);
    Tread(xid, ht->hashmap_record, ht->hashmap);
  }
  return ht->hashmap;
}

int jbHtInsert(int xid, jbHashTable_t *ht, const byte *key, size_t keylen, const byte *dat, size_t datlen) {

	int index = hash( key, keylen, ht->size);
	recordid rid = _getHashMap(xid, ht)[index];
	byte *newd;
	jbHashItem_t newi;

	if( rid.size == 0 ) { /* nothing with this hash has been inserted */

		newi.store = Talloc(xid, sizeof(jbHashItem_t)+keylen+datlen);
		newd = malloc(sizeof(jbHashItem_t)+keylen+datlen);
		newi.keylen = keylen;
		newi.datlen = datlen;
		newi.next = ZERO_RECORDID;
		memcpy(newd, &newi, sizeof(jbHashItem_t));
		memcpy(newd+sizeof(jbHashItem_t), key, keylen);
		memcpy(newd+sizeof(jbHashItem_t)+keylen, dat, datlen);
		Tset(xid, newi.store, newd);

		ht->hashmap[index] = newi.store;
		/* Tset(xid, ht->store, ht); */
		Tset(xid, ht->hashmap_record, ht->hashmap);
		
		free(newd);

	} else { 

		byte *item = NULL;

		do {

			free(item); /* NULL ignored by free */
			item = malloc(rid.size);
			Tread(xid, rid, item);
			if( ((jbHashItem_t*)item)->keylen == keylen && !memcmp(key, item+sizeof(jbHashItem_t), keylen)) {
				memcpy(item+sizeof(jbHashItem_t)+keylen, dat, ((jbHashItem_t*)item)->datlen);
				Tset(xid, ((jbHashItem_t*)item)->store, item);
				free(item);
				return 0;
			}
			rid = ((jbHashItem_t*)item)->next; /* could go off end of list */
		} while( ((jbHashItem_t*)item)->next.size != 0 );
		/* now item is the tail */

		newi.store = Talloc(xid, sizeof(jbHashItem_t)+keylen+datlen);
		newd = malloc(sizeof(jbHashItem_t)+keylen+datlen);
		newi.keylen = keylen;
		newi.datlen = datlen;
		newi.next = ZERO_RECORDID;
		memcpy(newd, &newi, sizeof(jbHashItem_t));
		memcpy(newd+sizeof(jbHashItem_t), key, keylen);
		memcpy(newd+sizeof(jbHashItem_t)+keylen, dat, datlen);
		Tset(xid, newi.store, newd);

		((jbHashItem_t*)item)->next = newi.store;
		Tset(xid, ((jbHashItem_t*)item)->store, item);
		free(item);
		free(newd);
	}

	return 0;
}

int jbHtLookup( int xid, jbHashTable_t *ht, const byte *key, size_t keylen, byte *buf ) {

	int index = hash(key, keylen, ht->size);
	recordid rid = _getHashMap(xid, ht)[index];
	if( rid.size == 0 ) { /* nothing inserted with this hash */
		return -1;
	} else {
		byte *item = NULL;
		item = malloc(rid.size);
		Tread(xid, rid, item);

		for( ; !(((jbHashItem_t*)item)->keylen == keylen && !memcmp(key, item+sizeof(jbHashItem_t), keylen));
				rid = ((jbHashItem_t*)item)->next ) {
			if( rid.size == 0) { /* at the end of the list and not found */
				return -1;
			}
			free(item);
			item = malloc(rid.size);
			Tread(xid, rid, item);
		}
		/* rid is what we want */

		memcpy(buf, item+sizeof(jbHashItem_t)+((jbHashItem_t*)item)->keylen, ((jbHashItem_t*)item)->datlen);
		free(item);
		return 0;
	}

	return 0;
}

int jbHtRemove( int xid, jbHashTable_t *ht, const byte *key, size_t keylen, byte *buf ) {

	int index = hash(key, keylen, ht->size);
	recordid rid = _getHashMap(xid, ht)[index];
	if( rid.size == 0) { /* nothing inserted with this hash */
		return -1;
	} else {
		byte *del = malloc(rid.size);
		Tread(xid, rid, del);
		if( ((jbHashItem_t*)del)->keylen == keylen && !memcmp(key, del+sizeof(jbHashItem_t), keylen) ) {
			/* the head is the entry to be removed */
			if( buf ) {
				memcpy( buf, del+sizeof(jbHashItem_t*)+keylen, ((jbHashItem_t*)del)->datlen);
			}
			ht->hashmap[index] = ((jbHashItem_t*)del)->next;
			/* Tset(xid, ht->store, ht); */
			Tset(xid, ht->hashmap_record, ht->hashmap);
			/* TODO: dealloc rid */
			free(del);
			return 0;
		} else {
			byte * prevd = NULL;
			while( ((jbHashItem_t*)del)->next.size ) {
				free(prevd); /* free will ignore NULL args */
				prevd = del;
				rid = ((jbHashItem_t*)del)->next;
				del = malloc(rid.size);
				Tread(xid, rid, del);
				if( ((jbHashItem_t*)del)->keylen == keylen && !memcmp(key, del+sizeof(jbHashItem_t), keylen) ) {
					if( buf ) {
						memcpy( buf, del+sizeof(jbHashItem_t)+keylen, ((jbHashItem_t*)del)->datlen);
					}
					((jbHashItem_t*)prevd)->next = ((jbHashItem_t*)del)->next;
					Tset(xid, ((jbHashItem_t*)prevd)->store, prevd);
					/* TODO: dealloc rid */
					free(prevd);
					free(del);
					return 0;
				}
			}
			/* could not find exact key */

			free(prevd);
			free(del);
			return -1;
		}
	}

	assert( 0 ); /* should not get here */
	return -1;
}

int jbHtFirst( int xid, jbHashTable_t *ht, byte *buf ) {

	ht->iterIndex = 0;
	ht->iterData = NULL;
	return jbHtNext( xid, ht, buf);
}

int jbHtNext( int xid, jbHashTable_t *ht, byte *buf ) {
  _getHashMap(xid, ht);
	if( ht->iterData && (((jbHashItem_t*)(ht->iterData))->next.size != 0) ) {
		recordid next = ((jbHashItem_t*)(ht->iterData))->next;
		free( ht->iterData );
		ht->iterData = malloc(next.size);
		Tread(xid, next, ht->iterData);
	} else {
		while(ht->iterIndex < ht->size) {
			if( ht->hashmap[ht->iterIndex].size ) 
				break;
			else
				ht->iterIndex++;
		}
		if( ht->iterIndex == ht->size) /* went through and found no data */
			return -1;

		free( ht->iterData );
		ht->iterData = malloc(ht->hashmap[ht->iterIndex].size); /* to account for the last post incr */
		Tread(xid, ht->hashmap[ht->iterIndex++], ht->iterData); /* increment for next round */
	}

	return jbHtCurrent(xid, ht, buf);
}

int jbHtCurrent(int xid, jbHashTable_t *ht, byte *buf) {

	if( ht->iterData ) {
		memcpy(buf, ht->iterData + sizeof(jbHashItem_t) + ((jbHashItem_t*)(ht->iterData))->keylen, ((jbHashItem_t*)(ht->iterData))->datlen);
		return 0;
	}
	return -1;
}


int jbHtCurrentKey(int xid, jbHashTable_t *ht, byte *buf) {

	if( ht->iterData ) {
		memcpy(buf, ht->iterData + sizeof(jbHashItem_t), ((jbHashItem_t*)(ht->iterData))->keylen);
		return 0;
	}
	return -1;
}

int jbHtDelete(int xid, jbHashTable_t *ht) {

	/* deralloc ht->store */

        if(ht->hashmap) { free(ht->hashmap); }    
        free(ht);

	return 0;
}
