#include <lladd/operations/lladdhash.h>

#include <assert.h>
#include <stdio.h>
#include <memory.h>
#include <stdlib.h>


#include <lladd/transactional.h>

static const recordid ZERO_RECORDID = {0,0,0};

typedef struct {
  int ht;
  size_t keylen;
  size_t datlen;
}  lladdHashRec_t;

static lladdHash_t * lladdHashes[MAX_LLADDHASHES];
int next_lladdHash = 0;

lladdHash_t * lHtCreate(int xid, int size) {

	lladdHash_t *ht;
	
	ht = lladdHashes[next_lladdHash] = (lladdHash_t*)malloc(sizeof(lladdHash_t));

	
	if( ht ) {
	        recordid * hm = malloc(sizeof(recordid) * size);
		if(hm) {
		  memset(hm, 0, sizeof(recordid)*size);
		  
		  ht->size  = size;
		  ht->store = next_lladdHash; /*Talloc(xid, sizeof(lladdHash_t));*/
		  ht->hashmap_record = Talloc(xid, sizeof(recordid) * size);
		  /*ht->hashmap = NULL;*/ /* Always should be NULL in the store, so that we know if we need to read it in */
		  /*		  Tset(xid, ht->store, ht); */
		  ht->hashmap = hm;
		  Tset(xid, ht->hashmap_record, ht->hashmap);
		  ht->iterIndex = 0;
		  ht->iterData = NULL;

		  next_lladdHash++;

		  return ht;
		} else {
		  free(ht);
		  return NULL;
		}
	}

	return NULL;
}

int lHtValid(int xid, lladdHash_t *ht) {
  /*
	int ret;
	lladdHash_t *test ; = (lladdHash_t*)malloc(sizeof(lladdHash_t));
		Tread(xid, ht->store, test);
	ret = ( test->store.size == ht->store.size
			&& test->store.slot == ht->store.slot
			&& test->store.page == ht->store.page ); */
	/* TODO:  Check hashmap_record? */
	/*	free(test); */

	assert(0); /* unimplemented! */

	return 1;
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
static recordid* _getHashMap(int xid, lladdHash_t *ht) {
  if(! ht->hashmap) { 
    printf("Reading in hashmap.\n");
    ht->hashmap = malloc(sizeof(recordid) * ht->size);
    Tread(xid, ht->hashmap_record, ht->hashmap);
  }


  return ht->hashmap;
}
/* TODO:  	Insert and Remove need to bypass Talloc(), so that recovery won't crash.  (Otherwise, we double-free records...This 
					was not noticed before, since recovery never freed pages.)  */
int _lHtInsert(int xid, recordid garbage, lladdHashRec_t * arg) {

  /* recordid ht_rec = arg->ht; */

  size_t keylen = arg->keylen;
  size_t datlen = arg->datlen;
  void * key = ((void*)arg) + sizeof(lladdHashRec_t);
  void * dat = ((void*)arg) + sizeof(lladdHashRec_t) + keylen; 


  lladdHash_t * ht;
  int index; 
  recordid rid; 
  void *newd;
  lladdHashItem_t newi;


//  printf("Inserting %d -> %d\n", *(int*)key, *(int*)dat); 

  ht = lladdHashes[arg->ht];

  /*  Tread(xid, ht_rec, &ht); */

  index = hash( key, keylen, ht->size);
  rid = _getHashMap(xid, ht)[index];
  
  /*  printf("Inserting %d -> %s %d {%d %d %d}\n", *(int*)key, dat, index, rid.page, rid.slot, rid.size); */

  
  if( rid.size == 0 ) { /* nothing with this hash has been inserted */

    newi.store = Talloc(xid, sizeof(lladdHashItem_t)+keylen+datlen);
    newd = malloc(sizeof(lladdHashItem_t)+keylen+datlen);
    newi.keylen = keylen;
    newi.datlen = datlen;
    newi.next = ZERO_RECORDID;
    memcpy(newd, &newi, sizeof(lladdHashItem_t));
    memcpy(newd+sizeof(lladdHashItem_t), key, keylen);
    memcpy(newd+sizeof(lladdHashItem_t)+keylen, dat, datlen);
    writeRecord(xid, newi.store, newd);
    
    ht->hashmap[index] = newi.store;
    /* Tset(xid, ht->store, ht); */
    /*    printf("Writing hashmap slot {%d %d %d}[%d] = {%d,%d,%d}.\n",
	   ht.hashmap_record.page,ht.hashmap_record.slot,ht.hashmap_record.size,
	   index, 
	   ht.hashmap[index].page,ht.hashmap[index].slot,ht.hashmap[index].size); */
    writeRecord(xid, ht->hashmap_record, ht->hashmap);

    free(newd);
    
  } else { 
    
    void *item = NULL;
    
    do {
      
      free(item); /* NULL ignored by free */
      item = malloc(rid.size);
      Tread(xid, rid, item);
      if( ((lladdHashItem_t*)item)->keylen == keylen && !memcmp(key, item+sizeof(lladdHashItem_t), keylen)) {
	memcpy(item+sizeof(lladdHashItem_t)+keylen, dat, ((lladdHashItem_t*)item)->datlen);
	writeRecord(xid, ((lladdHashItem_t*)item)->store, item);
	free(item);
	return 0;
      }
      rid = ((lladdHashItem_t*)item)->next; /* could go off end of list */
    } while( ((lladdHashItem_t*)item)->next.size != 0 );
    /* now item is the tail */
    
    newi.store = Talloc(xid, sizeof(lladdHashItem_t)+keylen+datlen);
    newd = malloc(sizeof(lladdHashItem_t)+keylen+datlen);
    newi.keylen = keylen;
    newi.datlen = datlen;
    newi.next = ZERO_RECORDID;
    memcpy(newd, &newi, sizeof(lladdHashItem_t));
    memcpy(newd+sizeof(lladdHashItem_t), key, keylen);
    memcpy(newd+sizeof(lladdHashItem_t)+keylen, dat, datlen);
    writeRecord(xid, newi.store, newd);

    ((lladdHashItem_t*)item)->next = newi.store;
    writeRecord(xid, ((lladdHashItem_t*)item)->store, item);
    free(item);
    free(newd);
  }
  return 0;
}
/**Todo:  ht->iterData is global to the hash table... seems like a bad idea! */
int lHtPosition( int xid, lladdHash_t *ht, const void *key, size_t key_length ) {
	int index = hash(key, key_length, ht->size);
	
	recordid rid = _getHashMap(xid, ht)[index];
	
	if(rid.size == 0) {
		printf("rid,size = 0\n");
		return -1;
	} else {
	  //void * item = NULL;
	  lladdHashItem_t * item = malloc(rid.size);
	  
	  
	  for(Tread(xid, rid, item) ; 
	      !(item->keylen == key_length && !memcmp(key, ((void*)item)+sizeof(lladdHashItem_t), key_length)) ;
	      rid = item->next) {
	    if(rid.size == 0) {
	      printf("Found bucket, but item not here!\n");
	      return -1;  // Not in hash table.
	    } 
	    free(item);
	    item = malloc(rid.size);
	    Tread(xid, rid, item);					
	  }
	  /* item is what we want.. */
	  ht->iterIndex = index+1; //iterIndex is the index of the next interesting hash bucket.
	  ht->iterData = item;		//Freed in lHtNext
	  return 0;
	}
		
}
int lHtLookup( int xid, lladdHash_t *ht, const void *key, size_t keylen, void *buf ) {

	int index = hash(key, keylen, ht->size);
	recordid rid = _getHashMap(xid, ht)[index];
	/*	printf("lookup: %d -> %d {%d %d %d} \n", *(int*)key, index, rid.page, rid.slot, rid.size); */
	if( rid.size == 0 ) { /* nothing inserted with this hash */
		return -1;
	} else {
		void *item = NULL;
		item = malloc(rid.size);
		Tread(xid, rid, item);

		for( ; !(((lladdHashItem_t*)item)->keylen == keylen && !memcmp(key, item+sizeof(lladdHashItem_t), keylen));
				rid = ((lladdHashItem_t*)item)->next ) {
			if( rid.size == 0) { /* at the end of the list and not found */
				return -1;
			}
			free(item);
			item = malloc(rid.size);
			Tread(xid, rid, item);
		}
		/* rid is what we want */

		memcpy(buf, item+sizeof(lladdHashItem_t)+((lladdHashItem_t*)item)->keylen, ((lladdHashItem_t*)item)->datlen);
		free(item);
		return 0;
	}

	return 0;
}

int _lHtRemove( int xid, recordid garbage, lladdHashRec_t * arg) {

  size_t keylen = arg->keylen;
  void * key = ((void*)arg) + sizeof(lladdHashRec_t);

  lladdHash_t * ht = lladdHashes[arg->ht];

  int index; 
  recordid rid; 

//	printf("Removing %d\n", *(int*)key);

  index = hash(key, keylen, ht->size);
  rid = _getHashMap(xid, ht)[index];
	
	if( rid.size == 0) { /* nothing inserted with this hash */
		return -1;
	} else {
		void *del = malloc(rid.size);
		Tread(xid, rid, del);
		if( ((lladdHashItem_t*)del)->keylen == keylen && !memcmp(key, del+sizeof(lladdHashItem_t), keylen) ) {
			/* the head is the entry to be removed */
		  /*			if( buf ) {
				memcpy( buf, del+sizeof(lladdHashItem_t*)+keylen, ((lladdHashItem_t*)del)->datlen);
				} */
			ht->hashmap[index] = ((lladdHashItem_t*)del)->next;
			/* Tset(xid, ht->store, ht); */
			writeRecord(xid, ht->hashmap_record, ht->hashmap);

			/* TODO: dealloc rid */
			return 0;
		} else {
			void * prevd = NULL;
			while( ((lladdHashItem_t*)del)->next.size ) {
				free(prevd); /* free will ignore NULL args */
				prevd = del;
				rid = ((lladdHashItem_t*)del)->next;
				del = malloc(rid.size);
				Tread(xid, rid, del);
				if( ((lladdHashItem_t*)del)->keylen == keylen && !memcmp(key, del+sizeof(lladdHashItem_t), keylen) ) {
				  /*					if( buf ) {
						memcpy( buf, del+sizeof(lladdHashItem_t)+keylen, ((lladdHashItem_t*)del)->datlen);
						}  */
					((lladdHashItem_t*)prevd)->next = ((lladdHashItem_t*)del)->next;
					writeRecord(xid, ((lladdHashItem_t*)prevd)->store, prevd);
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

int lHtFirst( int xid, lladdHash_t *ht, void *buf ) {

	ht->iterIndex = 0;
	ht->iterData = NULL;
	return lHtNext( xid, ht, buf);
}

int lHtNext( int xid, lladdHash_t *ht, void *buf ) {
  _getHashMap(xid, ht);
	if( ht->iterData && (((lladdHashItem_t*)(ht->iterData))->next.size != 0) ) {
		recordid next = ((lladdHashItem_t*)(ht->iterData))->next;
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
	
	return lHtCurrent(xid, ht, buf);
}

int lHtCurrent(int xid, lladdHash_t *ht, void *buf) {

	if( ht->iterData ) {
		if(buf)
			memcpy(buf, ht->iterData + sizeof(lladdHashItem_t) + ((lladdHashItem_t*)(ht->iterData))->keylen, ((lladdHashItem_t*)(ht->iterData))->datlen);
		return 0;
	}
	return -1;
}


int lHtCurrentKey(int xid, lladdHash_t *ht, void *buf) {

	if( ht->iterData ) {
		memcpy(buf, ht->iterData + sizeof(lladdHashItem_t), ((lladdHashItem_t*)(ht->iterData))->keylen);
		return 0;
	}
	return -1;
}

int lHtDelete(int xid, lladdHash_t *ht) {

	/* deralloc ht->store */

        if(ht->hashmap) { free(ht->hashmap); }    
        free(ht);

	return 0;
}


int lHtInsert(int xid, lladdHash_t *ht, const void *key, size_t keylen, void *dat, size_t datlen) {
  recordid rid;
  void * log_r;
  lladdHashRec_t lir;
  rid.page = 0;
  rid.slot = 0;
  rid.size = sizeof(lladdHashRec_t) + keylen + datlen;

  lir.ht = ht->store;
  lir.keylen = keylen;
  lir.datlen = datlen;

  log_r = malloc(rid.size);
  memcpy(log_r, &lir, sizeof(lladdHashRec_t));
  memcpy(log_r+sizeof(lladdHashRec_t), key, keylen);
  memcpy(log_r+sizeof(lladdHashRec_t)+keylen, dat, datlen);

  /*  printf("Tupdating: %d -> %s\n", *(int*)key, dat); */

  Tupdate(xid,rid,log_r, OPERATION_LHINSERT);
  return 0;
  
}
int lHtRemove( int xid, lladdHash_t *ht, const void *key, size_t keylen, void *buf, size_t buflen ) {

  recordid rid;
  void * log_r;
  lladdHashRec_t lrr;
  int ret = lHtLookup(xid, ht, key, keylen, buf);
	
/*	printf("Looked up: %d\n", *(int*)buf); */
	
  if(ret >= 0) {
    rid.page = 0;
    rid.slot = 0;
    rid.size = sizeof(lladdHashRec_t) + keylen + buflen;
    
    lrr.ht = ht->store;
    lrr.keylen = keylen;
	lrr.datlen = buflen;
	  
    log_r = malloc(sizeof(lladdHashRec_t) + keylen + buflen);
    memcpy(log_r, &lrr, sizeof(lladdHashRec_t));
    memcpy(log_r+sizeof(lladdHashRec_t), key, keylen);
    memcpy(log_r+sizeof(lladdHashRec_t)+keylen, buf, buflen);

    lrr.datlen = buflen;

    Tupdate(xid,rid,log_r, OPERATION_LHREMOVE);
	  
    free (log_r);
  }
  return ret;
}

Operation getLHInsert() {
  Operation o = {
    OPERATION_LHINSERT,
    SIZEOF_RECORD, /* use the size of the record as size of arg (nasty, ugly evil hack, since we end up passing in record = {0, 0, sizeof() */
    OPERATION_LHREMOVE,
    (Function)&_lHtInsert
  };
  return o;

}

Operation getLHRemove() {
  Operation o = {
    OPERATION_LHREMOVE,
    SIZEOF_RECORD, /* use the size of the record as size of arg (nasty, ugly evil hack.) */
    OPERATION_LHINSERT,
    (Function)&_lHtRemove
  };
  return o;

}
