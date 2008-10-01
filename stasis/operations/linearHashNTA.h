/**
    @file

    A reliable hashtable implementation.  The implementation makes
    use of nested top actions, and is reentrant.

    The implementation uses a linear hash function, allowing the
    bucket list to be resized dynamically.  Because the bucket list is
    built on top of arrayList, all operations are O(1), assuming the
    hash function behaves correctly.  Currently, linkedListNTA
    implements the bucket lists.

    @see nestedTopAction.h, linkedListNTA.h, arrayList.h

    @ingroup OPERATIONS

    $id$

*/

#include <stasis/iterator.h>

#ifndef __LINEAR_HASH_NTA_H
#define __LINEAR_HASH_NTA_H
/** Currently, only used in the type field of the iterators. */
#define FIXED_LENGTH_HASH 0
#define VARIABLE_LENGTH_HASH 1

/** Pass this into the keySize and/or valueSize parameter of the
    constructor below if the hashtable should support variable length
    keys and/or values, respectively. */
#define VARIABLE_LENGTH -1

typedef struct {
  recordid hashHeader;
  recordid bucket;
  int numBuckets;
  int keySize;
  int valueSize;
  stasis_linkedList_iterator * it;
  lladd_pagedList_iterator * pit;
} lladd_hash_iterator;

compensated_function recordid ThashCreate(int xid, int keySize, int valSize);
compensated_function void ThashDelete(int xid, recordid hash);
/* @return 1 if the key was defined, 0 otherwise. */
compensated_function int ThashInsert(int xid, recordid hash, const byte* key, int keySize, const byte* value, int valueSize);
/* @return 1 if the key was defined, 0 otherwise. */
compensated_function int ThashRemove(int xid, recordid hash, const byte* key, int keySize);

/** @return size of the value associated with key, or -1 if key not found. 
                   (a return value of zero means the key is associated with an
		   empty value.) */
compensated_function int ThashLookup(int xid, recordid hash, const byte* key, int keySize, byte ** value);
/** 
  Allocate a new hash iterator.  This API is designed to eventually be 
  overloaded, and is subject to change.  If the iterator is run to completion, 
  it is automatically freed.  Otherwise, it should be manually freed with free(). 
  @param xid transaction id
  @param hash the recordid returned by ThashAlloc
  @param keySize the same as the value passed into ThashAlloc.  
  @param valueSize the same as the value passed into ThashAlloc
  @deprecated  @see interator.h.  Use the linearHash implementation of that interface instead.
*/
lladd_hash_iterator * ThashIterator(int xid, recordid hash, int keySize, int valueSize);
/**
  Obtain the next value in the hash table.  
  
  @return 1 if another value exists; 0 if the iterator is done, and has been deallocated.

  @param xid Transaction id
  @param it The iterator that will be traversed.  @see ThashIterator().
  @param key a pointer to an uninitialized pointer value.  If another entry is 
         encountered, then the uninitialized pointer value will be set to point 
         to a malloc()'ed region of memory that contains the value's key.  This 
         region of memory should be manually free()'ed by the application.  LLADD
         normally leaves memory management to the application.  However, once 
         hashes with variable size entries are supported, it would be extremely 
         difficult for the application to malloc an appropriate buffer for the 
	 iterator, so this function call does not obey normal LLADD calling 
	 semantics.
  @param keySize The address of a valid integer.  It's initial value is ignored, 
         and will be overwritten by the length of the key.
  @param value analagous to value.
  @param valueSize analagous to keySize

  @deprecated  Use iterator.h's linearHash implementation instead.
*/
int ThashNext(int xid, lladd_hash_iterator * it, byte ** key, int * keySize, byte** value, int * valueSize);


/** Free the hash iterator and its associated resources. 
  @deprecated  @see interator.h.  Use the linearHash implementation of that interface instead.

*/
void ThashDone(int xid, lladd_hash_iterator * it);

Operation getLinearHashInsert();
Operation getLinearHashRemove();

void LinearHashNTAInit();

/** Iterator that complies with the standard LLADD iterator interface. 
    @todo current generic linearHashIterator implemnetation is just slapped on top of old, slow interface.
    @todo rename ThashGenericIterator to ThashIterator, and remove deprecated iterator interface...
*/

//void * linearHashNTAIterator_new  (void * arg);
lladdIterator_t *     ThashGenericIterator       (int xid, recordid hash);
void                  linearHashNTAIterator_close(int xid, void * it);
int                   linearHashNTAIterator_next (int xid, void * it);
int                   linearHashNTAIterator_key  (int xid, void * it, byte **key);
int                   linearHashNTAIterator_value(int xid, void * it, byte **value);

/** @todo these should be in linearHashNTA.c, but they've been moved
    here so that multiplexer.c can (temoprarily) implement a
    multiplexer for logical hash operations. */

typedef struct {
  recordid hashHeader;
  int keySize;
} linearHash_insert_arg;

typedef struct {
  recordid hashHeader;
  int keySize;
  int valueSize;
} linearHash_remove_arg;



//Support 16 entries by default.
#define HASH_INIT_BITS 4
#define HASH_FILL_FACTOR 0.7

#endif // __LINEAR_HASH_NTA_H
