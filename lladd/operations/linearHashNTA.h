/** 
    @file

    A reliable hashtable implementation.  The implementation makes
    use of nested top actions, and is reentrant.  Currently, all keys
    and values must be of the same length, although this restriction
    will eventually be removed.

    The implementation uses a linear hash function, allowing the
    bucket list to be resized dynamically.  Because the bucket list is
    built on top of arrayList, all operations are O(1), assuming the
    hash function behaves correctly.  Currently, linkedListNTA
    implements the bucket lists.

    @see nestedTopAction.h, linkedListNTA.h, arrayList.h

    @ingroup OPERATIONS

    $id$

*/

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
  lladd_linkedList_iterator * it;
  lladd_pagedList_iterator * pit;
} lladd_hash_iterator;

recordid ThashCreate(int xid, int keySize, int valSize);
void ThashDelete(int xid, recordid hash);
/* @return 1 if the key was defined, 0 otherwise. */
int ThashInsert(int xid, recordid hash, const byte* key, int keySize, const byte* value, int valueSize);
/* @return 1 if the key was defined, 0 otherwise. */
int ThashRemove(int xid, recordid hash, const byte* key, int keySize);

/** @return size of the value associated with key, or -1 if key not found. 
                   (a return value of zero means the key is associated with an
		   empty value.) */
int ThashLookup(int xid, recordid hash, const byte* key, int keySize, byte ** value);
/** 
  Allocate a new hash iterator.  This API is designed to eventually be 
  overloaded, and is subject to change.  If the iterator is run to completion, 
  it is automatically freed.  Otherwise, it should be manually freed with free(). 
  @param xid transaction id
  @param hash the recordid returned by ThashAlloc
  @param keySize the same as the value passed into ThashAlloc.  
  @param valueSize the same as the value passed into ThashAlloc
*/
lladd_hash_iterator * ThashIterator(int xid, recordid hash, int keySize, int valueSize);
/**
  Obtain the next value in the hash table.  
  
  @return 1 if another value exists; 0 if the iterator is done, and has been deallocated.
  @param keySize Currently, keySize and valueSize must
  be set to the correct sizes when ThashNext is called.  Once hashtables with 
  variable sized entries are supported, this restriction will be relaxed or removed
  entirely.
  @param key a pointer to an uninitialized pointer value.  If another entry is 
         encountered, then the uninitialized pointer value will be set to point 
         to a malloc()'ed region of memory that contains the value's key.  This 
         region of memory should be manually free()'ed by the application.  LLADD
         normally leaves memory management to the application.  However, once 
         hashes with variable size entries are supported, it would be extremely 
         difficult for the application to malloc an appropriate buffer for the 
	 iterator, so this function call does not obey normal LLADD calling 
	 semantics.
  @param value analagous to value.
  @param valueSize analagous to keySize
*/
int ThashNext(int xid, lladd_hash_iterator * it, byte ** key, int * keySize, byte** value, int * valueSize);


/** Free the hash iterator and its associated resources. */
void ThashDone(int xid, lladd_hash_iterator * it);

Operation getLinearHashInsert();
Operation getLinearHashRemove();

//Support 16 entries by default.
#define HASH_INIT_BITS 4
#define HASH_FILL_FACTOR 0.7

#endif // __LINEAR_HASH_NTA_H
