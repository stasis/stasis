#include <lladd/transactional.h>

#ifndef __ITERATOR_H
#define __ITERATOR_H

#define MAX_ITERATOR_TYPES 10
#define LINEAR_HASH_NTA_ITERATOR 0
#define ARRAY_ITERATOR           1

typedef struct { 
  //  void * new(void * arg);
  void (*close)(int xid, void * it);
  int  (*next) (int xid, void * it);
  int  (*key)  (int xid, void * it, byte ** key);
  int  (*value)(int xid, void * it, byte ** value);
} lladdIterator_def_t;

typedef struct { 
  int     type;
  void *  impl;
} lladdIterator_t;

void iterator_init();

//void lladdIterator_register(int type, lladdIterator_def_t info);

//lladdIterator_t Titerator(int type, void * arg);

void Titerator_close(int xid, lladdIterator_t * it);

/** 
    @param it the iterator 

    @return 1 if the iterator position could advance, or 0 at end of iterator. 

    @throw stanard lladd error values.

*/
int Titerator_next(int xid, lladdIterator_t * it);

/** 
    This function allows the caller to access the current iterator
    position.  When an iterator is initialized, it is in the 'null'
    position.  Therefore, this call may not be made until
    lladdIterator_next has been called.  Calling this function after
    lladdIterator_next has returned zero, or raised a compensation
    error will have undefined results.  

    Iterator support for concurrent modification is implementation
    specfic and optional.

    @param it the iterator

    @param key a pointer to the current key of the iterator.  This 
               memory is managed by the iterator implementation.

    @return the size of the value stored in key, or -1 if the
            iterator's backing store does not have a concept of keys.
            (-1 is used to distinguish 'empty key' from 'no key')

    @throw standard lladd error values
    
    @see lladdIterator_value
*/
int Titerator_key(int xid, lladdIterator_t * it, byte ** key);
/** 
    Analagour to lladdIterator_key.

    @see lladdIterator_key.
*/
int Titerator_value(int xid, lladdIterator_t * it, byte ** value);

#endif

