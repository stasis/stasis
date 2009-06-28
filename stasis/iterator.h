#include <stasis/transactional.h>

#ifndef __ITERATOR_H
#define __ITERATOR_H

BEGIN_C_DECLS

typedef struct {
  void (*close)(int xid, void * it);
  int  (*next) (int xid, void * it);
  int  (*tryNext) (int xid, void * it);
  int  (*key)  (int xid, void * it, byte ** key);
  int  (*value)(int xid, void * it, byte ** value);
  void (*tupleDone)(int xid, void * it);
} lladdIterator_def_t;

typedef struct {
  int     type;
  void *  impl;
} lladdIterator_t;

void iterator_init();

void lladdIterator_register(int type, lladdIterator_def_t info);

void Titerator_close(int xid, lladdIterator_t * it);

/**
   Advance the iterator by one position.  This should be called before
   you attempt to read from the iterator, and can block if the
   iterator is currently locked.

    @param xid transaction id

    @param it the iterator

    @return 1 if the iterator position could advance, or 0 at end of iterator.
*/
int Titerator_next(int xid, lladdIterator_t * it);

/**
   Attempt to advance the iterator by one position.  This function
   will return immediately, even if the iterator is locked (in such
   cases, the call has no effect).

   @param xid transaction id

   @param it the iterator

   @return 1 if the iterator position advanced, and releaseTuple must be called,
           0 if the iterator has been locked by another reader, no tuples are ready, or the iterator has been closed.

   @todo think more carefully about the return value of Titerator_tryNext().  I'm not convinced that a 0/1
         return value is adequate.
*/
int Titerator_tryNext(int xid, lladdIterator_t * it);

/**
    This function allows the caller to access the current iterator
    position.  When an iterator is initialized, it is in the 'null'
    position.  Therefore, this call may not be made until
    lladdIterator_next has been called.  Calling this function after
    lladdIterator_next has returned zero, or raised a compensation
    error will have undefined results.

    Iterator support for concurrent modification is implementation
    specific and optional.

    @param xid transaction id
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
    Analogous to lladdIterator_key.

    @see lladdIterator_key.
*/
int Titerator_value(int xid, lladdIterator_t * it, byte ** value);
/**
    Iterator callers must call this before calling next().  A separate
    call is required so that iterators can be reentrant. (Warning: Not
    all iterators are reentrant.)
*/
void Titerator_tupleDone(int xid, lladdIterator_t * it);

END_C_DECLS

#endif
