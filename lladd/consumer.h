#include <lladd/transactional.h>

#ifndef __CONSUMER_H
#define __ITERATOR_H

#define MAX_ITERATOR_TYPES 10
#define FIFO_CONSUMER       0
#define ARRAY_CONSUMER      1

typedef struct {
  int foo;
} lladdConsumer_def_t;

typedef struct {
  int        type;
  void *     impl;
} lladdConsumer_t;

/* call once per Tinit() call */
void consumer_init();

void Tconsumer_close(int xid, lladdConsumer_t * it);
/**
   
   @param xid Transaction id
   @param it  The consumer
   @param key Can be null if there is no key.
   @param value Can be null if there is no value, but both can't be null.  (Or can they???)
   
   @return Error.  Blocks when full.

*/
int Tconsumer_push(int xid, lladdConsumer_t * it, byte * key, size_t keySize, byte * val, size_t valSize);

/* @see Tconsumer_push
   @return Error, or 'consumer full'
*/
//int Tconsumer_tryPush(int xid, ....);

#endif
