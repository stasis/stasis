#include <lladd/transactional.h>

#ifndef __CONSUMER_H
#define __CONSUMER_H

#define MAX_CONSUMER_TYPES  10
#define FIFO_CONSUMER        0
#define ARRAY_CONSUMER       1
#define LOG_MEMORY_CONSUMER  2
#define POINTER_CONSUMER     3

typedef struct {
  int        type;
  void *     impl;
} lladdConsumer_t;


typedef struct {
  int (*push)(int xid, void * it, byte * key, size_t keySize, byte * val, size_t valSize);
  void(*close)(int xid, void *it);
} lladdConsumer_def_t;

/* call once per Tinit() call */
void consumer_init();

void Tconsumer_close(int xid, lladdConsumer_t * it);
/**
   
   @param xid Transaction id @param it  The consumer
   @param key Can be null if there is no key.
   @param value Can be null if there is no value, but both can't be null.  (Or can they???)
   
   @return Error.  Blocks when full.

*/
int Tconsumer_push(int xid, lladdConsumer_t * it, byte * key, size_t keySize, byte * val, size_t valSize);

/* @see Tconsumer_push
   @return Error, or 'consumer full'
*/
//int Tconsumer_tryPush(int xid, ....);

#endif // __CONSUMER_H

