#include <lladd/multiplexer.h>
#include <lladd/crc32.h>
#include <stdlib.h>
#include <lladd/operations/linearHashNTA.h>
lladdMultiplexer_t * lladdMultiplexer_alloc(int xid, lladdIterator_t * it, 
					    void (*multiplexer)(byte * key,
							      size_t keySize, 
							      byte * value, 
							      size_t valueSize, 
							      byte ** multiplexKey,
							      size_t * multiplexKeySize),
					    lladdConsumer_t * getConsumer(struct lladdFifoPool_t* getConsumerArg,
									  byte* multiplexKey, 
									  size_t multiplexKeySize),
					    lladdFifoPool_t * fifoPool) {
  lladdMultiplexer_t * ret = malloc(sizeof(lladdMultiplexer_t));
  ret->it = it;
  ret->multiplexer = multiplexer;
  ret->consumerHash = pblHtCreate();
  ret->getConsumer  = getConsumer;
  ret->getConsumerArg = fifoPool;
  ret->xid = xid;
  return ret;
}

void * multiplexer_worker(void * arg);

int lladdMultiplexer_start(lladdMultiplexer_t * multiplexer, pthread_attr_t * thread_attributes) {
  return pthread_create(&multiplexer->worker, thread_attributes, multiplexer_worker, multiplexer);
}


int lladdMultiplexer_join(lladdMultiplexer_t * multiplexer) {
  return pthread_join(multiplexer->worker,NULL);
}


void * multiplexer_worker(void * arg) { 
  lladdMultiplexer_t * m = arg;
  
  while(Titerator_next(m->xid, m->it)) {
    byte * mkey, * key, * value;
    size_t mkeySize, keySize, valueSize;
    
    keySize   = Titerator_key  (m->xid, m->it, &key);
    valueSize = Titerator_value(m->xid, m->it, &value);

    m->multiplexer(key, keySize, value, valueSize, &mkey, &mkeySize);

    lladdConsumer_t * consumer = m->getConsumer(m->getConsumerArg, mkey, mkeySize);

    /*   lladdConsumer_t * consumer = pblHtLookup(m->consumerHash);
    if(consumer == NULL) {
      consumer = m->newConsumer(m->newConsumerArg, mkey, mkeySize);
      pblHtInsert(m->consumerHash, mkey, mkeySize, consumer);
      } */
    
    Tconsumer_push(m->xid, consumer, key, keySize, value, valueSize);
    
  }
  
  // iterate over pblhash, closing consumers.

  Titerator_close(m->xid, m->it);

  return (void*)compensation_error();
}

/* ******************  END OF MULTIXPLEXER IMPLEMENTATION **************

  Sample callbacks follow.

*********************************************************************/
// @todo remove the code until the //-----, as it just makes up for code that Jimmy needs to commit!


lladdFifo_t * logMemory_init(int bufferSize, int initialLSN);

//---------



void multiplexHashLogByKey(byte * key,
			   size_t keySize, 
			   byte * value, 
			   size_t valueSize, 
			   byte ** multiplexKey,
			   size_t * multiplexKeySize) {
  // We don't care what the key is.  It's probably an LSN.
  const LogEntry * log = (const LogEntry*) value;
  const byte * updateArgs = getUpdateArgs(log);  // assume the log is a logical update entry.
  switch(log->contents.update.funcID) {

    // If you really want to know why insert takes
    // linearHash_remove_arg entries and vice versa, look at
    // linearHashNTA.  Note that during normal (physiological forward)
    // operation, ThashInsert() *generates* insert args for its undo
    // implementation, ThashRemove() and vice versa.  Therefore,
    // ThashRemove's operation implementation takes an insert
    // argument.

  case OPERATION_LINEAR_HASH_INSERT:
    {
      linearHash_remove_arg * arg = (linearHash_remove_arg*) updateArgs;  // this *is* correct.  Don't ask...
      *multiplexKey = (byte*) (arg+1);
      *multiplexKeySize = arg->keySize;
    }
  case OPERATION_LINEAR_HASH_REMOVE:
    {
      linearHash_insert_arg * arg = (linearHash_insert_arg*)updateArgs;  // this *is* correct.  Don't ask.... 
      *multiplexKey = (byte*) (arg + 1);
      *multiplexKeySize = arg->keySize;
    }
  default:
    abort();
  }
}

/** 
    Obtain a member of a fifoPool based on the value of multiplexKey.  Use CRC32 to assign the key to a consumer. */
lladdConsumer_t * fifoPool_getConsumerCRC32( lladdFifoPool_t * pool, byte * multiplexKey, size_t multiplexKeySize) {
  int memberId =  crc32(multiplexKey, multiplexKeySize, (unsigned long)-1L) % pool->fifoCount;
  return pool->pool[memberId]->consumer;
}

/**
   Create a new pool of ringBuffer based fifos

   @param consumerCount the number of consumers in the pool.
   @todo this function should be generalized to other consumer implementations.
*/
lladdFifoPool_t * fifoPool_ringBufferInit (int consumerCount, int bufferSize) {
  lladdFifoPool_t * pool = malloc(sizeof(lladdFifoPool_t));

  pool->pool = malloc(sizeof(lladdFifo_t*) * consumerCount);
  int i;
  for(i = 0; i < consumerCount; i++) {
    pool->pool[i] = logMemory_init(bufferSize, 0);
  }
  return pool;
}
