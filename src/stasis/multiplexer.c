#include <stasis/multiplexer.h>
#include <stasis/crc32.h>
#include <stasis/operations/linearHashNTA.h>

#include <stasis/logger/logMemory.h>

lladdMultiplexer_t * lladdMultiplexer_alloc(int xid, lladdIterator_t * it,
					    void (*multiplexer)(byte * key,
							      size_t keySize,
							      byte * value,
							      size_t valueSize,
							      byte ** multiplexKey,
							      size_t * multiplexKeySize),
					    /*		    lladdConsumer_t * getConsumer(struct lladdFifoPool_t* fifoPool,
									  byte* multiplexKey,
									  size_t multiplexKeySize), */
					    lladdFifoPool_t * fifoPool) {
  lladdMultiplexer_t * ret = malloc(sizeof(lladdMultiplexer_t));
  ret->it = it;
  ret->multiplexer = multiplexer;
  ret->consumerHash = pblHtCreate();
  //  ret->getConsumer  = getConsumer;
  ret->fifoPool = fifoPool;
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

void * lladdMultiplexer_flush(lladdMultiplexer_t * m) {
  //  lladdMultiplexer_t * m = arg;
  lladdConsumer_t * consumer;

  while(Titerator_tryNext(m->xid, m->it)) {
    byte * mkey, * key, * value;
    size_t mkeySize, keySize, valueSize;

    keySize   = Titerator_key  (m->xid, m->it, &key);
    valueSize = Titerator_value(m->xid, m->it, &value);

    m->multiplexer(key, keySize, value, valueSize, &mkey, &mkeySize);

    lladdFifo_t * fifo = m->fifoPool->getFifo(m->fifoPool, mkey, mkeySize);
    consumer = fifo->consumer;
    Tconsumer_push(m->xid, consumer, key, keySize, value, valueSize);
    Titerator_tupleDone(m->xid, m->it);
    lladdFifoPool_markDirty(m->xid, m->fifoPool, fifo);
  }

  // iterate over pblhash, closing consumers.

  /*  Titerator_close(m->xid, m->it);

  // @todo Does this belong in its own function in fifo.c?

  lladdFifoPool_t * pool = m->fifoPool;
  int i;
  for(i = 0; i < pool->fifoCount; i++) {
    Tconsumer_close(m->xid, pool->pool[i]->consumer);
  }

  if(m->fifoPool->dirtyPoolFifo) {
    Tconsumer_close(m->xid, m->fifoPool->dirtyPoolFifo->consumer);
  }
  */
  return (void*)0;
}


void * multiplexer_worker(void * arg) {
  lladdMultiplexer_t * m = arg;
  lladdConsumer_t * consumer;

  while(Titerator_next(m->xid, m->it)) {
    byte * mkey, * key, * value;
    size_t mkeySize, keySize, valueSize;

    keySize   = Titerator_key  (m->xid, m->it, &key);
    valueSize = Titerator_value(m->xid, m->it, &value);

    m->multiplexer(key, keySize, value, valueSize, &mkey, &mkeySize);

    lladdFifo_t * fifo = m->fifoPool->getFifo(m->fifoPool, mkey, mkeySize);
    consumer = fifo->consumer;
    Tconsumer_push(m->xid, consumer, key, keySize, value, valueSize);
    Titerator_tupleDone(m->xid, m->it);
    lladdFifoPool_markDirty(m->xid, m->fifoPool, fifo);
  }

  // iterate over pblhash, closing consumers.

  Titerator_close(m->xid, m->it);

  /** @todo Does this belong in its own function in fifo.c? */

  lladdFifoPool_t * pool = m->fifoPool;
  int i;
  for(i = 0; i < pool->fifoCount; i++) {
    Tconsumer_close(m->xid, pool->pool[i]->consumer);
  }

  if(m->fifoPool->dirtyPoolFifo) {
    Tconsumer_close(m->xid, m->fifoPool->dirtyPoolFifo->consumer);
  }

  return (void*)0;
}

/* ******************  END OF MULTIXPLEXER IMPLEMENTATION **************

  Sample callbacks follow.

*/
void multiplexHashLogByKey(byte * key,
			   size_t keySize,
			   byte * value,
			   size_t valueSize,
			   byte ** multiplexKey,
			   size_t * multiplexKeySize) {
  // We don't care what the key is.  It's probably an LSN.
  const LogEntry * log = (const LogEntry*) value;
  const byte * updateArgs = stasis_log_entry_update_args_cptr(log);  // assume the log is a logical update entry.
  switch(log->update.funcID) {

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
    break;
  case OPERATION_LINEAR_HASH_REMOVE:
    {
      linearHash_insert_arg * arg = (linearHash_insert_arg*)updateArgs;  // this *is* correct.  Don't ask....
      *multiplexKey = (byte*) (arg + 1);
      *multiplexKeySize = arg->keySize;
    }
    break;
  default:
    abort();
  }
}


void multiplexByValue(byte * key, size_t keySize, byte * value, size_t valueSize, byte **multiplexKey, size_t * multiplexSize) {
  *multiplexKey = value;
  *multiplexSize = valueSize;
}

void multiplexByRidPage(byte * key, size_t keySize, byte * value, size_t valueSize, byte **multiplexKey, size_t * multiplexSize) {
  *multiplexKey = (byte*)&(((recordid*)value)->page);
  *multiplexSize = sizeof(((recordid*)value)->page);
}
