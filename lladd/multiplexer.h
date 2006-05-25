#include "iterator.h"
#include "consumer.h"
#include "fifo.h"

#include <pbl/pbl.h>

void multiplexHashLogByKey(byte * key,
			   size_t keySize, 
			   byte * value, 
			   size_t valueSize, 
			   byte ** multiplexKey,
			   size_t * multiplexKeySize);

typedef struct  { 
  lladdIterator_t * it;
  void (*multiplexer)(byte * key, 
		      size_t keySize, 
		      byte * value, 
		      size_t valueSize, 
		      byte ** multiplexKey, 
		      size_t * multiplexKeySize);

  /** A hash of consumer implementations, keyed on the output of the multiplexKey parameter of *multiplex */
  pblHashTable_t * consumerHash;
  /** The next two fields are used to create new consumers on demand. */
  /*  lladdConsumer_t * (*getConsumer)(struct lladdFifoPool_t *  newConsumerArg,
				   byte*  multiplexKey, 
				   size_t multiplexKeySize); */
  lladdFifoPool_t * fifoPool;
  pthread_t worker;
  int xid;
} lladdMultiplexer_t;

void * lladdMultiplexer_flush(lladdMultiplexer_t * m) ;

lladdMultiplexer_t * lladdMultiplexer_alloc(int xid, lladdIterator_t * it, 
					    void (*multiplexer)(byte * key,
							      size_t keySize, 
							      byte * value, 
							      size_t valueSize, 
							      byte ** multiplexKey,
							      size_t * multiplexKeySize),
					    /*					    lladdConsumer_t * getConsumer(lladdFifoPool_t * fifoPool,
									  byte* multiplexKey, 
									  size_t multiplexKeySize), */
					    lladdFifoPool_t * fifoPool);

/** 
    creates a new thread that will consume input from it, and forward
    its output to the consumers.  

    @param thread_attributes passed through to pthread_create, it is
    fine if this is NULL, although it probably makes sense to set the
    stack size to something reasonable (PTHREAD_STACK_MIN will
    probably work.  LLADD is tested with 16K stacks under linux/x86
    (where 16K = PTHREAD_STACK_MIN) , while the default pthread stack
    size is 2M.  Your milage may vary.)

    @return zero on success, or error code (@see pthread_create for
    possible return values) on failure.
*/
int lladdMultiplexer_start(lladdMultiplexer_t * multiplexer, pthread_attr_t * thread_attributes);

/**
    block the current thread until the multiplexer shuts down.

    @todo lladdMultiplex_join does not propagate compensation errors as it should.
 */
int lladdMultiplexer_join(lladdMultiplexer_t * multiplexer);

void multiplexByValue(byte * key, size_t keySize, byte * value, size_t valueSize, byte **multiplexKey, size_t * multiplexSize);
void multiplexByRidPage(byte * key, size_t keySize, byte * value, size_t valueSize, byte **multiplexKey, size_t * multiplexSize);
