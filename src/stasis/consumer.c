#include <stasis/consumer.h>
#include <assert.h>
#include <stdlib.h>
#include <stasis/logger/logMemory.h>

static lladdConsumer_def_t consumers[MAX_CONSUMER_TYPES];

static void lladdConsumer_register(int type, lladdConsumer_def_t info) {
  assert(type < MAX_CONSUMER_TYPES);
  consumers[type] = info;
}

void consumer_init() { 
  lladdConsumer_def_t logMemory_def = {
    logMemory_consumer_push,
    logMemory_consumer_close
  };
  lladdConsumer_register(LOG_MEMORY_CONSUMER, logMemory_def);
  lladdConsumer_def_t pointer_def = {
    lladdFifoPool_consumer_push,
    lladdFifoPool_consumer_close
  };
  lladdConsumer_register(POINTER_CONSUMER, pointer_def);

}

compensated_function int Tconsumer_push(int xid, lladdConsumer_t *it, byte *key, size_t keySize, byte *val, size_t valSize) {
  return consumers[it->type].push(xid, it->impl, key, keySize, val, valSize);
}
compensated_function void Tconsumer_close(int xid, lladdConsumer_t *it) {
  consumers[it->type].close(xid, it->impl);
}
