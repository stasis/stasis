#include <stasis/iterator.h>
#include <assert.h>
#include <stdlib.h>

#include <stasis/operations.h>
#include <stasis/arrayCollection.h>
#include <stasis/logger/logMemory.h>
#include <stasis/operations/group.h>

static lladdIterator_def_t iterators[MAX_ITERATOR_TYPES];

void lladdIterator_register(int type, lladdIterator_def_t info) {
  assert(type < MAX_ITERATOR_TYPES);
  iterators[type] = info;
}
static void noopTupDone(int xid, void * foo) { }

void iterator_init() {
  lladdIterator_def_t array_def = {
    arrayIterator_close,
    arrayIterator_next,
    arrayIterator_next,
    arrayIterator_key,
    arrayIterator_value,
    noopTupDone,
  };
  lladdIterator_register(ARRAY_ITERATOR, array_def);
  lladdIterator_def_t logMemory_def = {
    logMemory_Iterator_close,
    logMemory_Iterator_next,
    logMemory_Iterator_tryNext,
    logMemory_Iterator_key,
    logMemory_Iterator_value,
    logMemory_Iterator_releaseTuple,
  };
  lladdIterator_register(LOG_MEMORY_ITERATOR, logMemory_def);
  lladdIterator_def_t pointer_def = {
    lladdFifoPool_iterator_close,
    lladdFifoPool_iterator_next,
    lladdFifoPool_iterator_tryNext,
    lladdFifoPool_iterator_key,
    lladdFifoPool_iterator_value,
    lladdFifoPool_iterator_tupleDone,
  };
  lladdIterator_register(POINTER_ITERATOR, pointer_def);

  stasis_log_structured_group_init();
}


//lladdIterator_t Titerator(int type, void * arg);
void Titerator_close  (int xid, lladdIterator_t * it)             {        iterators[it->type].close(xid, it->impl); free(it); }
int  Titerator_next   (int xid, lladdIterator_t * it)             { return iterators[it->type].next (xid, it->impl);           }
int  Titerator_tryNext(int xid, lladdIterator_t * it)             { return iterators[it->type].tryNext (xid, it->impl);        }
int  Titerator_key    (int xid, lladdIterator_t * it, byte ** key){ return iterators[it->type].key  (xid, it->impl, key);      }
int  Titerator_value(int xid, lladdIterator_t * it, byte ** value){ return iterators[it->type].value(xid, it->impl, value);    }
void Titerator_tupleDone(int xid, lladdIterator_t * it)           {        iterators[it->type].tupleDone(xid, it->impl);       }
