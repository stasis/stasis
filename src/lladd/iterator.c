#include <lladd/iterator.h>
#include <assert.h>
#include <stdlib.h>

#include <lladd/operations.h>
#include <lladd/arrayCollection.h>
#include "logger/logMemory.h"

static lladdIterator_def_t iterators[MAX_ITERATOR_TYPES];

static void lladdIterator_register(int type, lladdIterator_def_t info) {
  assert(type < MAX_ITERATOR_TYPES);
  iterators[type] = info;
}
static void noopTupDone(int xid, void * foo) { }

void iterator_init() {
  lladdIterator_def_t linearHashNTA_def = {
   linearHashNTAIterator_close, 
   linearHashNTAIterator_next, 
   linearHashNTAIterator_next, 
   linearHashNTAIterator_key, 
   linearHashNTAIterator_value,
   noopTupDone
  };
  lladdIterator_register(LINEAR_HASH_NTA_ITERATOR, linearHashNTA_def);
  lladdIterator_def_t array_def = {
    arrayIterator_close,        
    arrayIterator_next,         
    arrayIterator_next,         
    arrayIterator_key,         
    arrayIterator_value,         
    noopTupDone
  };
  lladdIterator_register(ARRAY_ITERATOR, array_def);
  lladdIterator_def_t logMemory_def = {
    logMemory_Iterator_close,    
    logMemory_Iterator_next,    
    logMemory_Iterator_tryNext,    
    logMemory_Iterator_key,   
    logMemory_Iterator_value,    
    logMemory_Iterator_releaseTuple 
  };
  lladdIterator_register(LOG_MEMORY_ITERATOR, logMemory_def);
}


//lladdIterator_t Titerator(int type, void * arg);
void Titerator_close  (int xid, lladdIterator_t * it)             {        iterators[it->type].close(xid, it->impl); free(it); }
int  Titerator_next   (int xid, lladdIterator_t * it)             { return iterators[it->type].next (xid, it->impl);           }
int  Titerator_tryNext(int xid, lladdIterator_t * it)             { return iterators[it->type].tryNext (xid, it->impl);        }
int  Titerator_key    (int xid, lladdIterator_t * it, byte ** key){ return iterators[it->type].key  (xid, it->impl, key);      }
int  Titerator_value(int xid, lladdIterator_t * it, byte ** value){ return iterators[it->type].value(xid, it->impl, value);    }
void Titerator_tupleDone(int xid, lladdIterator_t * it)           {        iterators[it->type].tupleDone(xid, it->impl);       }
