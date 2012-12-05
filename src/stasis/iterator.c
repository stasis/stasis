#include <stasis/iterator.h>
#include <stasis/operations.h>
//#include <stasis/arrayCollection.h>
#include <stasis/experimental/group.h>

#include <assert.h>

static lladdIterator_def_t iterators[MAX_ITERATOR_TYPES];

void lladdIterator_register(int type, lladdIterator_def_t info) {
  assert(type < MAX_ITERATOR_TYPES);
  iterators[type] = info;
}

void iterator_init(void) {
  /* no-op */
}


//lladdIterator_t Titerator(int type, void * arg);
void Titerator_close  (int xid, lladdIterator_t * it)             {        iterators[it->type].close(xid, it->impl); free(it); }
int  Titerator_next   (int xid, lladdIterator_t * it)             { return iterators[it->type].next (xid, it->impl);           }
int  Titerator_tryNext(int xid, lladdIterator_t * it)             { return iterators[it->type].tryNext (xid, it->impl);        }
int  Titerator_key    (int xid, lladdIterator_t * it, byte ** key){ return iterators[it->type].key  (xid, it->impl, key);      }
int  Titerator_value(int xid, lladdIterator_t * it, byte ** value){ return iterators[it->type].value(xid, it->impl, value);    }
void Titerator_tupleDone(int xid, lladdIterator_t * it)           {        iterators[it->type].tupleDone(xid, it->impl);       }
