#include <stasis/common.h>
#include <stasis/transactional.h>
typedef struct {
  byte * keyArray;
  byte * valueArray;
  unsigned int element;
  unsigned int keySize;
  unsigned int valueSize;
  unsigned int elementCount;
} array_iterator_t;


lladdIterator_t * arrayIterator(byte * keyArray, unsigned int keySize,
				byte * valueArray, unsigned int valueSize,
				unsigned int elementCount) {
  lladdIterator_t * ret = malloc(sizeof(lladdIterator_t));
  ret->type = ARRAY_ITERATOR;
  array_iterator_t * it = ret->impl = malloc(sizeof(array_iterator_t));

  it->keyArray = keyArray;
  it->valueArray = valueArray;

  it->element = -1;

  it->valueSize = valueSize;
  it->keySize = keySize;
  it->elementCount = elementCount;

  return ret;
}

void arrayIterator_close(int xid, void * impl) {
  free(impl);
}

int  arrayIterator_next (int xid, void * impl) {
  array_iterator_t * it = (array_iterator_t *) impl;
  it->element++;
  return it->element < it->elementCount;
}
int  arrayIterator_key  (int xid, void * impl, byte ** key) {
  array_iterator_t * it = (array_iterator_t *) impl;
  //  unsigned int offset = it->element * (it->valueSize + it->keySize);
  *key =  &(it->keyArray[it->element * it->keySize]);
  // printf("elt = %d, s = %d, k = %x\n", it->element, it->keySize, *key);
  return it->keySize;
}
int  arrayIterator_value(int xid, void * impl, byte ** value) {
  array_iterator_t * it = (array_iterator_t *) impl;
  //  unsigned int offset = it->element * (it->valueSize + it->keySize);
  *value =  &(it->valueArray[it->element * it->valueSize]);
  // printf("elt = %d, s = %d, v = %x\n", it->element, it->valueSize, *value);
  return it->valueSize;
}


typedef struct {
  byte * array;
  int element;
  int elementSize;
  int elementCount;
} pointer_array_iterator_t;

static void noopTupDone(int xid, void * foo) { }

void stasis_arrayCollection_init(void) {
  lladdIterator_def_t array_def = {
    arrayIterator_close,
    arrayIterator_next,
    arrayIterator_next,
    arrayIterator_key,
    arrayIterator_value,
    noopTupDone,
  };
  lladdIterator_register(ARRAY_ITERATOR, array_def);
}
