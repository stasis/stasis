/**
   @file 

   Implements a linked list using nested top actions.  Linked list
   entries are key, value pairs, where the keys and values are of
   fixed length.

   @see nestedTopAction.h

   @ingroup OPERATIONS

   $id$
*/

#ifndef __LINKED_LIST_NTA_H
#define __LINKED_LIST_NTA_H
typedef struct {
  recordid next;
} stasis_linkedList_entry;
typedef struct {
  int keySize;
  int valueSize;
  recordid next;
  /** The implementation of TlinkedListRemove always preserves 
      the location of the head of the linked list.  Therefore, 
      if the first entry is removed, *and* the iterator just returned
      the head of the list, then the iterator needs to reset itself. */
  int first;
  recordid listRoot;
} stasis_linkedList_iterator;

int TlinkedListInsert(int xid, recordid list, const byte * key, int keySize, const byte * value, int valueSize);
int TlinkedListFind(int xid, recordid list, const byte * key, int keySize, byte ** value);
int TlinkedListRemove(int xid, recordid list, const byte * key, int keySize);
int TlinkedListMove(int xid, recordid start_list, recordid end_list, const byte *key, int keySize);
/** The linked list iterator can tolerate the concurrent removal of values that 
    it has already returned.  In the presence of such removals, the iterator 
    will return the keys and values present in the list as it existed when next()
    was first called.

    @return a new iterator initialized to the head of the list.  */
stasis_linkedList_iterator * TlinkedListIterator(int xid, recordid list, int keySize, int valueSize);
void TlinkedListClose(int xid, stasis_linkedList_iterator * it);
/** @return 1 if there was another entry to be iterated over. 0 otherwise.  
     If this function returns 1, the caller must free() the malloced memory 
     returned via the key and value arguments.*/
int TlinkedListNext(int xid, stasis_linkedList_iterator * it, byte ** key, int * keySize, byte ** value, int * valueSize);
recordid TlinkedListCreate(int xid, int keySize, int ValueSize);
void TlinkedListDelete(int xid, recordid list);

void TlinkedListNTAInit();
void TlinkedListNTADeinit();
stasis_operation_impl stasis_op_impl_linked_list_insert();
stasis_operation_impl stasis_op_impl_linked_list_remove();
#endif //__LINKED_LIST_NTA_H
