/*---
This software is copyrighted by the Regents of the University of
California, and other parties. The following terms apply to all files
associated with the software unless explicitly disclaimed in
individual files.

The authors hereby grant permission to use, copy, modify, distribute,
and license this software and its documentation for any purpose,
provided that existing copyright notices are retained in all copies
and that this notice is included verbatim in any distributions. No
written agreement, license, or royalty fee is required for any of the
authorized uses. Modifications to this software may be copyrighted by
their authors and need not follow the licensing terms described here,
provided that the new terms are clearly indicated on the first page of
each file where they apply.

IN NO EVENT SHALL THE AUTHORS OR DISTRIBUTORS BE LIABLE TO ANY PARTY
FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES
ARISING OUT OF THE USE OF THIS SOFTWARE, ITS DOCUMENTATION, OR ANY
DERIVATIVES THEREOF, EVEN IF THE AUTHORS HAVE BEEN ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

THE AUTHORS AND DISTRIBUTORS SPECIFICALLY DISCLAIM ANY WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
NON-INFRINGEMENT. THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, AND
THE AUTHORS AND DISTRIBUTORS HAVE NO OBLIGATION TO PROVIDE
MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.

GOVERNMENT USE: If you are acquiring this software on behalf of the
U.S. government, the Government shall have only "Restricted Rights" in
the software and related documentation as defined in the Federal
Acquisition Regulations (FARs) in Clause 52.227.19 (c) (2). If you are
acquiring the software on behalf of the Department of Defense, the
software shall be classified as "Commercial Computer Software" and the
Government shall have only "Restricted Rights" as defined in Clause
252.227-7013 (c) (1) of DFARs. Notwithstanding the foregoing, the
authors grant the U.S. Government and others acting in its behalf
permission to use and distribute the software in accordance with the
terms specified in this license.
---*/

/**
  @file 
 
  A linked list implementation designed to handle variable length entries, and 
  minimize the number of pages spanned by each list. 
  
  The data is stored using the slotted page implementation.
  
  slot 0 is a long that points to the next page in the list.
  
  The rest of the slots store data.
  
  $Id $
*/

#ifndef __pageOrientedListNTA_H
#define __pageOrientedListNTA_H


typedef struct {
  long page;
  /* The slot of the next record to be returned. */
  int slot;
} lladd_pagedList_iterator;

//recordid dereferencePagedListRID(int xid, recordid rid);
/** @return 1 if the key was already in the list. */
int TpagedListInsert(int xid, recordid list, const byte * key, int keySize, const byte * value, int valueSize);
int TpagedListFind(int xid, recordid list, const byte * key, int keySize, byte ** value);
int TpagedListRemove(int xid, recordid list, const byte * key, int keySize);
int TpagedListMove(int xid, recordid start_list, recordid end_list, const byte *key, int keySize);
/** The linked list iterator can tolerate the concurrent removal of values that 
    it has already returned.  In the presence of such removals, the iterator 
    will return the keys and values present in the list as it existed when next()
    was first called.

    @return a new iterator initialized to the head of the list.  */
lladd_pagedList_iterator * TpagedListIterator(int xid, recordid list);
/** @return 1 if there was another entry to be iterated over. 0 otherwise.  
     If this function returns 1, the caller must free() the malloced memory 
     returned via the key and value arguments.*/
int TpagedListNext(int xid, lladd_pagedList_iterator * it, byte ** key, int * keySize, byte ** value, int * valueSize);
recordid TpagedListAlloc(int xid);
void TpagedListDelete(int xid, recordid list);
Operation getPagedListInsert();
Operation getPagedListRemove();
#endif
