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


#ifndef __LINKEDLIST_H__
#define __LINKEDLIST_H__

/**
  @file 
 
  Implements a simple sorted linked list.  Written for LLADD's
  recovery algorithms.
  
 */
#include <lladd/common.h>
BEGIN_C_DECLS

typedef struct LinkedList {
  long val;
  struct LinkedList *next;
} LinkedList;

//typedef struct Nodetmp LinkedList;
//typedef LinkedList *LinkedListPtr;

void printList(LinkedList ** l) ;

void addVal(LinkedList **list, long val);

/**
  @return 1 if val is in the list, 0 otherwise
*/
int findVal(LinkedList ** list, int long);

/**
  Deallocates all nodes in the list, and sets list to null
*/
void destroyList (LinkedList **list);

/**
 *   Add a value to the list in descending order
 */
void addSortedVal(LinkedList **list, long val);

/**
 *   Remove a value from the list
 */
void removeVal(LinkedList **list, long val);

/**
 * Remove the first value (first on list). 
 *
 * If all values were added using "addSortedVal", this is the largest value in the list.
 *
 * @return 0 if no values were on the list
 */
long popMaxVal(LinkedList **list);

END_C_DECLS

#endif
