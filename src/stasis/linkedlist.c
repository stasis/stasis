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
/******************************
 * $Id$
 *
 * simple linked list
 *****************************/
#include <stasis/linkedlist.h>
#include <stdio.h>
void printList(LinkedList **l) {
	LinkedList * tmp = *l;
	printf ("List is ");
	while (tmp!=NULL) {
		printf ("%ld ", tmp->val);
		tmp = tmp->next;
	}
	printf (".\n");
}
void addVal(LinkedList **list, long val) {
  LinkedList * new = (LinkedList *)malloc(sizeof(LinkedList));
  new->val = val;
  new->next = NULL;
  if (*list==NULL) {
    *list = new;
  }
  else {
    new->next = *list;
    *list = new;
  }
}
void removeVal(LinkedList **list, long val) {
  LinkedList * tmp;
  LinkedList * tmpprev;
  if (*list==NULL) return;
  if ((*list!=NULL) && ((*list)->val==val)) {
  	tmp = *list;
	*list = (*list)->next;
	free(tmp);
	return;
  }
  tmp = (*list)->next;
  tmpprev = *list;
  while (tmp!=NULL) {
    if (tmp->val==val) {
		tmpprev->next = tmp->next;
		free(tmp);
		return;
	}
	tmpprev = tmp;
	tmp = tmp->next;
  }
}
long popMaxVal(LinkedList **list) {
	LinkedList * tmp;
	long tmpval;
	if (*list!=NULL) {
	  tmp = *list;
	  (*list) = (*list)->next;
	  tmpval = tmp->val;
	  free(tmp);
	  return tmpval;
	}
	else return -1; /*this should be an error! */
}

void addSortedVal(LinkedList **list, long val) {
  LinkedList * tmp;
  LinkedList * tmpprev;
  LinkedList * new = malloc(sizeof(LinkedList));
  new->val = val;
  /*see if new entry should come in the beginning*/
  if ((*list==NULL) || ((*list)->val<val)) {
	new->next = *list;
	*list = new;
	return;
  }
  /*else determine where to put new entry*/
  tmp = (*list)->next;
  tmpprev = *list;
  while (tmp!=NULL) {
	  if (tmp->val<val) {
		  tmpprev->next = new;
		  new->next = tmp;
		  return;
	  }
	  tmpprev = tmp;
	  tmp = tmp->next;
  }
  /*if gotten here, tmp is null so put item at the end of the list*/
  new->next = NULL;
  tmpprev->next = new;
}
/*
  return 1 if val is in the list, 0 otherwise
*/
int findVal(LinkedList **list, long val) {
  LinkedList * tmp = *list;
  while (tmp!=NULL) {
    if (tmp->val==val)
      return 1;
    tmp = tmp->next;
  }
  return 0;
}

/*  Deallocates all nodes in the list, and sets list to null
 */
void destroyList (LinkedList **list) {
  LinkedList * tmp;
  while ((*list)!=NULL) {
    tmp = (*list)->next;
    free((*list));
    (*list)=tmp;
  }
  list = NULL;
}
