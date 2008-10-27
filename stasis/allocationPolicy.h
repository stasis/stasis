#ifndef ALLOCATION_POLICY_H
#define ALLOCATION_POLICY_H

#include <stasis/common.h>

struct allocationPolicy;
typedef struct allocationPolicy allocationPolicy;

typedef struct availablePage { 
  int freespace;
  pageid_t pageid;
  int lockCount;  // Number of active transactions that have alloced or dealloced from this page.
} availablePage;

allocationPolicy * allocationPolicyInit();
void allocationPolicyDeinit(allocationPolicy * ap);
void allocationPolicyAddPages(allocationPolicy * ap, availablePage** newPages);
availablePage * allocationPolicyFindPage(allocationPolicy * ap, int xid, int freespace);
void allocationPolicyTransactionCompleted(allocationPolicy * ap, int xid);
void allocationPolicyUpdateFreespaceUnlockedPage(allocationPolicy * ap, availablePage * key, int newFree);
void allocationPolicyUpdateFreespaceLockedPage(allocationPolicy * ap, int xid, availablePage * key, int newFree);
void allocationPolicyLockPage(allocationPolicy * ap, int xid, pageid_t page);
void allocationPolicyAllocedFromPage(allocationPolicy * ap, int xid, pageid_t page);
/**
   Check to see if it is safe to allocate from a particular page.

   If concurrent transactions have freed up space on a page, but they
   eventually abort, then it might not be safe for the current
   transaction to reuse the storage those transactions freed.  This is
   needed for methods such as TallocFromPage(), which do not consult
   allocation policy before deciding where to attempt allocation.

   @param ap The allocation policy managing the space in question
   @param xid The transaction that wants to allocate space from the page
   @param page The page that will be allocated from.
   @return true if the allocation would be safe.  false if not sure.
 */
int allocationPolicyCanXidAllocFromPage(allocationPolicy * ap, int xid, pageid_t page);
#endif // ALLOCATION_POLICY_H
