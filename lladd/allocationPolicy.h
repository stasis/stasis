#ifndef ALLOCATION_POLICY_H
#define ALLOCATION_POLICY_H

struct allocationPolicy;
typedef struct allocationPolicy allocationPolicy;

typedef struct availablePage { 
  int freespace;
  int pageid;
  int lockCount;  // Number of active transactions that have alloced or dealloced from this page.
} availablePage;

allocationPolicy * allocationPolicyInit();
void allocationPolicyDeinit(allocationPolicy * ap);
void allocationPolicyAddPages(allocationPolicy * ap, availablePage** newPages);
availablePage * allocationPolicyFindPage(allocationPolicy * ap, int xid, int freespace);
void allocationPolicyTransactionCompleted(allocationPolicy * ap, int xid);
void allocationPolicyUpdateFreespaceUnlockedPage(allocationPolicy * ap, availablePage * key, int newFree);
void allocationPolicyUpdateFreespaceLockedPage(allocationPolicy * ap, int xid, availablePage * key, int newFree);
void allocationPolicyLockPage(allocationPolicy * ap, int xid, int pageid);
void allocationPolicyAllocedFromPage(allocationPolicy * ap, int xid, int pageid); 
#endif // ALLOCATION_POLICY_H
