#ifndef ALLOCATION_POLICY_H
#define ALLOCATION_POLICY_H

struct allocationPolicy;
typedef struct allocationPolicy allocationPolicy;

typedef struct availablePage { 
  int freespace;
  int pageid;
} availablePage;

allocationPolicy * allocationPolicyInit();
void allocationPolicyAddPages(allocationPolicy * ap, availablePage** newPages);
availablePage * allocationPolicyFindPage(allocationPolicy * ap, int xid, int freespace);
void allocationPolicyTransactionCompleted(allocationPolicy * ap, int xid);
void allocationPolicyUpdateFreespaceUnlockedPage(allocationPolicy * ap, availablePage * key, int newFree);
void allocationPolicyUpdateFreespaceLockedPage(allocationPolicy * ap, int xid, availablePage * key, int newFree);

#endif // ALLOCATION_POLICY_H
