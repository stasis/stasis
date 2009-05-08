#ifndef ALLOCATION_POLICY_H
#define ALLOCATION_POLICY_H

#include <stasis/common.h>

struct allocationPolicy;
typedef struct allocationPolicy stasis_allocation_policy_t;

typedef struct availablePage { 
  int freespace;
  pageid_t pageid;
  int lockCount;  // Number of active transactions that have alloced or dealloced from this page.
} availablePage;

stasis_allocation_policy_t * stasis_allocation_policy_init();
void stasis_allocation_policy_deinit(stasis_allocation_policy_t * ap);
void stasis_allocation_policy_register_new_pages(stasis_allocation_policy_t * ap, availablePage** newPages);
availablePage * stasis_allocation_policy_pick_suitable_page(stasis_allocation_policy_t * ap, int xid, int freespace);
void stasis_allocation_policy_transaction_completed(stasis_allocation_policy_t * ap, int xid);
void stasis_allocation_policy_update_freespace_unlocked_page(stasis_allocation_policy_t * ap, availablePage * key, int newFree);
void stasis_allocation_policy_update_freespace_locked_page(stasis_allocation_policy_t * ap, int xid, availablePage * key, int newFree);
void stasis_allocation_policy_lock_page(stasis_allocation_policy_t * ap, int xid, pageid_t page);
void stasis_allocation_policy_alloced_from_page(stasis_allocation_policy_t * ap, int xid, pageid_t page);
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
int stasis_allocation_policy_can_xid_alloc_from_page(stasis_allocation_policy_t * ap, int xid, pageid_t page);
#endif // ALLOCATION_POLICY_H
