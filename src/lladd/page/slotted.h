/************************************************************************
 * @file implementation of variable-sized slotted pages

 STRUCTURE OF A PAGE
<pre>
 +-----------------------------------------+-----------------------+----+
 | DATA SECTION                 +--------->| RID: (PAGE, 0)        |    |
 |          +-----------------+ |          +-----------------------+    |
 |      +-->| RID: (PAGE, 1)  | |                                       |
 |      |   +-----------------+ |                                       |
 |      |                       |                                       |
 |      ----------------+       |        +------------------------------+
 |                      |       |   +--->| RID: (PAGE, n)               |
 |                      |       |   |    +------------------------------+
 |======================================================================|
 |  ^ FREE SPACE        |       |   |                                   |
 |  |                   |       |   |                                   |
 |  +-------------------|-------|---|--------------------+              |
 |                      |       |   |                    |              |
 |        +-------------|-------|---+                    |              |
 |        |             |       |                        |              |
 |    +---|---+-----+---|---+---|---+--------------+-----|------+-------+
 |    | slotn | ... | slot1 | slot0 | num of slots | free space |  ***  |
 +----+-------+-----+-------+-------+--------------+------------+-------+
</pre>

 *** = @see page.h for information on this field.

 NOTE:
   - slots are zero indexed.
   - slots are of implemented as (offset, length)

Slotted page layout: 

 END:
         lsn (4 bytes)
	 type (2 bytes)
	 free space (2 bytes)
	 num of slots (2 bytes)
	 freelist head(2 bytes)
	 slot 0 (4 bytes)
	 slot 1 (4 bytes)
	 ...
	 slot n (4 bytes)
	 ...
	 unused
	 ...
	 record n (x bytes)
	 ...
	 record 0 (y bytes)
	 record 1 (z bytes)

 START

 $Id$

@todo slotted.c Should know that specific record types (like blobs) exist,
                (but should not hardcode information about these types) This
                has been handled, except in slottedPostRalloc...

************************************************************************/

#define SLOTTED_PAGE_OVERHEAD_PER_RECORD (2 * sizeof(short))
#define SLOTTED_PAGE_HEADER_OVERHEAD (3 * sizeof(short))

//#define SLOTTED_PAGE_CHECK_FOR_OVERLAP 1
//#define SLOTTED_PAGE_SKIP_SANITY_CHECKS 1

#ifdef LONG_TEST
#define SLOTTED_PAGE_CHECK_FOR_OVERLAP 1
#endif

void slottedWrite(Page * page, recordid rid, const byte *data);
void slottedRead(Page * page, recordid rid, byte *buff);
void slottedWriteUnlocked(Page * page, recordid rid, const byte *data);
void slottedReadUnlocked(Page * page, recordid rid, byte *buff);

void slottedPageInitialize(Page * p);

#define freespace_ptr(page)      shorts_from_end((page), 1)
#define numslots_ptr(page)       shorts_from_end((page), 2)
#define freelist_ptr(page)       shorts_from_end((page), 3)
#define slot_ptr(page, n)        shorts_from_end((page), (2*(n))+4)
#define slot_length_ptr(page, n) shorts_from_end((page), (2*(n))+5)
#define record_ptr(page, n)      bytes_from_start((page), *slot_ptr((page), (n)))
#define isValidSlot(page, n)   ((*slot_ptr((page), (n)) == INVALID_SLOT) ? 0 : 1)

/**
 * allocate a record.  This must be done in two phases.  The first
 * phase reserves a slot, and produces a log entry.  The second phase
 * sets up the slot according to the contents of the log entry.
 *
 * Essentially, the implementation of this function chooses a page
 * with enough space for the allocation, then calls slottedRawRalloc.
 *
 * Ralloc implements the first phase.
 *
 * @param xid The active transaction.
 * @param size The size of the new record
 * @return allocated record
 *
 * @see postRallocSlot the implementation of the second phase.
 *
 */
//compensated_function recordid slottedPreRalloc(int xid, unsigned long size, Page**p);

/**
 * The second phase of slot allocation.  Called after the log entry
 * has been produced, and during recovery.  
 * 
 * @param page The page that should contain the new record.
 *
 * @param lsn The lsn of the corresponding log entry (the page's LSN
 * is updated to reflect this value.)
 * 
 * @param rid A recordid that should exist on this page.  If it does
 * not exist, then it is created.  Because slottedPreRalloc never
 * 'overbooks' pages, we are guaranteed to have enough space on the
 * page for this record (though it is possible that we need to compact
 * the page)
 */
recordid slottedPostRalloc(int xid, Page * page, lsn_t lsn, recordid rid);
/**
 * Mark the space used by a record for reclamation.
 * @param xid the transaction resposible for the deallocation, or -1 if outside of a transaction.
 * @param page a pointer to the binned page that contains the record
 * @param lsn the LSN of the redo log record that records this deallocation.
 * @param rid the recordid to be freed.
 */
void     slottedDeRalloc(int xid, Page * page, lsn_t lsn, recordid rid);

void slottedPageInit();
void slottedPageDeInit();

/**
 *
 * Bypass logging and allocate a record.  It should only be used
 * when the recordid returned is deterministic, and will be available
 * during recovery, as it bypassses the normal two-phase alloc / log
 * procedure for record allocation.  This usually means that this
 * function must be called by an Operation implementation that
 * alocates entire pages, logs the page ids that were allocated,
 * initializes the pages and finally calls this function in a
 * deterministic fashion.
 *
 * @see indirect.c for an example of how to use this function
 * correctly.
 *
 * This function assumes that the page is already loaded in memory.
 * It takes as parameters a Page and the size in bytes of the new
 * record.
 *
 * If you call this function, you probably need to be holding
 * lastFreepage_mutex.
 *
 * @see lastFreepage_mutex
 *
 * @return a recordid representing the newly allocated record.
 *
 * NOTE: might want to pad records to be multiple of words in length, or, simply
 *       make sure all records start word aligned, but not necessarily having 
 *       a length that is a multiple of words.  (Since Tread(), Twrite() ultimately 
 *       call memcpy(), this shouldn't be an issue)
 *
 * NOTE: pageRalloc() assumes that the caller already made sure that sufficient
 *       amount of freespace exists in this page.  
 * @see slottedFreespace()
 *
 * @todo pageRalloc's algorithm for reusing slot id's reclaims the
 * most recently freed slots first, which may encourage fragmentation.
 */
recordid slottedRawRalloc(Page * page, int size);

/**
 * Obtain an estimate of the amount of free space on this page.
 * Assumes that the page is already loaded in memory. 
 *
 * (This function is particularly useful if you need to use
 * slottedRawRalloc for something...)
 * 
 * @param p the page whose freespace will be estimated. 
 *
 * @return an exact measurment of the freespace, or, in the case of
 * fragmentation, an underestimate.
 */
size_t slottedFreespace(Page * p);

/** 
 *  Check to see if a slot is a normal slot, or something else, such
 *  as a blob.  This is stored in the size field in the slotted page
 *  structure.  If the size field is greater than PAGE_SIZE, then the
 *  slot contains a special value, and the size field indicates the
 *  type.  (The type is looked up in a table to determine the amount
 *  of the page's physical space used by the slot.)
 *
 *  @param p the page of interest
 *  @param slot the slot in p that we're checking.
 *  @return The type of this slot.
 */
int  slottedGetType(Page * p, int slot); 
/**
 *  Set the type of a slot to a special type, such as BLOB_SLOT.  This
 *  function does not update any other information in the page
 *  structure, and just writes the raw value of type into the slot's
 *  size field.  In particular, setting the type to NORMAL_SLOT will
 *  result in undefined behavior.  (Such a call would destroy all
 *  record of the slot's true physical size)
 *
 *  @param p the page containing the slot to be updated.
 *  @param slot the slot whose type will be changed.
 *  @param type the new type of slot.  Must be greater than PAGE_SIZE.
 *
 */
void slottedSetType(Page * p, int slot, int type);
/** The caller of this function must have a write lock on the page. */
void slottedCompact(Page * page);

void fsckSlottedPage(const Page const * page); 
