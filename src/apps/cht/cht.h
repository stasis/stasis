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
#include <libdfa/libdfa.h>
#include <pbl/jbhash.h>
#include "../../2pc/2pc.h"
#define CHT_COORDINATOR 1
#define CHT_SERVER      2
#define CHT_CLIENT      3
 

typedef struct {
  int id;
} clusterHashTable_t;

/**
 * jbHtCreate makes a new persistant hashtable
 * @param xid transaction id
 * @param dfaSet The state machine set that will be used to manage the cluster hash table.  (TODO: Who initializes this?)
 * @param new_ht a preallocated buffer to hold the new hash table.
 * @return 1 on success, 0 on error.pointer to hashtable, or NULL on error
 */
int cHtCreate(state_machine_id xid, DfaSet * dfaSet, clusterHashTable_t * new_ht);


/**
 * Insert a key/value pair
 * makes a SHALLOW COPY of the data to keep in durable storage
 * will REPLACE an existing entry with the same key
 * @param xid transaction id
 * @param ht hashtable in which to insert
 * @param key pointer to data serving as key
 * @param keylen how much data to use from pointer
 * @param dat data to insert
 * @param datlen length data
 * @return -1 on error, 0 on success
 */
int cHtInsert(state_machine_id xid, DfaSet * dfaSet, clusterHashTable_t * ht, void * key, size_t keylen, void * dat, size_t datlen);

/**
 * Lookup a value with a key
 *
 * *TODO* This api is only safe becuase value lengths are bounded!  
 * Need to fix API, and implementation to support arbitrary length
 * keys and values.
 *
 *
 * @param xid transaction id
 * @param ht hashtable in which to look
 * @param key pointer to key data
 * @param keylen length of key
 * @param dat preallocated buffer in which to put data
 * @param datlen overwritten with the size of the data that was read.
 * @return -1 if error occurs, including nothing found
 */
int cHtLookup(state_machine_id xid, DfaSet * dfaSet, clusterHashTable_t *ht, void *key, size_t keylen, void *dat, size_t* datlen);


/**
 * Delete entry associated with key
 * @param xid transaction id
 * @param ht hashtable in which to delete
 * @param key pointer to key data
 * @param keylen length of key
 * @param buf if non-NULL, preallocated space to copy data from deleted key
 * @return -1 on errors or not found, 0 if existing entry was deleted
 */
int cHtRemove( state_machine_id xid, DfaSet * dfaSet, clusterHashTable_t *ht, void *key, size_t keylen, void *dat, size_t * datlen );

/**
 * Delete a hashtable
 * table must be empty
 * @param xid transaction id
 * @param ht hashtable to delete
 * @return 0 on success, -1 on error
 */
int cHtDelete(state_machine_id xid, DfaSet * dfaSet, clusterHashTable_t *ht);

/**
 *  Returns a new DfaSet that is ready to have main_loop() or
 *  request() called on it.  Of course, the DfaSet implements a
 *  cluster hash table. ;)
 *
 * @param cht_type CHT_CLIENT, CHT_COORDINATOR, CHT_SUBORDINATE,
 * depending on which portion of the CHT the dfa set should implement.
 *
 * @param get_broadcast_group A function that maps key values (from
 * the messages key field) to broadcast groups.
 *
 * @see dfa_malloc for a description of the other parameters.
 *
 *
 */
DfaSet * cHtInit(int cht_type,/* char * localhost,*/
		 short (* get_broadcast_group)(DfaSet *, Message *),
		 /*short port,
		 char *** broadcast_lists,
		 int  broadcast_lists_count,
		 int* broadcast_list_host_count*/
		 NetworkSetup * ns);
DfaSet * cHtClientInit(char * config_file);
DfaSet * cHtCoordinatorInit(char * config_file, short(*get_broadcast_group)(DfaSet *, Message *));
DfaSet * cHtSubordinateInit(char * config_file, short(*get_broadcast_group)(DfaSet *, Message *), int subordinate_number);
int cHtGetXid(state_machine_id* xid, DfaSet * dfaSet);
/*int cHtCommit(state_machine_id xid, DfaSet * dfaSet);
  int cHtAbort(state_machine_id xid, DfaSet * dfaSet);*/

/** The server side state for a CHT. */
typedef struct {
  int ht_xid;
  jbHashTable_t * xid_ht;
  jbHashTable_t * ht_ht;
  /** This gets incremented by the coordinator each time a new hashtable is allocated. */
  int next_hashTableId;
} CHTAppState;


callback_fcn veto_or_prepare_cht;
callback_fcn commit_cht;
callback_fcn tally_cht;
callback_fcn abort_cht;
callback_fcn init_xact_cht;

short multiplex_interleaved(DfaSet * dfaSet, Message * m);
