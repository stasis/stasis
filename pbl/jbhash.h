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
 * header file for jb hash table structs
 * Based on Peter Graf's pblhash, <http://mission.base.com/peter/source/>
 * Jim Blomo <jim@xcf.berkeley.edu>
 * $Id$
 */

#ifndef __JBHASH_H__
#define __JBHASH_H__

#include <lladd/common.h>
#include <lladd/transactional.h>

/*  #define JB_HASHTABLE_SIZE 79 */

typedef struct {
	recordid store;
	size_t keylen;
	size_t datlen;
	recordid next;
} jbHashItem_t;

typedef struct {
        int size;
        recordid hashmap_record; /*[JB_HASHTABLE_SIZE]*/
	recordid store;
	jbHashItem_t *iterItem;
	unsigned int iterIndex;
	byte *iterData;
        recordid* hashmap;
} jbHashTable_t;

/**
 * jbHtCreate makes a new persistant hashtable
 * @param xid transaction id
 *
 * @param size The number of hashbuckets.  Currently, jbHash does not
 * resize its bucket table, so it is important to set this number
 * appropriately.
 *
 * @return pointer to hashtable, or NULL on error
 */
jbHashTable_t* jbHtCreate(int xid, int size);

/**
 * jbHtValid determins if a hashtable pointer is valid
 * @param xid transaction id
 * @param ht hashtable you want to validate
 * @return true if valid, false otherwise
 */
int jbHtValid(int xid, jbHashTable_t *ht);

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
int jbHtInsert(int xid, jbHashTable_t *ht, const byte *key, size_t keylen, const byte *dat, size_t datlen);


/**
 * Lookup a value with a key
 * @param xid transaction id
 * @param ht hashtable in which to look
 * @param key pointer to key data
 * @param keylen length of key
 * @param buf preallocated buffer in which to put data
 * @return -1 if error occurs, including nothing found
 */
int jbHtLookup( int xid, jbHashTable_t *ht, const byte *key, size_t keylen, byte *buf );

/**
 * Delete entry associated with key
 * @param xid transaction id
 * @param ht hashtable in which to delete
 * @param key pointer to key data
 * @param keylen length of key
 * @param buf if non-NULL, preallocated space to copy data from deleted key
 * @return -1 on errors or not found, 0 if existing entry was deleted
 */
int jbHtRemove( int xid, jbHashTable_t *ht, const byte *key, size_t keylen, byte *buf );

/**
 * Start an iterator on the hash table
 * @param xid transaction id
 * @param ht hashtable
 * @param buf preallocated space to put data
 * @return -1 for no data or error, 0 on success
 */
int jbHtFirst( int xid, jbHashTable_t *ht, byte *buf );

/**
 * iterates to the next item
 * @param xid transaction id
 * @param ht hashtable
 * @param buf preallocated space to put data
 * @return -1 for no data or error, 0 on success
 */
int jbHtNext( int xid, jbHashTable_t *ht, byte *buf );

/**
 * get data for the place the iterator is currently in
 * @param xid transaction id
 * @param ht hashtable
 * @param buf preallocated space to put data
 * @return -1 for no data or error, 0 on success
 */
int jbHtCurrent(int xid, jbHashTable_t *ht, byte *buf);

/**
 * get key for the place the iterator is currently in
 * @param xid transaction id
 * @param ht hashtable
 * @param buf preallocated space to put key
 * @return -1 for no data or error, 0 on success
 */
int jbHtCurrentKey(int xid, jbHashTable_t *ht, byte *buf);

/**
 * Delete a hashtable
 * table must be empty
 * @param xid transaction id
 * @param ht hashtable to delete
 * @return 0 on success, -1 on error
 */
int jbHtDelete(int xid, jbHashTable_t *ht);

#endif
