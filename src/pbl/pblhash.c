/*
 pblhash.c - hash table implementation

 Copyright (C) 2002    Peter Graf

   This file is part of PBL - The Program Base Library.
   PBL is free software.

    This library is free software; you can redistribute it and/or
    modify it under the terms of the GNU Lesser General Public
    License as published by the Free Software Foundation; either
    version 2.1 of the License, or (at your option) any later version.

    This library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
    Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public
    License along with this library; if not, write to the Free Software
    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

   For more information on the Program Base Library or Peter Graf,
   please see: http://mission.base.com/.

    $Log$
    Revision 1.7  2004/10/20 06:54:39  sears
    parameter tweak..

    Revision 1.6  2004/10/20 06:48:27  sears
    Set some constants to values appropriate for my desktop.

    Revision 1.5  2004/10/19 04:45:42  sears
    Speedups, most notably in the logging subsystem.

    Revision 1.4  2004/10/18 18:24:51  sears
    Preliminary version of logical logging linear hash.  (No latching yet, and there are some bugs re-opening a closed hash.)

    Revision 1.3  2004/10/17 02:17:00  sears
    Optimized the 'naive' linear hash table, and moved to sourceforge. :)

    Revision 1.2  2004/07/20 00:15:17  sears
    pageCache.c is now re-entrant.

    Revision 1.1.1.1  2004/06/24 21:11:54  sears
    Need to send laptop in for warranty service, so it's time to put this code into CVS. :)

    Vs. the paper version of LLADD, this version has a re-written logger + recovery system.  It also includes unit tests and API documentation.

    Revision 1.4  2004/05/26 09:55:31  sears
    Misc bugfixes / optimizations.

    Revision 1.3  2003/12/11 10:48:16  jim
    compiles, not link. added quasi-pincount, shadow pages

    Revision 1.2  2003/12/11 09:21:20  jim
    update includes

    Revision 1.1  2003/12/11 09:10:48  jim
    pbl

    Revision 1.2  2002/09/12 20:46:30  peter
    added the isam file handling to the library

    Revision 1.1  2002/09/05 13:45:01  peter
    Initial revision


*/

/*
 * make sure "strings <exe> | grep Id | sort -u" shows the source file versions
 */
static char* rcsid = "$Id$";
static int   rcsid_fct() { return( rcsid ? 0 : rcsid_fct() ); }

#include <stdio.h>
#include <memory.h>
#include <stdlib.h>

#include <pbl/pbl.h>

/*****************************************************************************/
/* #defines                                                                  */
/*****************************************************************************/
/*#define PBL_HASHTABLE_SIZE      1019 */
/*#define PBL_HASHTABLE_SIZE      2017*/
#define PBL_HASHTABLE_SIZE      2048
/* #define PBL_HASHTABLE_SIZE 5003  */
/*#define PBL_HASHTABLE_SIZE 16384*/
/*#define PBL_HASHTABLE_SIZE 8192*/
/*#define PBL_HASHTABLE_SIZE   100003 */

/*****************************************************************************/
/* typedefs                                                                  */
/*****************************************************************************/

typedef struct pbl_hashitem_s
{
    void                  * key;
    size_t                  keylen;

    void                  * data;

    struct pbl_hashitem_s * next;
    struct pbl_hashitem_s * prev;

    struct pbl_hashitem_s * bucketnext;
    struct pbl_hashitem_s * bucketprev;

} pbl_hashitem_t;

typedef struct pbl_hashbucket_s
{
    pbl_hashitem_t * head;
    pbl_hashitem_t * tail;

} pbl_hashbucket_t;

struct pbl_hashtable_s
{
    char             * magic;
    int                currentdeleted;
    pbl_hashitem_t   * head;
    pbl_hashitem_t   * tail;
    pbl_hashitem_t   * current;
    pbl_hashbucket_t * buckets;

};
typedef struct pbl_hashtable_s pbl_hashtable_t;
    
/*****************************************************************************/
/* globals                                                                   */
/*****************************************************************************/

/*****************************************************************************/
/* functions                                                                 */
/*****************************************************************************/

/*static int hash( const unsigned char * key, size_t keylen )
{
    int ret = 104729;

    for( ; keylen-- > 0; key++ )
    {
        if( *key )
        {
            ret *= *key + keylen;
            ret %= PBL_HASHTABLE_SIZE;
        }
    }

    return( ret % PBL_HASHTABLE_SIZE );
}*/
#include <lladd/crc32.h>
/*static unsigned int hash( const unsigned char * key, size_t keylen ) {
  if(keylen == sizeof(int)) {return *key &(PBL_HASHTABLE_SIZE-1);}//% PBL_HASHTABLE_SIZE;}
  return ((unsigned int)(crc32((char*)key, keylen, -1))) & (PBL_HASHTABLE_SIZE-1); //% PBL_HASHTABLE_SIZE;
}*/

#define hash(x, y) (((keylen)==sizeof(int) ?  \
		    (*(unsigned int*)key) & (PBL_HASHTABLE_SIZE-1) :\
		    ((unsigned int)(crc32((char*)(key), (keylen), -1))) & (PBL_HASHTABLE_SIZE-1)))


/**
 * create a new hash table
 *
 * @return pblHashTable_t * retptr != NULL: pointer to new hash table
 * @return pblHashTable_t * retptr == NULL: OUT OF MEMORY
 */
pblHashTable_t * pblHtCreate( void )
{
    pbl_hashtable_t * ht;

    ht = pbl_malloc0( "pblHtCreate hashtable", sizeof( pbl_hashtable_t ) );
    if( !ht )
    {
        return( 0 );
    }

    ht->buckets = pbl_malloc0( "pblHtCreate buckets",
                               sizeof( pbl_hashbucket_t ) * PBL_HASHTABLE_SIZE);
    if( !ht->buckets )
    {
        PBL_FREE( ht );
        return( 0 );
    }

    /*
     * set the magic marker of the hashtable
     */
    ht->magic = rcsid;

    return( ( pblHashTable_t * )ht );
}

/**
 * insert a key / data pair into a hash table
 *
 * only the pointer to the data is stored in the hash table
 * no space is malloced for the data!
 *
 * @return  int ret == 0: ok
 * @return  int ret == -1: an error, see pbl_errno:
 * @return    PBL_ERROR_EXISTS:        an item with the same key already exists
 * @return    PBL_ERROR_OUT_OF_MEMORY: out of memory
 */

int pblHtInsert(
pblHashTable_t          * h,      /** hash table to insert to             */
void                    * key,    /** key to insert                       */
size_t                    keylen, /** length of that key                  */
void                    * dataptr /** dataptr to insert                   */
)
{
    pbl_hashtable_t  * ht = ( pbl_hashtable_t * )h;
    pbl_hashbucket_t * bucket = 0;
    pbl_hashitem_t   * item = 0;

    int                hashval = hash( key, keylen );
    
    bucket = ht->buckets + hashval;

    if( keylen < (size_t)1 )
    {
        /*
         * the length of the key can not be smaller than 1
         */
        pbl_errno = PBL_ERROR_EXISTS;
        return( -1 );
    }

    for( item = bucket->head; item; item = item->bucketnext )
    {
        if(( item->keylen == keylen ) && !memcmp( item->key, key, keylen ))
        {
            snprintf( pbl_errstr, PBL_ERRSTR_LEN,
                      "insert of duplicate item in hashtable\n" );
            pbl_errno = PBL_ERROR_EXISTS;
            return( -1 );
        }
    }

    item = pbl_malloc0( "pblHtInsert hashitem", sizeof( pbl_hashitem_t ) );
    if( !item )
    {
        return( -1 );
    }

    item->key = pbl_memdup( "pblHtInsert item->key", key, keylen );
    if( !item->key )
    {
        PBL_FREE( item );
        return( -1 );
    }
    item->keylen = keylen;
    item->data = dataptr;

    /*
     * link the item
     */
    PBL_LIST_APPEND( bucket->head, bucket->tail, item, bucketnext, bucketprev );
    PBL_LIST_APPEND( ht->head, ht->tail, item, next, prev );

    ht->current = item;
    return( 0 );
}

/**
 * search for a key in a hash table
 *
 * @return void * retptr != NULL: pointer to data of item found
 * @return void * retptr == NULL: no item found with the given key
 * @return     PBL_ERROR_NOT_FOUND:
 */

void * pblHtLookup(
pblHashTable_t              * h,      /** hash table to search in          */
void                        * key,    /** key to search                    */
size_t                        keylen  /** length of that key               */
)
{
    pbl_hashtable_t  * ht = ( pbl_hashtable_t * )h;
    pbl_hashbucket_t * bucket = 0;
    pbl_hashitem_t   * item = 0;

    int                hashval = hash( key, keylen );
    
    bucket = ht->buckets + hashval;

    for( item = bucket->head; item; item = item->bucketnext )
    {
      if(( item->keylen == keylen ) && !memcmp( item->key, key, keylen ))
        {
            ht->current = item;
            ht->currentdeleted = 0;
            return( item->data );
        }
    }
            
    pbl_errno = PBL_ERROR_NOT_FOUND;

    return( 0 );
}

/**
 * get data of first key in hash table
 *
 * This call and \Ref{pblHtNext} can be used in order to loop through all items
 * stored in a hash table.
 *
 * <PRE>
   Example:

   for( data = pblHtFirst( h ); data; data = pblHtNext( h ))
   {
       do something with the data pointer
   }
   </PRE>

 * @return void * retptr != NULL: pointer to data of first item
 * @return void * retptr == NULL: the hash table is empty
 * @return     PBL_ERROR_NOT_FOUND:
 */

void * pblHtFirst(
pblHashTable_t              * h       /** hash table to look in            */
)
{
    pbl_hashtable_t  * ht = ( pbl_hashtable_t * )h;
    pbl_hashitem_t   * item = 0;

    item = ht->head;
    if( item )
    {
        ht->current = item;
        ht->currentdeleted = 0;
        return( item->data );
    }

    pbl_errno = PBL_ERROR_NOT_FOUND;
    return( 0 );
}

/**
 * get data of next key in hash table
 *
 * This call and \Ref{pblHtFirst} can be used in order to loop through all items
 * stored in a hash table.
 *
 * <PRE>
   Example:

   for( data = pblHtFirst( h ); data; data = pblHtNext( h ))
   {
       do something with the data pointer
   }
   </PRE>

 * @return void * retptr != NULL: pointer to data of next item
 * @return void * retptr == NULL: there is no next item in the hash table
 * @return     PBL_ERROR_NOT_FOUND: 
 */

void * pblHtNext(
pblHashTable_t              * h       /** hash table to look in            */
)
{
    pbl_hashtable_t  * ht = ( pbl_hashtable_t * )h;
    pbl_hashitem_t   * item = 0;

    if( ht->current )
    {
        if( ht->currentdeleted )
        {
            item = ht->current;
        }
        else
        {
            item = ht->current->next;
        }
        ht->currentdeleted = 0;
    }
    if( item )
    {
        ht->current = item;
        return( item->data );
    }

    pbl_errno = PBL_ERROR_NOT_FOUND;
    return( 0 );
}

/**
 * get data of current key in hash table
 *
 * @return void * retptr != NULL: pointer to data of current item
 * @return void * retptr == NULL: there is no current item in the hash table
 * @return     PBL_ERROR_NOT_FOUND:
 */

void * pblHtCurrent(
pblHashTable_t              * h       /** hash table to look in            */
)
{
    pbl_hashtable_t  * ht = ( pbl_hashtable_t * )h;

    if( ht->current )
    {
        return( ht->current->data );
    }

    pbl_errno = PBL_ERROR_NOT_FOUND;
    return( 0 );
}

/**
 * get key of current key in hash table
 *
 * @return void * retptr != NULL: pointer to data of current item
 * @return void * retptr == NULL: there is no current item in the hash table
 * @return     PBL_ERROR_NOT_FOUND:
 */

void * pblHtCurrentKey(
pblHashTable_t              * h       /** hash table to look in            */
)
{
    pbl_hashtable_t  * ht = ( pbl_hashtable_t * )h;

    if( ht->current )
    {
        return( ht->current->key );
    }

    pbl_errno = PBL_ERROR_NOT_FOUND;
    return( 0 );
}

/**
 * remove an item from the hash table
 *
 * parameters key and keylen are optional, if they are not given
 * the current record is deleted
 *
 * if the current record is removed the pointer to the current record
 * is moved to the next record.
 *
 * <PRE>
   Example:

   for( data = pblHtFirst( h ); data; data = pblHtRemove( h, 0, 0 ))
   {
       this loop removes all items from a hash table
   }
   </PRE>
 *
 * if the current record is moved by this function the next call to
 * \Ref{pblHtNext} will return the data of the then current record.
 * Therefore the following code does what is expected:
 * It visits all items of the hash table and removes the expired ones.
 *
 * <PRE>
   for( data = pblHtFirst( h ); data; data = pblHtNext( h ))
   {
       if( needs to be deleted( data ))
       {
           pblHtRemove( h, 0, 0 );
       }
   }
   </PRE>
 
 * @return int ret == 0: ok
 * @return int ret == -1: an error, see pbl_errno:
 * @return     PBL_ERROR_NOT_FOUND: the current item is not positioned
 * @return                          or there is no item with the given key
 */

int pblHtRemove(
pblHashTable_t            * h,     /** hash table to remove from           */
void                      * key,   /** OPT: key to remove                  */
size_t                      keylen /** OPT: length of that key             */
)
{
    pbl_hashtable_t  * ht = ( pbl_hashtable_t * )h;
    pbl_hashbucket_t * bucket = 0;
    pbl_hashitem_t   * item = 0;

    int                hashval = 0;

    if( keylen && key )
    {
        hashval = hash( key, keylen );
        bucket = ht->buckets + hashval;

        for( item = bucket->head; item; item = item->bucketnext )
        {
            if(( item->keylen == keylen ) && !memcmp( item->key, key, keylen ))
            {
                break;
            }
        }
    }
    else
    {
        item = ht->current;

        if( item )
        {
            hashval = hash( item->key, item->keylen );
            bucket = ht->buckets + hashval;
        }
    }

    if( item )
    {
        if( item == ht->current )
        {
            ht->currentdeleted = 1;
            ht->current = item->next;
        }

        /*
         * unlink the item
         */
        PBL_LIST_UNLINK( bucket->head, bucket->tail, item,
                         bucketnext, bucketprev );
        PBL_LIST_UNLINK( ht->head, ht->tail, item, next, prev );

        PBL_FREE( item->key );
        PBL_FREE( item );
        return( 0 );
    }

    pbl_errno = PBL_ERROR_NOT_FOUND;
    return( -1 );
}

/**
 * delete a hash table
 *
 * the hash table has to be empty!
 *
 * @return int ret == 0: ok
 * @return int ret == -1: an error, see pbl_errno:
 * @return     PBL_ERROR_EXISTS: the hash table is not empty
 */

int pblHtDelete(
pblHashTable_t * h        /** hash table to delete */
)
{
    pbl_hashtable_t  * ht = ( pbl_hashtable_t * )h;

    if( ht->head )
    {
        pbl_errno = PBL_ERROR_EXISTS;
        return( -1 );
    }

    PBL_FREE( ht->buckets );
    PBL_FREE( ht );

    return( 0 );
}

