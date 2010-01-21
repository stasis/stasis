
/** 
    This file defines pbl's hashtable interface.  It is only included
    in order to ease the transition away from PBL, and to remove all
    of PBL from the source tree. 

    @see lhtable.h which replaces this interface.
*/

#ifndef PBL_H
#define PBL_H
#ifdef PBL_COMPAT

#include <stasis/common.h>
#include <stdio.h>

struct  pblHashTable_t;
typedef struct pblHashTable_t pblHashTable_t;

pblHashTable_t * pblHtCreate( );

int    pblHtDelete  ( pblHashTable_t * h );
int    pblHtInsert  ( pblHashTable_t * h, const void * key, size_t keylen,
		      void * dataptr);
int    pblHtRemove  ( pblHashTable_t * h, const void * key, size_t keylen );
void * pblHtLookup  ( pblHashTable_t * h, const void * key, size_t keylen );
void * pblHtFirst   ( pblHashTable_t * h );
void * pblHtNext    ( pblHashTable_t * h );
void * pblHtCurrent ( pblHashTable_t * h );
void * pblHtCurrentKey ( pblHashTable_t * h );

#else
#error pbl.h was included, but PBL_COMPAT is not defined!
#endif

#endif
