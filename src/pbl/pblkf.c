/*
 pblkf.c - key file library implementation

 Copyright (C) 2002      Peter Graf

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
    Revision 1.1  2004/06/24 21:12:03  sears
    Initial revision

    Revision 1.1  2003/12/11 09:10:49  jim
    pbl

    Revision 1.4  2003/02/19 22:26:49  peter
    made sure #defined values can be set by compiler switches

    Revision 1.3  2002/11/01 13:56:24  peter
    Truncation of the memory block list is now called
    at every file close.

    Revision 1.2  2002/11/01 13:27:30  peter
    The block reference hash table is deleted when the
    last block is deleted from the table, i.e. when the
    last file is closed

    Revision 1.1  2002/09/12 20:47:07  peter
    Initial revision


*/

/*
 * make sure "strings <exe> | grep Id | sort -u" shows the source file versions
 */
static char * rcsid = "$Id$";
static int    rcsid_fkt() { return( rcsid ? 0 : rcsid_fkt() ); }

#ifndef _WIN32

#include <unistd.h>

#endif

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

#include <sys/types.h>
#include <sys/stat.h>

#include <fcntl.h>
#include <memory.h>
#include <string.h>


#include <pbl/pbl.h>                 /* program base library                      */

/******************************************************************************/
/* #defines                                                                   */
/******************************************************************************/

#ifndef PBL_KF_TEST_DEFINES

#ifndef PBLDATASIZE             /* value might be set by compiler switch      */
#define PBLDATASIZE      4096   /* size of entire block data                  */
#endif

#ifndef PBLBLOCKSPERFILE        /* value might be set by compiler switch      */
#define PBLBLOCKSPERFILE   64   /* default number of cache blocks per file    */
#endif

#ifndef PBLNEXPANDED            /* value might be set by compiler switch      */
#define PBLNEXPANDED       32   /* default number of expanded blocks          */
#endif

#ifndef PBLFILEBLOCKS           /* value might be set by compiler switch      */
#define PBLFILEBLOCKS  0xffff   /* number of blocks per big file  on disk     */
#endif

#ifndef PBL_NFILES              /* value might be set by compiler switch      */
#define PBL_NFILES        256   /* maximum number of open key files           */
#endif

#else /* PBL_TEST_DEFINES */

#define PBLDATASIZE        64   /* size of entire block data                  */
#define PBLBLOCKSPERFILE   10   /* default number of blocks in our cache      */
#define PBLNEXPANDED        5   /* default number of expanded blocks          */
#define PBLFILEBLOCKS       3   /* number of blocks per file                  */

#undef  PBLKEYLENGTH
#define PBLKEYLENGTH        8   /* maximum length of a key                  */

#undef  PBLDATALENGTH
#define PBLDATALENGTH      16   /* maximum length of data that is stored      */
                                /* with an item on the level 0 block          */
                                /* data that is longer is stored on data      */
                                /* blocks                                     */
#ifndef PBL_NFILES
#define PBL_NFILES        256   /* maximum number of open key files           */
#endif

#endif /* PBL_TEST_DEFINES */

#define PBLHEADERSIZE      13   /* offset of first free byte on a new block   */

#define PBLMAGIC          "1.00 Peter's B Tree"
                                /* during creation this string is written     */
                                /* to the files                               */

/******************************************************************************/
/* #macros                                                                    */
/******************************************************************************/

/*
 * macro to determine free space on an index block
 */
#define PBLINDEXBLOCKNFREE( B ) ((PBLDATASIZE - B->free) - (2 * B->nentries))

/*
 * macro to determine free space on a data block
 */
#define PBLDATABLOCKNFREE( B ) ( PBLDATASIZE - B->free )

/*
 * macros for getting pointers or buffer offsets from the index
 */
#define PBLINDEXTOPTR( B, I ) ( B->data\
                              + ( PBLDATASIZE - 2 * ( 1 + ( I ) )))

#define PBLINDEXTOBUFOFF( B, I ) pbl_BufToShort( PBLINDEXTOPTR( B, I ))

/******************************************************************************/
/* typedefs                                                                   */
/******************************************************************************/

/*
 * PBL key file descriptor
 */
typedef struct PBLKFILE_s
{
    char * magic;                /* magic string                              */
    int    bf;                   /* file handle from bigfile handling         */
    int    update;               /* flag: file open for update                */

    long   blockno;              /* of block current record is on             */
    int    index;                /* of current record on this block           */

    long   saveblockno;          /* block number of saved position            */
    int    saveindex;            /* item index of saved position              */

    void * filesettag;           /* file set tag attached to file             */

    int    transactions;         /* number of transactions active for file    */
    int    rollback;             /* next commit should lead to a rollback     */

    void * writeableListHead;    /* head and tail of writeable blocks         */
    void * writeableListTail;

                                 /* a user defined key compare function       */
    int (*keycompare)( void * left, size_t llen, void * right, size_t rlen );

} PBLKFILE_t;

/*
 * items are the things we store in the data array of the index blocks
 */
typedef struct PBLITEM_s
{
    unsigned int    level;       /* level where item is inserted              */

    int             keycommon;   /* number of initial bytes this item has in  */
                                 /* common with its predecessor on tbe block  */

    int             keylen;      /* length of the key                         */
    unsigned char * key;         /* pointer to the key                        */

    long            datalen;     /* length of the data                        */
    unsigned char * data;        /* the data of the item                      */

    long            datablock;   /* number of block the data is on            */
    long            dataoffset;  /* offset of data on block                   */

    struct PBLITEM_s * next;     /* we save items in a list                   */
    struct PBLITEM_s * prev;     /* during an insert                          */

} PBLITEM_t;

/*
 * memory blocks that have expanded keys are stored in an extra list
 */
typedef struct PBLBLOCKLINK_s
{
    struct PBLBLOCKLINK_s * next;
    struct PBLBLOCKLINK_s * prev;

} PBLBLOCKLINK_t;

/*
 * a file block in memory has this format
 */
typedef struct PBLBLOCK_s
{
    PBLBLOCKLINK_t   blocklink;         /* for linking blocks in a list       */

    unsigned int   level;               /* of block                           */

    long           blockno;             /* block number in file               */
    int            bf;                  /* big file handle block belongs to   */
    void         * filesettag;          /* file set tag attached              */

    unsigned char  data[ PBLDATASIZE ]; /* the real data                      */

    long           nblock;              /* next block of same level           */
    long           pblock;              /* previous block of same level       */
    unsigned int   nentries;            /* number of entries                  */
    int            free;                /* offset of first free byte          */

    long           parentblock;         /* number of parent block             */
    int            parentindex;         /* index on parentblock               */

    int            writeable;           /* block can be written to            */
    int            dirty;               /* has the block been written to ?    */

    unsigned char *expandedkeys;        /* pointer to expanded keys of        */
                                        /* the block                          */

    struct PBLBLOCK_s * next;           /* we keep the blocks we have         */
    struct PBLBLOCK_s * prev;           /* in memory in a LRU chain           */

} PBLBLOCK_t;

/*
 * all file handles of all open filesystem files of
 * all bigfiles are stored in a list
 */
typedef struct PBLBIGFILEHANDLE_s
{
    int    bf;                     /* bigfile handle of file                  */
    int    n;                      /* bigfile index of file                   */

    int    fh;                     /* filesystem handle                       */
    int    mode;                   /* open mode                               */

    struct PBLBIGFILEHANDLE_s * next;
    struct PBLBIGFILEHANDLE_s * prev;

} PBLBIGFILEHANDLE_t;

/*
 * a bigfile in memory
 */
typedef struct PBLBIGFILE_s
{
    char * name;                    /* name of first filesystem file          */

    int    mode;                    /* open mode                              */
    long   blocksperfile;           /* blocks per filesystem file             */
    long   blocksize;               /* size of one block                      */
    long   nextblockno;             /* block number of next free block        */

} PBLBIGFILE_t;

/*
 * references to blocks are stored in a hashtable
 */
typedef struct PBLBLOCKREF_s
{
    long           blockno;             /* block number in file               */
    long           bf;                  /* big file handle block belongs to   */

    PBLBLOCK_t   * block;               /* address of block                   */

} PBLBLOCKREF_t;

typedef struct PBLBLOCKHASHKEY_s
{
    long           blockno;             /* block number in file               */
    long           bf;                  /* big file handle block belongs to   */

} PBLBLOCKHASHKEY_t;

/******************************************************************************/
/* globals                                                                    */
/******************************************************************************/

long pblnreads  = 0;
long pblnwrites = 0;

static char * magic = PBLMAGIC;

/*
 * pool of open bigfiles
 */
static PBLBIGFILE_t pbf_pool[ PBL_NFILES ];

/*
 * headers for lists
 */
static PBLBIGFILEHANDLE_t     * pbf_ft_head;
static PBLBIGFILEHANDLE_t     * pbf_ft_tail;
static PBLBLOCKLINK_t         * linkListHead;
static PBLBLOCKLINK_t         * linkListTail;
static PBLBLOCK_t             * blockListHead;
static PBLBLOCK_t             * blockListTail;
static PBLITEM_t              * itemListHead;
static PBLITEM_t              * itemListTail;

/*
 * counters
 */
static int            pblnblocks;
static int            pblnlinks;
static int            pblblocksperfile = PBLBLOCKSPERFILE;
static int            pblexpandedperfile = PBLNEXPANDED;
static long           pblnfiles;

/*
 * block reference hash table
 */
static pblHashTable_t * pblblockhash;

/******************************************************************************/
/* declarations                                                               */
/******************************************************************************/
static void pblBlockKeysRelease( PBLBLOCK_t * block );
static int pblBlockKeysExpand( PBLBLOCK_t * block );

/******************************************************************************/
/* functions                                                                  */
/******************************************************************************/
/*
 * verify consistency of parameters
 */
static int pblParamsCheck(
unsigned char * key,
unsigned int    keylen,
unsigned char * data,
long            datalen
)
{
    if(( keylen < 1 ) || ( keylen > 255 ))
    {
        pbl_errno = PBL_ERROR_PARAM_KEYLEN;
        return( -1 );
    }

    if( datalen < 0 )
    {
        pbl_errno = PBL_ERROR_PARAM_DATALEN;
        return( -1 );
    }

    if( !key )
    {
        pbl_errno = PBL_ERROR_PARAM_KEY;
        return( -1 );
    }

    if( datalen && ( !data ))
    {
        pbl_errno = PBL_ERROR_PARAM_DATA;
        return( -1 );
    }

    return( 0 );
}

/*
 * functions on the block reference hash table
 */
/*
------------------------------------------------------------------------------
  FUNCTION:     pblBlockHashInsert

  DESCRIPTION:  inserts a new block reference into the hash table

  RETURNS:      int rc == 0: a new reference was inserted
                int rc == 1: the reference was already there, it was udpated
                int rc <  0: some error occured, see pbl_errno
                 PBL_ERROR_OUT_OF_MEMORY:        malloc failed, out of memory
------------------------------------------------------------------------------
*/
static int pblBlockHashInsert( long blockno, long bf, PBLBLOCK_t * block )
{
    PBLBLOCKHASHKEY_t   key;
    PBLBLOCKREF_t     * ref;
    int                 rc;

    memset( &key, 0, sizeof( key ));
    key.blockno = blockno;
    key.bf      = bf;

    if( !pblblockhash )
    {
        pblblockhash = pblHtCreate();
        if( !pblblockhash )
        {
            return( -1 );
        }
    }

    /*
     * see whether we have the reference already
     */
    ref = pblHtLookup( pblblockhash, &key, sizeof( key ));
    if( ref )
    {
        /*
         * the reference is already there, update the block pointer
         */
        ref->block = block;
        return( 1 );
    }

    if( pbl_errno == PBL_ERROR_NOT_FOUND )
    {
        pbl_errno = 0;
    }

    /*
     * allocate memory for new reference
     */
    ref = pbl_malloc0( "pblBlockHashInsert BLOCKREF", sizeof( PBLBLOCKREF_t ));
    if( !ref )
    {
        return( -1 );
    }

    /*
     * insert to the hash table
     */
    rc = pblHtInsert( pblblockhash, &key, sizeof( key ), ref );
    if( !rc )
    {
        ref->block   = block;
        ref->blockno = blockno;
        ref->bf      = bf;
    }
    else
    {
        PBL_FREE( ref );
        return( -1 );
    }

    return( 0 );
}

/*
------------------------------------------------------------------------------
  FUNCTION:     pblBlockHashRemove

  DESCRIPTION:  Remove a block reference from the hash table

  RETURNS:      int rc == 0: call went ok;
                otherwise:   block not found in hash table
------------------------------------------------------------------------------
*/
static int pblBlockHashRemove( long blockno, long bf )
{
    PBLBLOCKHASHKEY_t   key;
    PBLBLOCKREF_t     * ref;
    int                 rc;

    memset( &key, 0, sizeof( key ));
    key.blockno = blockno;
    key.bf      = bf;

    /*
     * if there is no hash table yet, the reference is not found
     */
    if( !pblblockhash )
    {
        return( 1 );
    }

    /*
     * see whether we have the reference
     */
    ref = pblHtLookup( pblblockhash, &key, sizeof( key ));
    if( !ref )
    {
        if( pbl_errno == PBL_ERROR_NOT_FOUND )
        {
            pbl_errno = 0;
        }

        return( 1 );
    }

    /*
     * remove the reference from the hash table
     */
    rc = pblHtRemove( pblblockhash, &key, sizeof( key ));
    if( rc )
    {
        return( 1 );
    }

    PBL_FREE( ref );

    /*
     * attempt to remove the hashtable
     */
    rc = pblHtDelete( pblblockhash );
    if( !rc )
    {
        /*
         * the hash table was empty and is deleted now
         */
        pblblockhash = 0;
    }
    else
    {
        /*
         * the hash table was not deleted because it is not
         * empty, clear the error
         */
        pbl_errno = 0;
    }

    return( 0 );
}

/*
------------------------------------------------------------------------------
  FUNCTION:     pblBlockHashFind

  DESCRIPTION:  Find a block reference in the hash table

  RETURNS:      PBLBLOCK_t * retptr == 0: block not found
                otherwise:                pointer to block in memory
------------------------------------------------------------------------------
*/
static PBLBLOCK_t * pblBlockHashFind( long blockno, long bf )
{
    PBLBLOCKHASHKEY_t   key;
    PBLBLOCKREF_t     * ref;

    memset( &key, 0, sizeof( key ));
    key.blockno = blockno;
    key.bf      = bf;

    /*
     * if there is no hash table yet, the reference is not found
     */
    if( !pblblockhash )
    {
        return( 0 );
    }

    /*
     * see whether we have the reference
     */
    ref = pblHtLookup( pblblockhash, &key, sizeof( key ));
    if( !ref )
    {
        if( pbl_errno == PBL_ERROR_NOT_FOUND )
        {
            pbl_errno = 0;
        }

        return( 0 );
    }

    return( ref->block );
}

/*
 * BIGFILE functions
 */
/*
 * find a filehandle of a filesystem file
 */
static PBLBIGFILEHANDLE_t * pbf_fh_find( int bf, int n )
{
    PBLBIGFILEHANDLE_t * entry;

    /*
     * file system handles are kept in an LRU list
     */
    for( entry = pbf_ft_head; entry; entry = entry->next )
    {
        if( bf == entry->bf && n == entry->n )
        {
            if( entry != pbf_ft_head )
            {
                PBL_LIST_UNLINK( pbf_ft_head, pbf_ft_tail, entry, next, prev );
                PBL_LIST_PUSH( pbf_ft_head, pbf_ft_tail, entry, next, prev );
            }
            return( entry );
        }
    }

    return( 0 );
}

/*
 * close files with the filesystem
 */
static void pbf_fh_close( int bf, int n )
{
    PBLBIGFILEHANDLE_t * entry;
    PBLBIGFILEHANDLE_t * tmp;

    if( n < 0 )
    {
        /*
         * close all file handles of this big file
         */
        for( entry = pbf_ft_head; entry; )
        {
            tmp = entry;
            entry = entry->next;

            if( bf == tmp->bf )
            {
                close( tmp->fh );
                PBL_LIST_UNLINK( pbf_ft_head, pbf_ft_tail, tmp, next, prev );
                PBL_FREE( tmp );
            }
        }

        return;
    }

    /*
     * close a particular file handle
     */
    entry = pbf_fh_find( bf, n );
    if( !entry )
    {
        return;
    }

    close( entry->fh );

    PBL_LIST_UNLINK( pbf_ft_head, pbf_ft_tail, entry, next, prev );
    PBL_FREE( entry );
}

/*
 * create the bigfile path of a file
 * bigfiles are stored in multiple filesystem files
 * if the first one is called "file.ext",
 * the others have names like "file_0002.ext" etc.
 */
static char * pbf_fh_path( char * name, long n )
{
    char * path;
    char * dotptr;

    if( n < 1 )
    {
        /*
         * the name of the first file does not change
         */
        path = strdup( name );
        if( !path )
        {
            pbl_errno = PBL_ERROR_OUT_OF_MEMORY;
            return( 0 );
        }
        return( path );
    }

    path = pbl_malloc( "pbf_fh_path path", 6 + strlen( name ));
    if( !path )
    {
        return( 0 );
    }

    /*
     * see if there is a ".ext" after the last slash
     */
    dotptr = strrchr( name, '.' );
    if( dotptr )
    {
        if( strchr( dotptr, '/' ) || strchr( dotptr, '\\' ))
        {
            dotptr = 0;
        }
    }

    /*
     * build the filename, start counting at one
     */
    n++;
    if( dotptr )
    {
        memcpy( path, name, dotptr - name );
        snprintf( path + ( dotptr - name ), 6, "_%04lx", 0xffff & n );
        strcat( path, dotptr );
    }
    else
    {
        strcpy( path, name );
        snprintf( path + strlen( path ), 6, "_%04lx", 0xffff & n );
    }

    return( path );
}

/*
 * open a file with the file system
 */
static int pbf_fh_open( char * name, int mode, int bf, int n )
{
    PBLBIGFILEHANDLE_t * entry;
    int                  fh = -1;
    int                  i;
    char               * path;

    /*
     * look in LRU list of open filesystem files
     */
    entry = pbf_fh_find( bf, n );
    if( entry )
    {
        if( entry->mode != mode )
        {
            /*
             * the file was found, but the open mode is wrong, close the file
             */
            pbf_fh_close( bf, n );
            entry = 0;
        }
        else
        {
            /*
             * the file is open in the right mode, use the file handle
             */
            return( entry->fh );
        }
    }

    /*
     * open the file
     */
    path = pbf_fh_path( name, n );
    if( !path )
    {
        return( -1 );
    }

    for( i = 0; i < 3; i++ )
    {
        fh = open( path, mode, S_IREAD | S_IWRITE );
        if( -1 == fh && pbf_ft_tail )
        {
            /*
             * maybe the process or the system ran out of filehandles
             * close one file and try again
             */
            pbf_fh_close( pbf_ft_tail->bf, pbf_ft_tail->n );
            continue;
        }

        break;
    }
    PBL_FREE( path );

    if( -1 == fh )
    {
        pbl_errno = PBL_ERROR_OPEN;
        return( -1 );
    }

    /*
     * create and link the file handle list entry
     */
    entry = pbl_malloc0( "pbf_fh_open *entry", sizeof( *entry ));
    if( !entry )
    {
        close( fh );
        return( 0 );
    }

    entry->fh   = fh;
    entry->mode = mode;
    entry->bf   = bf;
    entry->n    = n;

    PBL_LIST_PUSH( pbf_ft_head, pbf_ft_tail, entry, next, prev );

    return( entry->fh );
}

/*
 * close a bigfile
 */
static int pbf_close( int bf )
{
    if( bf < 0 || bf >= PBL_NFILES )
    {
        return( -1 );
    }

    /*
     * close all filesystem files
     */
    pbf_fh_close( bf, -1 );

    PBL_FREE( pbf_pool[ bf ].name );
    pbf_pool[ bf ].name = 0;

    return( 0 );
}

/*
 * open a bigfile
 */
static int pbf_open(
char * name,
int    update,
long   blocksperfile,
long   blocksize
)
{
    int        fh = -1;
    int        mode;
    int        i;
    char     * path;

    path = pbf_fh_path( name, 0 );
    if( !path )
    {
        return( -1 );
    }

    if( update )
    {
        mode = O_CREAT | O_BINARY | O_RDWR;
    }
    else
    {
        mode = O_BINARY | O_RDONLY;
    }

    /*
     * find a slot in the big file pool that is free
     */
    for( i = 0; i < PBL_NFILES; i++ )
    {
        if( pbf_pool[ i ].name )
        {
            continue;
        }

        /*
         * reserve the slot
         */
        pbf_pool[ i ].mode = mode;
        pbf_pool[ i ].nextblockno = -1;
        pbf_pool[ i ].blocksperfile = blocksperfile;
        pbf_pool[ i ].blocksize = blocksize;

        /*
         * open the first filesystem file
         */
        fh = pbf_fh_open( path, mode, i, 0 );
        if( -1 == fh )
        {
            PBL_FREE( path );
            pbl_errno = PBL_ERROR_OPEN;
            return( -1 );
        }

        pbf_pool[ i ].name = path;
        return( i );
    }

    PBL_FREE( path );
    pbl_errno = PBL_ERROR_OPEN;
    return( -1 );
}

/*
 * file io for a bigfile
 */
static int pbf_blockio(
int             bf,
int             blockwrite,
long            blockno,
unsigned char * buffer
)
{
    long       rc = -1;
    long       n;
    int        fh;
    int        i;
    int        j;
    long       offset;

    if( bf < 0 || bf >= PBL_NFILES || !pbf_pool[ bf ].name )
    {
        pbl_errno = PBL_ERROR_BAD_FILE;
        return( -1 );
    }

    /*
     * make sure the n'th filesystem file is open
     */
    n = blockno / pbf_pool[ bf ].blocksperfile;

    fh = pbf_fh_open( pbf_pool[ bf ].name, pbf_pool[ bf ].mode, bf, n );
    if( -1 == fh )
    {
        pbl_errno = PBL_ERROR_READ;
        return( -1 );
    }

    blockno %= pbf_pool[ bf ].blocksperfile;
    offset = blockno * pbf_pool[ bf ].blocksize;

    if( blockwrite )
    {
        /*
         * try the write more than once if needed
         */
        for( i = 0; i < 3; i++ )
        {
            /*
             * try the seek more than once if needed
             */
            for( j = 0; j < 3; j++ )
            {
                rc = lseek( fh, offset, SEEK_SET );
                if( offset != rc )
                {
                    continue;
                }
                break;
            }
            if( offset != rc )
            {
                pbl_errno = PBL_ERROR_SEEK;
                return( -1 );
            }

            rc = write( fh, buffer, (unsigned int) pbf_pool[ bf ].blocksize );
            if( rc != pbf_pool[ bf ].blocksize )
            {
                if( errno == EINTR )
                {
                    continue;
                }
                pbl_errno = PBL_ERROR_WRITE;
                return( -1 );
            }
            break;
        }
        if( i >= 3 )
        {
            pbl_errno = PBL_ERROR_WRITE;
            return( -1 );
        }

        pblnwrites++;
    }
    else
    {
        /*
         * try the read more than once if needed
         */
        for( i = 0; i < 3; i++ )
        {
            /*
             * try the seek more than once if needed
             */
            for( j = 0; j < 3; j++ )
            {
                rc = lseek( fh, offset, SEEK_SET );
                if( offset != rc )
                {
                    continue;
                }
                break;
            }
            if( offset != rc )
            {
                pbl_errno = PBL_ERROR_SEEK;
                return( -1 );
            }

            rc = read( fh, buffer, (unsigned int) pbf_pool[ bf ].blocksize );
            if( rc < 0 )
            {
                if( errno == EINTR )
                {
                    continue;
                }
                pbl_errno = PBL_ERROR_READ;
                return( -1 );
            }
            pblnreads++;

            if( rc != pbf_pool[ bf ].blocksize )
            {
                if( errno == EINTR )
                {
                    continue;
                }
                pbl_errno = PBL_ERROR_BAD_FILE;
                return( -1 );
            }
            break;
        }
        if( i >= 3 )
        {
            pbl_errno = PBL_ERROR_READ;
            return( -1 );
        }
    }

    return( 0 );
}

/*
 * read a block from a bigfile
 */
static int pbf_blockread( int bf, long blockno, unsigned char * buffer )
{
    int        rc;

    rc = pbf_blockio( bf, 0, blockno, buffer );

    return( rc );
}

/*
 * write a block to a bigfile
 */
static int pbf_blockwrite( int bf, long blockno, unsigned char * buffer )
{
    int        rc;

    rc = pbf_blockio( bf, 1, blockno, buffer );

    return( rc );
}

/*
 * append a block to a bigfile
 */
static long pbf_blockappend( int bf, unsigned char * buffer )
{
    int           fh;
    long          offset;
    int           i;
    int           j;
    long          rc = -1;
    unsigned char c;

    if( bf < 0 || bf >= PBL_NFILES || !pbf_pool[ bf ].name )
    {
        pbl_errno = PBL_ERROR_BAD_FILE;
        return( -1 );
    }

    if( pbf_pool[ bf ].nextblockno == 0x3fff )
    {
        rc = -1;
    }

    /*
     * if we don't know the next free block of the file yet
     */
    if( pbf_pool[ bf ].nextblockno < 0 )
    {
        /*
         * find the next free block of a bigfile
         */
        for( i = 0; 1; i++ )
        {
            /*
             * create and open next filesytem file used for the bigfile
             */
            fh = pbf_fh_open( pbf_pool[ bf ].name, pbf_pool[ bf ].mode, bf, i );
            if( -1 == fh )
            {
                pbl_errno = PBL_ERROR_WRITE;
                return( -1 );
            }

            /*
             * reopen the filesystem file read only
             */
            fh = pbf_fh_open( pbf_pool[ bf ].name, O_BINARY | O_RDONLY, bf, i );
            if( -1 == fh )
            {
                pbl_errno = PBL_ERROR_WRITE;
                return( -1 );
            }

            /*
             * seek to the end of the file
             */
            offset = pbf_pool[ bf ].blocksperfile * pbf_pool[ bf ].blocksize -1;

            for( j = 0; j < 3; j++ )
            {
                rc = lseek( fh, offset, SEEK_SET );
                if( offset != rc )
                {
                    if( errno == EINTR )
                    {
                        continue;
                    }
                    rc = -1;
                    break;
                }

                /*
                 * check whether the block exist, by reading its last byte
                 */
                rc = read( fh, &c, 1 );
                if( rc < 1 )
                {
                    if( errno == EINTR )
                    {
                        continue;
                    }
                }
                break;
            }
                
            if( rc == 1 )
            {
                /*
                 * we can read the last byte of the block, it is not free
                 */
                continue;
            }

            /*
             * we found the last filesystem file used for the bigfile
             * seek to the end of the file
             */
            for( j = 0; j < 3; j++ )
            {
                rc = lseek( fh, 0, SEEK_END );
                if( rc < 0 )
                {
                    if( errno == EINTR )
                    {
                        continue;
                    }

                    pbl_errno = PBL_ERROR_WRITE;
                    return( -1 );
                }
                break;
            }

            if( rc % pbf_pool[ bf ].blocksize )
            {
                pbl_errno = PBL_ERROR_BAD_FILE;
                return( -1 );
            }

            pbf_pool[ bf ].nextblockno = rc / pbf_pool[ bf ].blocksize;
            break;
        }
    }

    /*
     * append the block to the bigfile
     */
    rc = pbf_blockio( bf, 1, pbf_pool[ bf ].nextblockno, buffer );
    if( rc )
    {
        return( rc );
    }

    return( pbf_pool[ bf ].nextblockno++ );
}

/*
 * ITEM functions
 *
 * Layout of an item in the data of a block with a level greater than 0
 *
 * LENGTH    NAME        SEMANTICS
 *
 * 1        KEYLEN        length of the key of the item
 * 1        KEYCOMMON     number of bytes the key has in common with
 *                        the key of the predecessor of the item on the block
 * VARLONG  DATABLOCK     block number of block this item points to
 * KEYLEN   KEY           the key itself, only the last KEYLEN - KEYCOMMON
 *                        bytes are stored
 *
 * Layout of an item in the data of a block with a level 0 and
 * with the datalen of the item smaller than or equal to PBLDATALENGTH
 *
 * LENGTH    NAME        SEMANTICS
 *
 * 1         KEYLEN       length of the key of the item
 * 1         KEYCOMMON    number of bytes the key has in common with
 *                        the key of the predecessor of the item on the block
 * VARLONG   DATALEN      length of the data stored on the block
 * KEYLEN    KEY          the key itself, only the last KEYLEN - KEYCOMMON
 *                        bytes are stored
 * DATALEN   DATA         the data is stored on the block
 *
 * Layout of an item in the data of a block with a level 0 and
 * with the datalen of the item greater than PBLDATALENGTH
 *
 * LENGTH    NAME        SEMANTICS
 *
 * 1         KEYLEN       length of the key of the item
 * 1         KEYCOMMON    number of bytes the key has in common with
 *                        the key of the predecessor of the item on the block
 * VARLONG   DATALEN      length of the data stored on the datablock
 * KEYLEN    KEY          the key itself, only the last KEYLEN - KEYCOMMON
 *                        bytes are stored
 * VARLONG   DATABLOCK    block number of block data is stored on
 * VARLONG   DATAOFFSET   offset of the data on that block
 *
 * The long values stored for an item, DATALEN, DATABLOCK and DATAOFFSET
 * are stored as variable length buffers, i.e. the number of bytes
 * used in the file for the values depends on their numerical value.
 * See the VarBuf* functions for details.
 *
 * The smallest item of an inner block always has KEYLEN 0, which makes
 * it's key logically smaller than any other possible key of an item.
 *
 * The items stored on a block start at address ( block->data + PBLHEADERSIZE ).
 * The items are always stored immediately one after the other starting at
 * at this address, we leave no space in between.
 *
 * As the keys of the items and therefore the items themselves can have
 * different lengths, we store the relative offsets of the items in the
 * data of a block also in the data of the block. Those relative offsets
 * are stored as two byte unsigned shorts at the end of the data of the block.
 * The relative offsets are called slots in the code below.
 *
 * The slots in the slot array are kept in order.
 *
 * For every block the short block->free gives the relative offset of the first
 * free byte of the block, immediately after the end of the last item stored.
 *
 * Before we append an item we check if there is enough space for the item
 * and its slot between the end of the last item stored and the beginning
 * of the slot array of the items stored. If not, PBL_ERROR_NOFIT is given
 * to the pblinsert procedure which then has to split the block.
 *
 * During deletion we pack the ordered array of the slots
 * and the items themselves by shifting them on the block.
 *
 * The number of bytes the key of an item has in common with the key
 * of the predecessor of the item is stored with each item.
 * This is done in order to minimize the space needed for keys.
 *
 * EG: If a key is "New York" and its predecessor is "New Haven",
 *     only the string "York" is stored for the second key together
 *     with one byte indicating that the key has the first 4 bytes
 *     with its predecessor, the key "New Haven".
 * 
 * Whenever the full keys are needed for the items of a block,
 * the keys of the items of the block have to be "expanded" first.
 */

/*
 * The following three static procedures are the only ones that 'know' the
 * layout of an item stored on a block
 */
static void pblItemToBuf( PBLBLOCK_t * block, int bufoff, PBLITEM_t * item )
{
    unsigned char * ptr = &( block->data[ bufoff ] );

    *ptr++ = ( unsigned char ) item->keylen;
    *ptr++ = ( unsigned char ) item->keycommon;

    if( block->level > 0 )
    {
        ptr += pbl_LongToVarBuf( ptr, item->datablock );
    }
    else
    {
        ptr += pbl_LongToVarBuf( ptr, item->datalen );
    }

    if( item->keylen - item->keycommon > 0 )
    {
        /*
         * make sure we don't copy in place
         */
        if( ptr != item->key + item->keycommon )
        {
            pbl_memlcpy( ptr, PBLKEYLENGTH,
                         item->key + item->keycommon,
                         item->keylen - item->keycommon );
        }
        ptr += item->keylen - item->keycommon;
    }

    /*
     * the block needs to be written to the filesystem
     */
    block->dirty = 1;

    if( block->level > 0 )
    {
        return;
    }

    if( item->datalen <= PBLDATALENGTH )
    {
        if( item->datalen > 0 )
        {
            if( ptr != item->data )
            {
                memcpy( ptr, item->data, item->datalen );
            }
        }
    }
    else
    {
        ptr += pbl_LongToVarBuf( ptr, item->datablock );
        pbl_LongToVarBuf( ptr, item->dataoffset );
    }
}

static int pblBufToItem( PBLBLOCK_t * block, int bufoff, PBLITEM_t * item )
{
    unsigned char * ptr;
    unsigned char * endptr;

    item->level = block->level;

    /*
     * make sure the offset is in bounds
     */
    if( bufoff >= sizeof( block->data ))
    {
        pbl_errno = PBL_ERROR_BAD_FILE;
        return( -1 );
    }
    endptr = &( block->data[ sizeof( block->data ) ] );
    ptr = &( block->data[ bufoff ] );

    /*
     * parse the item
     */
    item->keylen = *ptr++;
    item->keycommon = *ptr++;

    if( block->level > 0 )
    {
        ptr += pbl_VarBufToLong( ptr, &( item->datablock ));
        item->datalen = 0;
    }
    else
    {
        ptr += pbl_VarBufToLong( ptr, &( item->datalen ));
        item->datablock = 0;
    }

    if( ptr >= endptr )
    {
        pbl_errno = PBL_ERROR_BAD_FILE;
        return( -1 );
    }

    if( item->keylen - item->keycommon > 0 )
    {
        item->key = ptr;
        ptr += item->keylen - item->keycommon;
        if( ptr >= endptr )
        {
            pbl_errno = PBL_ERROR_BAD_FILE;
            return( -1 );
        }
    }
    else
    {
        item->key = 0;
    }

    if( block->level > 0 )
    {
        item->dataoffset = 0;
        item->data = 0;

        return( 0 );
    }

    if( item->datalen <= PBLDATALENGTH )
    {
        item->dataoffset = 0;
        if( item->datalen > 0 )
        {
            item->data = ptr;
            ptr += item->datalen;
            if( ptr >= endptr )
            {
                pbl_errno = PBL_ERROR_BAD_FILE;
                return( -1 );
            }
        }
        else
        {
            item->data = 0;
        }
    }
    else
    {
        ptr += pbl_VarBufToLong( ptr, &( item->datablock ));
        pbl_VarBufToLong( ptr, &( item->dataoffset ));
        item->data = 0;
    }

    if( ptr >= endptr )
    {
        pbl_errno = PBL_ERROR_BAD_FILE;
        return( -1 );
    }
    return( 0 );
}

static int pblItemSize( PBLBLOCK_t * block, PBLITEM_t * item )
{
    int rc = 2;

    if( block->level > 0 )
    {
        rc += pbl_LongSize( item->datablock );
    }
    else
    {
        rc += pbl_LongSize( item->datalen );
    }

    if( item->keylen - item->keycommon > 0 )
    {
        rc += item->keylen - item->keycommon;
    }

    if( block->level > 0 )
    {
        return( rc );
    }

    if( item->datalen <= PBLDATALENGTH )
    {
        if( item->datalen > 0 )
        {
            rc += item->datalen;
        }
    }
    else
    {
        rc += pbl_LongSize( item->datablock );
        rc += pbl_LongSize( item->dataoffset );
    }

    return( rc );
}

/*
 * compare two items
 */
static int pblItemCompare( PBLKFILE_t *kf, PBLITEM_t *left, PBLITEM_t *right )
{
    int rc;

    if( kf->keycompare )
    {
        /*
         * there is a specific compare function for the key file
         */
        rc = (*kf->keycompare)( left->key, left->keylen,
                                right->key, right->keylen );
    }
    else
    {
        /*
         * use the default key compare function
         */
        rc = pbl_memcmp( left->key, left->keylen,
                         right->key, right->keylen );
    }

    return( rc );
}

static int pblItemGet( PBLBLOCK_t * block, unsigned int index, PBLITEM_t *item )
{
    /*
     * if there is no item with required index
     */
    if( index >= (unsigned int)block->nentries )
    {
        pbl_errno = PBL_ERROR_BAD_FILE;
        return( -1 );
    }

    /*
     * copy the item with the required index
     */
    if( pblBufToItem( block, PBLINDEXTOBUFOFF( block, index ), item ))
    {
        return( -1 );
    }

    /*
     * if we have the expanded keys for the block we
     * actually use those keys
     */
    if( item->keycommon > 0 && block->expandedkeys )
    {
        item->key = block->expandedkeys + index * PBLKEYLENGTH;
    }

    return( 0 );
}

/*
 * append an item to a block
 */
static int pblItemAppend(
PBLBLOCK_t      * block,
unsigned char   * predkey,
unsigned int      predkeylen,
PBLITEM_t       * item
)
{
    unsigned char * ptr;
    unsigned int    itemsize;

    if( !block->writeable )
    {
        pbl_errno = PBL_ERROR_PROGRAM;
        return( -1 );
    }

    if( !predkey && block->nentries > 0 )
    {
        pbl_errno = PBL_ERROR_PROGRAM;
        return( -1 );
    }

    /*
     * calculate how many bytes the key of the item has in common
     * with the key of the predecessor on the block
     */
    if( predkey && ( predkeylen > 0 ))
    {
        item->keycommon = pbl_memcmplen( predkey, predkeylen,
                                         item->key, item->keylen );
    }
    else
    {
        item->keycommon = 0;
    }

    /*
     * calculate the size the item needs on the block
     */
    itemsize = pblItemSize( block, item );

    /*
     * check if the item fits here, the "+ 2" is for the slot!
     */
    if( PBLINDEXBLOCKNFREE( block ) < itemsize + 2 )
    {
        pbl_errno = PBL_ERROR_NOFIT;
        return( -1 );
    }

    /*
     * put item to data part of block
     */
    pblItemToBuf( block, block->free, item );

    /*
     * put the slot to the block
     */
    ptr = PBLINDEXTOPTR( block, block->nentries );
    pbl_ShortToBuf( ptr, block->free );

    block->free     += itemsize;
    block->nentries += 1;

    if( block->expandedkeys )
    {
        pblBlockKeysRelease( block );
    }

    return( 0 );
}

/*
 * delete an item from a block
 */
static int pblItemDelete( PBLBLOCK_t * block, int index )
{
    PBLITEM_t       item;
    int             ntomove;
    unsigned int    i;
    int             offset;
    int             itemoffset;
    unsigned char * ptr;
    unsigned int    itemsize;

    /*
     * read the item to delete
     */
    if( pblItemGet( block, index, &item ))
    {
        return( -1 );
    }

    /*
     * calculate the size the item ocupies on the block
     */
    itemsize = pblItemSize( block, &item );

    /*
     * calculate the number of items that have to be moved
     */
    itemoffset = PBLINDEXTOBUFOFF( block, index );
    ptr = block->data + itemoffset;
    ntomove = block->free - ( itemoffset + itemsize );
    if( ntomove < 0 )
    {
        pbl_errno = PBL_ERROR_BAD_FILE;
        return( -1 );
    }

    if( ntomove > 0 )
    {
        /*
         * move the other items to the left
         */
        memmove( ptr, ptr + itemsize, ntomove );

        /*
         * the slots who's offsets are to the right of the deleted item
         * have to change, because the items where moved
         */
        for( i = 0; i < block->nentries; i++ )
        {
            offset = PBLINDEXTOBUFOFF( block, i );
            if( offset > itemoffset )
            {
                offset -= itemsize;
                ptr = PBLINDEXTOPTR( block, i );
                pbl_ShortToBuf( ptr, offset );
            }
        }
    }

    /*
     * blank out the bytes deleted
     */
    memset(( block->data + block->free ) - itemsize, 0, itemsize );

    /*
     * there is more free space on the block now
     */
    block->free -= itemsize;

    /*
     * delete the slot from the slots array
     */
    ptr = PBLINDEXTOPTR( block, block->nentries - 1 );
    if( index < (int)block->nentries - 1 )
    {
        ntomove = ( block->nentries - 1 ) - index;
        memmove( ptr + 2, ptr, ntomove * 2 );
    }

    /*
     * blank out the last slot
     */
    *ptr++ = 0;
    *ptr   = 0;

    /*
     * there is one less slot on the block
     */
    block->nentries -= 1;

    if( block->expandedkeys )
    {
        pblBlockKeysRelease( block );
    }

    block->dirty = 1;
    return( 0 );
}

/*
 * insert an item on a block before the item with a given index
 */
static int pblItemInsert( PBLBLOCK_t * block, PBLITEM_t * item, int index )
{
    int             ntomove;
    unsigned char * ptr;
    unsigned int    itemsize;

    /*
     * calculate the size the item needs on the block
     */
    itemsize = pblItemSize( block, item );

    /*
     * check if the item fits here, the "+ 2" is for the slot!
     */
    if( PBLINDEXBLOCKNFREE( block ) < itemsize + 2 )
    {
        pbl_errno = PBL_ERROR_NOFIT;
        return( -1 );
    }

    /*
     * put item to data part of block
     */
    pblItemToBuf( block, block->free, item );

    /*
     * move the slots of the items after index
     */
    ptr = PBLINDEXTOPTR( block, block->nentries );
    ntomove = block->nentries - index;
    if( ntomove > 0 )
    {
        memmove( ptr, ptr + 2, 2 * ntomove );
    }

    /*
     * put the slot to the slots array
     */
    ptr = PBLINDEXTOPTR( block, index );
    pbl_ShortToBuf( ptr, block->free );

    block->free     += itemsize;
    block->nentries += 1;

    if( block->expandedkeys )
    {
        pblBlockKeysRelease( block );
    }

    block->dirty = 1;
    return( 0 );
}

/*
 * remove an item from a block
 */
static int pblItemRemove( PBLBLOCK_t *block, unsigned int index )
{
    PBLITEM_t         peeritem;
    PBLITEM_t         previtem;
    unsigned char     data[ PBLDATALENGTH ];
    unsigned char     savekey[ PBLKEYLENGTH ];
    int               rc;
    int               keycommon;
    int               dodelete = 0;

    if( !block->writeable )
    {
        pbl_errno = PBL_ERROR_PROGRAM;
        return( -1 );
    }

    /*
     * if there is no item with required index
     */
    if( index >= block->nentries )
    {
        pbl_errno = PBL_ERROR_BAD_FILE;
        return( -1 );
    }

    /*
     * create the expanded keys of the block
     */
    if( pblBlockKeysExpand( block ))
    {
        return( -1 );
    }

    if( index == block->nentries - 1 )
    {
        /*
         * delete the last item on the block
         */
        rc = pblItemDelete( block, index );
        return( rc );
    }

    /*
     * read the previous item on the block
     */
    if( index > 0 )
    {
        if( pblItemGet( block, index - 1, &previtem ))
        {
            return( -1 );
        }
    }

    /*
     * read the next item on the block
     */
    if( pblItemGet( block, index + 1, &peeritem ))
    {
        return( -1 );
    }

    if(( index > 0 ) && ( previtem.keylen > 0 ))
    {
        keycommon = pbl_memcmplen( previtem.key, previtem.keylen,
                                   peeritem.key, peeritem.keylen );
    }
    else
    {
        keycommon = 0;
    }

    /*
     * if the next item has to change
     */
    if( keycommon != peeritem.keycommon )
    {
        /*
         * delete and reinsert the next item, because its keycommon changed
         */
        dodelete = 1;

        /*
         * set the new keycommon value for the next item
         */
        peeritem.keycommon = keycommon;

        /*
         * save the data of the next item
         */
        if( peeritem.datalen <= PBLDATALENGTH )
        {
            memcpy( data, peeritem.data, peeritem.datalen );
            peeritem.data = data;
        }

        /*
         * save the key of the next item
         */
        memcpy( savekey, peeritem.key, peeritem.keylen );
        peeritem.key = savekey;

        /*
         * delete the next item
         */
        rc = pblItemDelete( block, index + 1 );
        if( rc )
        {
            return( rc );
        }
    }

    /*
     * delete the index'th item
     */
    rc = pblItemDelete( block, index );
    if( rc )
    {
        return( rc );
    }

    if( dodelete )
    {
        /*
         * insert the saved item again
         */
        rc = pblItemInsert( block, &peeritem, index );
    }
    return( rc );
}

/*
 * find an item that has a key equal to the search key
 *
 * - uses a binary search to position to an item on a block
 */
static int pblItemFind(
PBLKFILE_t * kf,
PBLBLOCK_t * block,
PBLITEM_t  * item,
int          which
)
{
    int       found = -1;
    int       index = 0;
    int       left  = 0;
    int       right = block->nentries - 1;
    int       rc    = 1;
    PBLITEM_t curitem;

    while( left <= right )
    {
        index = ( left + right ) / 2;

        if( pblItemGet( block, index, &curitem ))
        {
            return( -1 );
        }

        rc = pblItemCompare( kf, &curitem, item );
        if( rc == 0 )
        {
            switch( which )
            {
              case PBLLT:
                right = index - 1;
                break;

              case PBLLE:
              case PBLFI:
                found = index;
                right = index - 1;
                break;

              case PBLEQ:
                found = index;
                return( found );

              case PBLLA:
              case PBLGE:
                found = index;
                left  = index + 1;
                break;

              case PBLGT:
                left  = index + 1;
                break;
            }
        }
        else if ( rc < 0 )
        {
            switch( which )
            {
              case PBLLT:
              case PBLLE:
                found = index;
                left  = index + 1;
                break;

              case PBLFI:
              case PBLEQ:
              case PBLLA:
              case PBLGE:
              case PBLGT:
                left  = index + 1;
                break;
            }
        }
        else
        {
            switch( which )
            {
              case PBLLT:
              case PBLLE:
              case PBLFI:
              case PBLEQ:
              case PBLLA:
                right = index - 1;
                break;

              case PBLGE:
              case PBLGT:
                found = index;
                right = index - 1;
                break;
            }
        }
    }

    if( found < 0 )
    {
        pbl_errno = PBL_ERROR_NOT_FOUND;
    }

    return( found );
}

static int pblItemAdd( PBLKFILE_t * kf, PBLBLOCK_t * block, PBLITEM_t * item )
{
    PBLITEM_t         previtem;
    PBLITEM_t         peeritem;
    int               rc;
    int               index;
    unsigned char     savekey[ PBLKEYLENGTH ];
    unsigned int      savekeylen;
    unsigned char     data[ PBLDATALENGTH ];
    unsigned int      itemsize;
    int               keycommon;

    if( !block->writeable )
    {
        pbl_errno = PBL_ERROR_PROGRAM;
        return( -1 );
    }

    if( block->nentries < 1 )
    {
        if( pblItemAppend( block, 0, 0, item ))
        {
            return( -1 );
        }
        return( 0 );
    }

    /*
     * create the expanded keys of the block
     */
    if( pblBlockKeysExpand( block ))
    {
        return( -1 );
    }

    /*
     * find the first item that is bigger than the one we insert
     */
    index = pblItemFind( kf, block, item, PBLGT );
    if( index < 0 )
    {
        if( pbl_errno != PBL_ERROR_NOT_FOUND )
        {
            return( -1 );
        }

        /*
         * append to the block
         */
        pbl_errno = 0;
        index = block->nentries;

        if( pblItemGet( block, index - 1, &peeritem ))
        {
            return( -1 );
        }

        savekeylen = peeritem.keylen;
        if( savekeylen > 0 )
        {
            pbl_memlcpy( savekey, sizeof( savekey ), peeritem.key, savekeylen );
        }

        if( pblItemAppend( block, savekey, savekeylen, item ))
        {
            return( -1 );
        }
        return( index );
    }

    /*
     * read the previous item on the block
     */
    if( index > 0 )
    {
        if( pblItemGet( block, index - 1, &previtem ))
        {
            return( -1 );
        }
    }

    /*
     * calculate the number of bytes the key of the item has in
     * common with the key of its predecessor
     */
    if(( index > 0 ) && ( previtem.keylen > 0 ))
    {
        item->keycommon = pbl_memcmplen( previtem.key, previtem.keylen,
                                         item->key, item->keylen );
    }
    else
    {
        item->keycommon = 0;
    }

    /*
     * calculate the size the item needs on the block
     */
    itemsize = pblItemSize( block, item );

    /*
     * check if the item fits here, the "+ 2" is for the slot!
     */
    if( PBLINDEXBLOCKNFREE( block ) < itemsize + 2 )
    {
        pbl_errno = PBL_ERROR_NOFIT;
        return( -1 );
    }

    /*
     * read the next item on the block
     */
    if( pblItemGet( block, index, &peeritem ))
    {
        return( -1 );
    }

    /*
     * calculate the number of bytes the key of the next item has in
     * common with the key of the new item
     */
    if( item->keylen > 0 )
    {
        keycommon = pbl_memcmplen( item->key, item->keylen,
                                   peeritem.key, peeritem.keylen );
    }
    else
    {
        keycommon = 0;
    }

    /*
     * if the next item has to change
     */
    if( keycommon != peeritem.keycommon )
    {
        /*
         * set the new keycommon value for the next item
         */
        peeritem.keycommon = keycommon;

        /*
         * save the data of that item
         */
        if( peeritem.datalen <= PBLDATALENGTH )
        {
            memcpy( data, peeritem.data, peeritem.datalen );
            peeritem.data = data;
        }

        /*
         * save the key of the item
         */
        memcpy( savekey, peeritem.key, peeritem.keylen );
        peeritem.key = savekey;

        /*
         * delete the next item
         */
        rc = pblItemDelete( block, index );
        if( rc )
        {
            return( rc );
        }

        /*
         * insert the saved item again
         */
        rc = pblItemInsert( block, &peeritem, index );
        if( rc )
        {
            return( rc );
        }
    }

    /*
     * insert the new item
     */
    rc = pblItemInsert( block, item, index );
    if( rc )
    {
        return( rc );
    }

    return( index );
}

/*
 * BLOCK functions
 *
 * The size of a block in the file is PBLDATASIZE
 *
 * The layout is as follows:
 *
 * OFFSET    NAME    SEMANTICS
 *
 * 0        LEVEL    Level of the block in the tree
 *                   if 0:      the block is a leaf node
 *                   if 254:    the block is a free block, can be reused
 *                   if 255:    the block is a data block, not belonging to
 *                              the tree.
 *                   otherwise: block is an inner node of the tree
 *
 * 1  -  4 NOFFSET   block number of next block of same level, as the root block
 *                   always is the only block of it's level, the block number
 *                   of the last data block is stored here, we use this block
 *                   for appending data if necessary.
 *
 * 5  -  8 POFFEST   file offset of previous block of the same level,
 *                   as the root block always is the only block of it's level,
 *                   the block number of the first free block with level 254
 *                   is stored on the root block
 *
 * 9  - 10 NENTRIES  number of items stored in the data of the block.
 *                   always 0 for data blocks.
 *
 * 11 - 12 FREE      relative offset of first free byte in the data of the block
 *
 * 13 - ( PBLDATASIZE - 1 ) data area of the block used for storing items on
 *                          tree blocks ( level 0 - 253 ) or used for storing
 *                          the data of the items on data blocks ( level 255 ).
 *
 * The root block of the tree is always stored at file offset 0. The first data
 * block at file offset PBLDATASIZE. There are at least two blocks in a file
 * even if there are no items stored at all.
 *
 * For records with a datalen of less or equal to PBLDATALENGTH characters
 * the data is stored on the level 0 index blocks. For records with
 * data longer than PBLDATALENGTH characters the data is stored on data blocks.
 *
 * This is done in order to keep the height of the tree small and to allow
 * data lengths of more than PBLDATASIZE bytes. What we pay for this is that
 * the space in the file used for the data of records stored on data blocks
 * and that are deleted or updated is lost.
 *
 * Blocks read from disk are cached in an LRU list, references to blocks
 * in that list are kept in a hash table in order to speed up access.
 *
 * Blocks changed during a transaction are kept in the writeable block
 * list. If a cached block that is dirty has to be made writeable a copy
 * of the block is created in the writeable list, if the block is not
 * dirty the block is moved from the cached list to the writeable list
 * without creating a copy.
 *
 * During a transaction blocks from the writeable list take precedence
 * over the copy of the same block from the cached list.
 *
 * During a rollback all blocks from the writeable list are discarded,
 * thus reverting the file to the state before the beginning of the 
 * transaction.
 *
 * During a commit all blocks are moved from the writeable list to the
 * cached list.
 */

/*
 * The following two procedures are the only ones that 'know' the layout
 * of a the data of a block in the file
 */
static void pblDataToBlock( PBLBLOCK_t * block )
{
    block->data[ 0 ] = block->level;

    pbl_LongToBuf ( &( block->data[ 1 ]),  block->nblock );
    pbl_LongToBuf ( &( block->data[ 5 ]),  block->pblock );
    pbl_ShortToBuf( &( block->data[ 9 ]),  block->nentries );
    pbl_ShortToBuf( &( block->data[ 11 ]), block->free );
}

static void pblBlockToData( PBLBLOCK_t * block )
{
    block->level = block->data[ 0 ];

    block->nblock   = pbl_BufToLong ( &( block->data[ 1 ]));
    block->pblock   = pbl_BufToLong ( &( block->data[ 5 ]));
    block->nentries = pbl_BufToShort( &( block->data[ 9 ]));
    block->free     = pbl_BufToShort( &( block->data[ 11 ]));
}

/*
 * release all memory blocks of a file
 */
static void pblBlocksRelease( int bf )
{
    PBLBLOCK_t * block;

    /*
     * all blocks that were belonging to the file are marked unused
     */
    for( block = blockListHead; block; block = block->next )
    {
        if( block->bf == bf )
        {
            pblBlockHashRemove( block->blockno, block->bf );
            pblBlockKeysRelease( block );
            block->bf         = -1;
            block->filesettag = NULL;
        }
    }
    return;
}

/*
 * release the expanded keys buffer of a block
 */
static void pblBlockKeysRelease( PBLBLOCK_t * block )
{
    PBLBLOCKLINK_t * blink;

    if( block->expandedkeys )
    {
        blink = ( PBLBLOCKLINK_t * )block;

        PBL_LIST_UNLINK( linkListHead, linkListTail, blink, next, prev );
        pblnlinks--;

        PBL_FREE( block->expandedkeys );

        block->expandedkeys = 0;
    }
}

/*
 * expand all keys of a block
 */
static int pblBlockKeysExpand( PBLBLOCK_t * block )
{
    unsigned int      i;
    PBLITEM_t         item;
    unsigned char   * expandedkeys = 0;
    PBLBLOCKLINK_t  * blink;

    if( block->expandedkeys )
    {
        blink = ( PBLBLOCKLINK_t * )block;

        /*
         * make the block link the first in the LRU chain
         */
        if( blink != linkListHead )
        {
            PBL_LIST_UNLINK( linkListHead, linkListTail, blink, next, prev );
            PBL_LIST_PUSH( linkListHead, linkListTail, blink, next, prev );
        }
        return( 0 );
    }

    /*
     * get space for the expanded keys of the block
     */
    if( block->nentries > 0 )
    {
        i = block->nentries * PBLKEYLENGTH;
    }
    else
    {
        i = 1;
    }

    expandedkeys = pbl_malloc( "pblBlockKeysExpand expandedkeys", i );
    if( !expandedkeys )
    {
        return( -1 );
    }

    /*
     * run through all items of the block
     */
    for( i = 0; i < block->nentries; i++ )
    {
        pblItemGet( block, i, &item );

        if( item.keylen < 1 )
        {
            continue;
        }

        if( item.keycommon < 1 || i < 1 )
        {
            /*
             * this item does not have bytes in common with the predecessor
             */
            pbl_memlcpy( expandedkeys + i * PBLKEYLENGTH,
                         PBLKEYLENGTH, item.key, item.keylen );
        }
        else
        {
            /*
             * copy the bytes the key has in common with
             * the key of the predecessor
             */
            pbl_memlcpy( expandedkeys + i * PBLKEYLENGTH, PBLKEYLENGTH,
                         expandedkeys + ( i - 1 ) * PBLKEYLENGTH,
                         item.keycommon );

            if( item.keylen - item.keycommon > 0 )
            {
                /*
                 * copy the bytes that are unique for the key
                 */
                pbl_memlcpy( expandedkeys + i * PBLKEYLENGTH + item.keycommon,
                             PBLKEYLENGTH, item.key,
                             item.keylen - item.keycommon );
            }
        }
    }

    block->expandedkeys = expandedkeys;

    /*
     * release some expanded key structures if there are too many
     */
    while( pblnlinks > 8 + ( pblexpandedperfile * pblnfiles ))
    {
        pblBlockKeysRelease( ( PBLBLOCK_t * )linkListTail );
    }

    /*
     * link the block to the list of blocks that have expanded keys
     */
    blink = ( PBLBLOCKLINK_t * )block;
    PBL_LIST_PUSH( linkListHead, linkListTail, blink, next, prev );
    pblnlinks++;

    return( 0 );
}

static int pblBlockWrite( PBLBLOCK_t * block )
{
    long rc;

    /*
     * prepare the block data for writing
     */
    pblDataToBlock( block );

    /*
     * write the block to the big file
     */
    rc = pbf_blockwrite( block->bf, block->blockno, block->data );
    if( rc < 0 )
    {
        block->bf         = -1;
        block->filesettag = NULL;
        pbl_errno = PBL_ERROR_WRITE;
        return( -1 );
    }

    return( 0 );
}

static int pblBlockFlush( int bf, int release )
{
    PBLBLOCK_t * block;
    PBLBLOCK_t * tmp;

    for( tmp = blockListHead; tmp; )
    {
        /*
         * move through the list of blocks before handling this one
         */
        block = tmp;
        tmp = tmp->next;

        if( block->bf != bf )
        {
            continue;
        }

        /*
         * if a file set tag is set for the block we flush
         * all blocks having the same tag set
         */
        if( block->dirty && block->filesettag )
        {
            PBLBLOCK_t * b;

            /*
             * run through all blocks on all files in the set and write them
             */
            for( b = blockListHead; b; b = b->next )
            {
                if( b->dirty && b->bf >= 0
                 && ( block->filesettag == b->filesettag ))
                {
                    if( pblBlockWrite( b ))
                    {
                        pblBlocksRelease( b->bf );
                        break;
                    }
                    else
                    {
                        b->dirty = 0;
                    }
                }
            }
        }
                
        if( block->dirty )
        {         
            if( pblBlockWrite( block ))
            {
                /*
                 * this write always is a rewrite of an existing block
                 * therefore a write error is a strange condition,
                 * we unlink all blocks from the file
                 * most likely the file is inconsistent after that !!!!
                 */
                pblBlocksRelease( bf );
                return( -1 );
            }
            else
            {
                block->dirty = 0;
            }
        }

        if( release )
        {
            pblBlockHashRemove( block->blockno, block->bf );
            pblBlockKeysRelease( block );
            block->bf         = -1;
            block->filesettag = NULL;

            /*
             * put the block to the end of the LRU list
             */
            if( block != blockListTail )
            {
                PBL_LIST_UNLINK( blockListHead, blockListTail,
                                 block, next, prev );
                PBL_LIST_APPEND( blockListHead, blockListTail,
                                 block, next, prev );
            }
        }
    }
    return( 0 );
}

static PBLBLOCK_t * pblBlockGetVictim( PBLKFILE_t * file )
{
    int          rc;
    PBLBLOCK_t * block;

    /*
     * if we have not exceeded the number of blocks we can have at most
     * or of the last block in the LRU chain is dirty and we are updating
     */
    if(( pblnblocks < 8 + ( pblblocksperfile * pblnfiles ) )
     ||( blockListTail && blockListTail->dirty
      && blockListTail->bf != -1 && file->writeableListHead ))
    {
        block = pbl_malloc0( "pblBlockGetVictim BLOCK", sizeof( PBLBLOCK_t ));
        if( !block )
        {
            return( ( PBLBLOCK_t * ) 0 );
        }
        pblnblocks++;
        PBL_LIST_PUSH( blockListHead, blockListTail, block, next, prev );
    }
    else
    {
        /*
         * we reuse the last block in the LRU chain
         */
        if( blockListTail )
        {
            if( blockListTail->bf != -1 )
            {
                /*
                 * flush the block to disk if it is dirty
                 */
                if( blockListTail->dirty )
                {
                    rc = pblBlockFlush( blockListTail->bf, 0 );
                    if( rc )
                    {
                        return( ( PBLBLOCK_t * ) 0 );
                    }
                }

                /*
                 * remove the reference to the block from the hash table
                 */
                pblBlockHashRemove( blockListTail->blockno, blockListTail->bf );
            }

            if(( block = blockListTail ))
            {
                PBL_LIST_UNLINK( blockListHead, blockListTail,
                                 block, next, prev );
                PBL_LIST_PUSH(   blockListHead, blockListTail,
                                 block, next, prev );
            }
        }
        else
        {
            pbl_errno = PBL_ERROR_PROGRAM;
            return( 0 );
        }

        /*
         * make sure the expanded keys of the block are freed
         */
        pblBlockKeysRelease( block );
    }

    block->parentblock  = -1;
    block->parentindex  = -1;
    block->dirty        = 0;
    block->bf           = -1;
    block->filesettag   = NULL;

    return( block );
}

static PBLBLOCK_t * pblBlockFind( PBLKFILE_t * file, long blockno )
{
    PBLBLOCK_t * block;

    /*
     * check if the block is in the LRU list of writeable blocks
     */
    for( block = file->writeableListHead; block; block = block->next )
    {
        if(( block->blockno == blockno )
         &&( block->bf      == file->bf ))
        {
            /*
             * the block is already there, make it the first of the LRU chain
             */
            if( block != file->writeableListHead )
            {
                PBL_LIST_UNLINK( file->writeableListHead,
                                 file->writeableListTail, block, next, prev );
                PBL_LIST_PUSH( file->writeableListHead,
                               file->writeableListTail, block, next, prev );
            }

            return( block );
        }
    }

    /*
     * check if the block is the head of the LRU list of blocks cached
     */
    if( blockListHead
     && blockListHead->blockno == blockno
     && blockListHead->bf      == file->bf )
    {
        return( blockListHead );
    }

    /*
     * lookup the block in the LRU list of blocks cached
     */
    block = pblBlockHashFind( blockno, file->bf );
    if( block )
    {
        /*
         * the block is there, make it the first of the LRU chain
         */
        if( block != blockListHead )
        {
            PBL_LIST_UNLINK( blockListHead, blockListTail, block, next, prev );
            PBL_LIST_PUSH( blockListHead, blockListTail, block, next, prev );
        }

        return( block );
    }

    return( 0 );
}

static PBLBLOCK_t * pblBlockGet( PBLKFILE_t * file, long blockno )
{
    PBLBLOCK_t * block;
    long       rc;

    /*
     * check if block is in memory
     */
    block = pblBlockFind( file, blockno );
    if( block )
    {
        return( block );
    }

    /*
     * block is not in memory, we have to load it; get an empty block
     */
    block = pblBlockGetVictim( file );
    if( !block )
    {
        return( block );
    }

    /*
     * read the data from file
     */
    rc = pbf_blockread( file->bf, blockno, block->data );
    if( rc < 0 )
    {
        return( ( PBLBLOCK_t * ) 0 );
    }

    /*
     * block has been read successfully, so it belongs to this file from now on
     */
    pblBlockToData( block );

    block->blockno    = blockno;
    block->bf         = file->bf;
    block->filesettag = file->filesettag;

    /*
     * insert the reference into the hash table
     */
    if( pblBlockHashInsert( block->blockno, block->bf, block ) )
    {
        pbl_errno = PBL_ERROR_PROGRAM;
        return( 0 );
    }
    return( block );
}

/*
 * get a version of block we can write to to the writeable list of the file
 */
static PBLBLOCK_t * pblBlockGetWriteable( PBLKFILE_t * file, long blockno )
{
    PBLBLOCK_t * newblock;
    PBLBLOCK_t * block;

    /*
     * get the block to memory
     */
    block = pblBlockGet( file, blockno );
    if( !block || block->writeable )
    {
        return( block );
    }

    if( !block->dirty )
    {
        /*
         * move the block over to the writeable list
         */
        pblnblocks--;
        PBL_LIST_UNLINK( blockListHead, blockListTail, block, next, prev );
        pblBlockHashRemove( block->blockno, block->bf );

        block->writeable = 1;
        PBL_LIST_PUSH( file->writeableListHead,
                       file->writeableListTail, block, next, prev );

        return( block );
    }

    /*
     * create a copy of the block in the writeable block list
     */
    newblock = pbl_memdup( "pblBlockGetWriteable block",
                           block, sizeof( *block ) );
    if( !newblock )
    {
        return( newblock );
    }

    newblock->writeable = 1;
    PBL_LIST_PUSH( file->writeableListHead, file->writeableListTail,
                   newblock, next, prev );

    /*
     * make sure the expanded keys are only stored once
     */
    if( block->expandedkeys )
    {
        PBLBLOCKLINK_t  * blink;

        blink = ( PBLBLOCKLINK_t * ) block;
        PBL_LIST_UNLINK( linkListHead, linkListTail, blink, next, prev );

        blink = ( PBLBLOCKLINK_t * ) newblock;
        PBL_LIST_PUSH( linkListHead, linkListTail, blink, next, prev );

        block->expandedkeys = 0;
    }

    return( newblock );
}

static int pblBlockFree( PBLKFILE_t * file, long blockno )
{
    PBLBLOCK_t * rootblock;
    PBLBLOCK_t * block;
    PBLBLOCK_t * nblock = 0;
    PBLBLOCK_t * pblock = 0;

    /*
     * get the root block to memory
     */
    rootblock = pblBlockGetWriteable( file, 0 );
    if( !rootblock )
    {
        return( -1 );
    }

    /*
     * read the block to be freed
     */
    block = pblBlockGetWriteable( file, blockno );
    if( !block )
    {
        return( -1 );
    }

    /*
     * read the previous and next block if they exists
     */
    if( block->nblock )
    {
        nblock = pblBlockGetWriteable( file, block->nblock );
        if( !nblock )
        {
            return( -1 );
        }
    }

    if( block->pblock )
    {
        pblock = pblBlockGetWriteable( file, block->pblock );
        if( !pblock )
        {
            return( -1 );
        }
    }

    if( nblock )
    {
        nblock->pblock = block->pblock;
        nblock->dirty = 1;
    }

    if( pblock )
    {
        pblock->nblock = block->nblock;
        pblock->dirty = 1;
    }

    pblBlockKeysRelease( block );

    /*
     * set the values of the free block
     */
    block->level  = 254;
    block->nblock = rootblock->pblock;

    /*
     * blocks freed always have their predecessor set to 0
     */
    block->pblock   = 0;
    block->nentries = 0;
    block->free     = PBLHEADERSIZE;           /* offset of first free byte   */

    memset( block->data, 0, PBLDATASIZE );

    block->dirty = 1;

    /*
     * set the link from the rootblock to the block
     */
    rootblock->pblock = blockno;
    rootblock->dirty  = 1;

    return( 0 );
}

static int pblBlockConcat(
PBLKFILE_t      * file,
PBLBLOCK_t      * block,
PBLBLOCK_t      * from,
unsigned char   * key,
unsigned int      keylen
)
{
    PBLBLOCK_t      tmpblock;
    unsigned char   predkey[ PBLKEYLENGTH ];
    unsigned int    predkeylen = 0;
    PBLITEM_t       item;
    int             rc;
    unsigned int    i;
    int             nentries;

    if( !block->writeable || !from->writeable )
    {
        pbl_errno = PBL_ERROR_PROGRAM;
        return( -1 );
    }

    /*
     * read the last item of left block
     */
    nentries = block->nentries;
    if( block->nentries > 0 )
    {
        if( pblBlockKeysExpand( block ))
        {
            return( -1 );
        }

        pblItemGet( block, block->nentries - 1, &item );

        predkeylen = item.keylen;
        if( predkeylen )
        {
            pbl_memlcpy( predkey, sizeof( predkey ), item.key, predkeylen );
        }
    }

    /*
     * we do not need the expanded keys of the block anymore
     */
    pblBlockKeysRelease( block );

    /*
     * create a local copy to concatenate to
     */
    tmpblock = *block;

    /*
     * expand the keys of the right block
     */
    if( pblBlockKeysExpand( from ))
    {
        return( -1 );
    }

    /*
     * copy all items to be merged to the temporary block
     */
    for( i = 0; i < from->nentries; i++ )
    {
        pblItemGet( from, i, &item );

        /*
         * the first item can have an empty key, if so we use
         * key given as parameter
         */
        if( i == 0 && keylen > 0 && item.keylen < 1 )
        {
            item.key = key;
            item.keylen = keylen;
        }
        rc = pblItemAppend( &tmpblock, predkey, predkeylen, &item );
        if( rc )
        {
            if( pbl_errno == PBL_ERROR_NOFIT )
            {
                pbl_errno = 0;
                return( 0 );
            }

            return( rc );
        }

        predkeylen = item.keylen;
        if( predkeylen > 0 )
        {
            pbl_memlcpy( predkey, sizeof( predkey ), item.key, predkeylen );
        }
    }

    /*
     * copy the values back to our original block
     */
    block->nentries = tmpblock.nentries;
    block->free     = tmpblock.free;
    memcpy( block->data, tmpblock.data, PBLDATASIZE );

    block->dirty = 1;

    /*
     * change values to the current record if they point to the right block
     */
    if( file->blockno == from->blockno )
    {
        /*
         * set the current record values to the left block
         */
        file->blockno = block->blockno;
        file->index  += nentries;
    }

    return( 1 );
}

static long pblBlockAppend(
PBLKFILE_t * file,
int          level,
long         nblock,
long         pblock
)
{
    PBLBLOCK_t      * rootblock = 0;
    PBLBLOCK_t      * block = 0;
    long              freeblockno = 0;

    /*
     * if nblock is 1, we are called to create the first block of a file
     * no need to try to read any block of the file
     */
    if( nblock != 1 && pblock != 1 )
    {
        /*
         * get the root block to memory
         */
        rootblock = pblBlockGetWriteable( file, 0 );
        if( !rootblock )
        {
            pbl_errno = PBL_ERROR_BAD_FILE;
            return( -1 );
        }

        /*
         * read block number of first free block of file
         */
        freeblockno = rootblock->pblock;
        if( freeblockno )
        {
            /*
             * read the free block
             */
            block = pblBlockGetWriteable( file, freeblockno );
            if( block )
            {
                if( block->level != 254 )
                {
                    pbl_errno = PBL_ERROR_BAD_FILE;
                    return( -1 );
                }

                /*
                 * set the next free block to the rootblock
                 */
                rootblock->pblock = block->nblock;
                if( !rootblock->pblock && block->pblock )
                {
                    /*
                     * the block read is a new block,
                     * set the next block of the root block to a big value
                     */
                    rootblock->pblock = block->pblock;
                }

                rootblock->dirty = 1;
            }
            else
            {
                /*
                 * no error, append a new block
                 */
                pbl_errno = 0;
                freeblockno += 1;
            }
        }
    }

    if( !block )
    {
        PBLBLOCK_t newblock;

        /*
         * init the new block
         */
        memset( &newblock, 0, sizeof( newblock ));

        /*
         * add a free block
         */
        newblock.level = ( char ) 254 & 0xff;

        /*
         * new blocks appended have their next block set to 0
         * and their previous block set to the highest free block
         *
         * blocks freed always set the pblock to 0
         */
        newblock.nblock = 0;
        newblock.pblock = freeblockno;;
        newblock.free   = PBLHEADERSIZE;

        /*
         * prepare the new block for writing
         */
        pblDataToBlock( &newblock );

        /*
         * append a new block to the file
         */
        freeblockno = pbf_blockappend( file->bf, newblock.data );
        if( freeblockno < 0 )
        {
            pbl_errno = PBL_ERROR_BAD_FILE;
            return( -1 );
        }

        /*
         * read the free block
         */
        block = pblBlockGetWriteable( file, freeblockno );
        if( !block || block->level != 254 )
        {
            pbl_errno = PBL_ERROR_BAD_FILE;
            return( -1 );
        }

        if( rootblock )
        {
            /*
             * set the next free block to the rootblock
             */
            rootblock->pblock = block->pblock;
            if( !rootblock->pblock )
            {
                rootblock->pblock = 1;
            }
            rootblock->dirty = 1;
        }
    }

    /*
     * init the new block
     */
    memset( block->data, 0, PBLDATASIZE );

    block->level      = ( char ) level & 0xff;
    block->nentries   = 0;
    block->nblock     = nblock;
    block->pblock     = pblock;
    block->free       = PBLHEADERSIZE;
    block->dirty      = 1;

    return( block->blockno );
}

static int pblBlockMerge(
PBLKFILE_t * file,
long         parentblockno,
int          parentindex,
long         blockno
)
{
    int             merged = 0;
    PBLBLOCK_t    * parentblock;
    PBLBLOCK_t    * block;
    PBLBLOCK_t    * peerblock;
    PBLITEM_t       item;
    int             rc;
    unsigned char   key[ PBLKEYLENGTH ];
    unsigned int    keylen;

    /*
     * read the parentblock
     */
    parentblock = pblBlockGet( file, parentblockno );
    if( !parentblock )
    {
        /*
         * no error because the parent block might have been split
         */
        pbl_errno = 0;
        return( merged );
    }

    /*
     * check the parentindex because the parentblock might have been
     * split without the child knowing about it
     */
    if( parentindex >= (int)parentblock->nentries )
    {
        return( merged );
    }

    /*
     * read the item pointing to blockno
     */
    if( pblItemGet( parentblock, parentindex, &item ))
    {
        return( -1 );
    }

    /*
     * check the pointer to the child, because the parentblock might have been
     * split without the child knowing about it
     */
    if( blockno != item.datablock )
    {
        return( merged );
    }

    /*
     * if there is a block to the left
     */
    while( parentindex > 0 )
    {
        /*
         * check the parentindex because the parentblock might have been
         * split without the child knowing about it
         */
        if( parentindex >= (int)parentblock->nentries )
        {
            return( merged );
        }

        /*
         * read the item pointing to blockno
         */
        if( pblItemGet( parentblock, parentindex, &item ))
        {
            return( -1 );
        }

        /*
         * set the pointer to the child
         */
        blockno = item.datablock;

        /*
         * read the child block
         */
        block = pblBlockGet( file, blockno );
        if( !block )
        {
            return( -1 );
        }

        /*
         * read the item pointing to the peer
         */
        if( pblItemGet( parentblock, parentindex - 1, &item ))
        {
            return( -1 );
        }

        /*
         * read the peerblock
         */
        peerblock = pblBlockGet( file, item.datablock );
        if( !peerblock )
        {
            return( -1 );
        }

        /*
         * see how much empty space we have on the two blocks
         */
        rc = PBLINDEXBLOCKNFREE( block ) + PBLINDEXBLOCKNFREE( peerblock );
        if( rc < ( PBLDATASIZE + 6 + PBLKEYLENGTH ))
        {
            /*
             * we do not merge
             */
            break;
        }

        /*
         * read the child block
         */
        block = pblBlockGetWriteable( file, blockno );
        if( !block )
        {
            return( -1 );
        }

        /*
         * read the peerblock
         */
        peerblock = pblBlockGetWriteable( file, item.datablock );
        if( !peerblock )
        {
            return( -1 );
        }

        /*
         * read the first key of the right block to merge
         */
        parentblock = pblBlockGetWriteable( file, parentblockno );
        if( !parentblock )
        {
            return( -1 );
        }

        /*
         * check the parentindex
         */
        if( parentindex >= (int)parentblock->nentries )
        {
            return( merged );
        }

        if( pblBlockKeysExpand( parentblock ))
        {
            return( -1 );
        }

        /*
         * read the item pointing to blockno
         */
        if( pblItemGet( parentblock, parentindex, &item ))
        {
            return( -1 );
        }

        if( item.keylen < 1 )
        {
            pbl_errno = PBL_ERROR_BAD_FILE;
            return( -1 );
        }

        keylen = item.keylen;
        pbl_memlcpy( key, sizeof( key ), item.key, keylen );

        /*
         * concatenate the two blocks
         */
        rc = pblBlockConcat( file, peerblock, block, key, keylen );
        if( rc < 0 )
        {
            return( rc );
        }
        else if( rc == 0 )
        {
            /*
             * we could not merge, break the loop
             */
            break;
        }

        /*
         * the two blocks were merged
         */
        merged += 1;

        /*
         * free the block
         */
        rc = pblBlockFree( file, blockno );
        if( rc )
        {
            return( rc );
        }

        rc = pblItemRemove( parentblock, parentindex );
        if( rc )
        {
            return( rc );
        }
    }

    /*
     * if there is a block to the left
     */
    while( parentindex < (int)parentblock->nentries - 1 )
    {
        /*
         * read the item pointing to blockno
         */
        if( pblItemGet( parentblock, parentindex, &item ))
        {
            return( -1 );
        }

        /*
         * set the pointer to the child
         */
        blockno = item.datablock;

        /*
         * read the child block
         */
        block = pblBlockGet( file, blockno );
        if( !block )
        {
            return( -1 );
        }

        /*
         * read the item pointing to the peer
         */
        if( pblItemGet( parentblock, parentindex + 1, &item ))
        {
            return( -1 );
        }

        /*
         * read the peerblock
         */
        peerblock = pblBlockGet( file, item.datablock );
        if( !peerblock )
        {
            return( -1 );
        }

        /*
         * see how much empty space we have on the two blocks
         */
        rc = PBLINDEXBLOCKNFREE( block ) + PBLINDEXBLOCKNFREE( peerblock );
        if( rc < ( PBLDATASIZE + 6 + PBLKEYLENGTH ))
        {
            /*
             * we do not merge
             */
            break;
        }

        /*
         * read the child block
         */
        block = pblBlockGetWriteable( file, blockno );
        if( !block )
        {
            return( -1 );
        }

        /*
         * read the peerblock
         */
        blockno = item.datablock;
        peerblock = pblBlockGetWriteable( file, blockno );
        if( !peerblock )
        {
            return( -1 );
        }

        /*
         * read the first key of the right block to merge
         */
        parentblock = pblBlockGetWriteable( file, parentblockno );
        if( !parentblock )
        {
            return( -1 );
        }

        /*
         * check the parentindex
         */
        if( parentindex >= (int)parentblock->nentries - 1 )
        {
            return( merged );
        }

        if( pblBlockKeysExpand( parentblock ))
        {
            return( -1 );
        }

        /*
         * read the item pointing to blockno
         */
        if( pblItemGet( parentblock, parentindex + 1, &item ))
        {
            return( -1 );
        }

        if( item.keylen < 1 )
        {
            pbl_errno = PBL_ERROR_BAD_FILE;
            return( -1 );
        }

        keylen = item.keylen;
        pbl_memlcpy( key, sizeof( key ), item.key, keylen );

        /*
         * concatenate the two blocks
         */
        rc = pblBlockConcat( file, block, peerblock, key, keylen );
        if( rc < 0 )
        {
            return( rc );
        }
        else if( rc == 0 )
        {
            /*
             * we could not merge, break the loop
             */
            break;
        }

        /*
         * the two blocks were merged
         */
        merged += 1;

        /*
         * free the block
         */
        rc = pblBlockFree( file, blockno );
        if( rc )
        {
            return( rc );
        }

        rc = pblItemRemove( parentblock, parentindex + 1 );
        if( rc )
        {
            return( rc );
        }
    }

    return( merged );
}

/*
 * truncate the blocklist to the number of blocks allowed
 */
static int pblBlockListTruncate( void )
{
    PBLBLOCK_t * block;
    int          rc;

    /*
     * truncate the list of blocks we have in memory
     */
    while( pblnblocks >= 8 + ( pblblocksperfile * pblnfiles ) )
    {
        block = blockListTail;
        if( !block )
        {
            pblnblocks = 0;
            break;
        }

        if( block->bf != -1 )
        {
            if( block->dirty )
            {
                /*
                 * if one block of a file is dirty, all blocks are flushed
                 */
                rc = pblBlockFlush( block->bf, 0 );
                if( rc )
                {
                    return( rc );
                }
            }

            pblBlockHashRemove( block->blockno, block->bf );
        }

        PBL_LIST_UNLINK( blockListHead, blockListTail, block, next, prev );
        pblBlockKeysRelease( block );
        PBL_FREE( block );

        pblnblocks--;
    }

    return( 0 );
}

/**
 * change the number of cache blocks used per open key file
 *
 * the default number is 64, a memory block uses about 4096 bytes of heap memory
 *
 * @return int rc: the number of blocks used after the call
 *
 *
 */

int pblKfInit(
int nblocks            /* number of blocks used per open file */
)
{
    pbl_errno = 0;

    if( nblocks < 1 )
    {
        return( pblnblocks );
    }

    if( nblocks < 8 )
    {
        nblocks = 8;
    }

    pblblocksperfile = nblocks;
    pblexpandedperfile = pblblocksperfile / 2;

    return( pblblocksperfile );
}

/*
 * FILE functions
 */
static long pblDataAppend(
PBLKFILE_t * file,
char       * data,
long         datalen,
long       * offset
)
{
    long blockno;
    long returnoffset;
    long returnblock;
    long diff;
    long bytesWritten = 0;
    int  nbytes;
    PBLBLOCK_t * rootblock = 0;
    PBLBLOCK_t * datablock = 0;

    rootblock = pblBlockGet( file, 0 );
    if( !rootblock )
    {
        return( -1 );
    }

    /*
     * rootblock->nblock always contains the number of the last datablock
     */
    datablock = pblBlockGetWriteable( file, rootblock->nblock );
    if( !datablock )
    {
        return( -1 );
    }

    if( datablock->level != 255 )
    {
        pbl_errno = PBL_ERROR_BAD_FILE;
        return( -1 );
    }

    returnoffset = datablock->free;
    returnblock  = datablock->blockno;

    while( bytesWritten < datalen )
    {
        diff = datalen - bytesWritten;
        if( diff > PBLDATABLOCKNFREE( datablock ))
        {
            nbytes = PBLDATABLOCKNFREE( datablock );
        }
        else
        {
            nbytes = (( int )( diff % PBLDATASIZE));
        }

        if( nbytes > 0 )
        {
            memcpy((void *) &(datablock->data[ datablock->free ]),
                   (void *) data,
                   nbytes );
            datablock->dirty = 1;
        }

        bytesWritten        += nbytes;
        data                += nbytes;
        datablock->free += nbytes;

        if( PBLDATABLOCKNFREE( datablock ) < 1 )
        {
            /*
             * make a new data block
             */
            blockno = pblBlockAppend( file, -1, 0, datablock->blockno );
            if( blockno < 0 )
            {
                return( -1 );
            }

            datablock->nblock = blockno;
            datablock->dirty  = 1;

            /*
             * get the new datablock to memory
             */
            datablock = pblBlockGetWriteable( file, blockno );
            if( !datablock )
            {
                return( -1 );
            }

            /*
             * set address to rootblock
             */
            rootblock = pblBlockGetWriteable( file, 0 );
            if( !rootblock )
            {
                return( -1 );
            }

            rootblock->nblock = blockno;
            rootblock->dirty  = 1;
        }
    }

    *offset = returnoffset;

    return( returnblock );
}

/**
 * create a key file with the name specified by path.
 *
 * a file set tag can be attached to the file,
 * if a file having a non NULL file set tag is flushed
 * to disk all files having the same file set tag attached
 * are flushed as well.
 *
 * <P>
 * <B>RESTRICTIONS</B>:
 * <BR> - the file to create must not exists.
 * <BR> - the current record of the file will not be set
 *
 * @return  pblKeyFile_t * retptr == NULL: an error occured, see pbl_errno
 * @return  pblKeyFile_t * retptr != NULL: a pointer to a key file descriptor
 */

pblKeyFile_t * pblKfCreate(
char * path,       /** path of file to create                                 */
void * filesettag  /** file set tag, for flushing multiple files consistently */
)
{
    pblKeyFile_t * k         = NULL;
    PBLKFILE_t   * kf        = NULL;
    PBLBLOCK_t   * rootblock = NULL;
    PBLITEM_t      item;
    int            fh;
    int            bf;
    long           blockno;
    int            rc;

    pbl_errno = 0;

    /*
     * make sure we have one filehandle for the create, close one file
     */
    if( pbf_ft_tail )
    {
        pbf_fh_close( pbf_ft_tail->bf, pbf_ft_tail->n );
    }

    /*
     * do a exclusive create open, make sure the file does not exist yet
     */
    fh = open( path, O_CREAT | O_EXCL | O_BINARY | O_RDWR, S_IREAD | S_IWRITE );
    if( -1 == fh )
    {
        pbl_errno = PBL_ERROR_CREATE;
        return( 0 );
    }
    close( fh );

    /*
     * open the file
     */
    bf = pbf_open( path, 1, PBLFILEBLOCKS, PBLDATASIZE );
    if( bf < 0 )
    {
        return( 0 );
    }

    /*
     * get and init file structure
     */
    kf = pbl_malloc0( "pblKfCreate FILE", sizeof( PBLKFILE_t ));
    if( !kf )
    {
        goto errout;
    }

    /*
     * we have a key file set the return value
     */
    k              = ( pblKeyFile_t * )kf;
    kf->magic      = rcsid;
    kf->bf         = bf;
    kf->update     = 1;
    kf->filesettag = filesettag;

    /*
     * start a transaction on the file
     */
    pblKfStartTransaction( k );

    /*
     * make the root block, next offset is first data block
     */
    blockno = pblBlockAppend( kf, 0, 1, 1 );
    if( blockno < 0 )
    {
        goto errout;
    }

    rootblock = pblBlockGet( kf, 0 );
    if( !rootblock )
    {
        goto errout;
    }

    /*
     * make the first data block
     */
    blockno = pblBlockAppend( kf, -1, 0, 0 );
    if( blockno != 1 )
    {
        goto errout;
    }

    /*
     * init the first item we insert into each file
     */
    item.level     = 0;
    item.key       = ( char * ) 0;
    item.keylen    = 0;
    item.keycommon = 0;
    item.datalen   = strlen( magic ) + 1;
    item.data      = magic;

    /*
     * append the magic string as first data item
     */
    if( item.datalen > PBLDATALENGTH )
    {
        item.datablock = pblDataAppend( kf, item.data,
                                        item.datalen, &item.dataoffset );
        if( item.datablock < 1 )
        {
            goto errout;
        }
        item.data = 0;
    }

    /*
     * insert the first item into the root block
     */
    rootblock = pblBlockGetWriteable( kf, 0 );
    if( !rootblock )
    {
        goto errout;
    }

    rc = pblItemAdd( kf, rootblock, &item );
    if( rc )
    {
        goto errout;
    }

    /*
     * no current record yet
     */
    kf->blockno = -1;
    kf->index   = -1;

    /*
     * commit the changes
     */
    if( pblKfCommit( k, 0 ))
    {
        goto errout;
    }

    if( pblBlockFlush( kf->bf, 0 ))
    {
        goto errout;
    }

    pblnfiles++;

    return( k );

errout:

    if( kf )
    {
        if( kf->bf >= 0 )
        {
            pblKfCommit( k, 1 );
        }
        PBL_FREE( kf );
    }

    if( -1 != bf )
    {
        pblBlocksRelease( bf );
        close( fh );
        unlink( path );
    }

    return( 0 );
}

/**
 * open an existing key file
 * 
 * if update is 0, the file is opened for read access only,
 * if update is not 0 the file is opened for reading and writing
 *
 * a file set tag can be attached to the file,
 * if a file having a non NULL file set tag is flushed
 * to disk all files having the same file set tag attached
 * are flushed as well.
 *
 * <P>
 * <B>RESTRICTIONS</B>:
 * <BR> - the file must exist already
 * <BR> - the current record of the file will not be set
 *
 * @return  pblKeyFile_t * retptr == NULL: an error occured, see pbl_errno
 * @return  pblKeyFile_t * retptr != NULL: a pointer to a key file descriptor
 */

pblKeyFile_t * pblKfOpen(
char * path,       /** path of file to create                                 */
int    update,     /** flag: should file be opened for update?                */
void * filesettag  /** file set tag, for flushing multiple files consistently */
)
{
    PBLKFILE_t  * kf;
    PBLBLOCK_t  * datablock;
    int           bf;

    pbl_errno = 0;

    bf = pbf_open( path, update, PBLFILEBLOCKS, PBLDATASIZE );
    if( -1 == bf )
    {
        return( 0 );
    }

    /*
     * get and init file structure
     */
    kf = pbl_malloc0( "pblKfOpen FILE", sizeof( PBLKFILE_t ));
    if( !kf )
    {
        pbf_close( bf );
        return( 0 );
    }
    kf->magic      = rcsid;
    kf->bf         = bf;
    kf->update     = update;
    kf->filesettag = filesettag;
    kf->blockno    = -1;
    kf->index      = -1;

    /*
     * read and check the first datablock
     */
    datablock = pblBlockGet( kf, 1 );
    if( !datablock || ( datablock->level != 255 ))
    {
        pblBlocksRelease( bf );
        pbf_close( bf );
        PBL_FREE( kf );
        pbl_errno = PBL_ERROR_BAD_FILE;
        return( 0 );
    }

    pblnfiles++;

    return( ( pblKeyFile_t * ) kf );
}

/**
 * close a key file
 *
 * all changes are flushed to disk before, 
 * all memory allocated for the file is released. 
 *
 * @return int rc == 0: call went ok, file is closed
 * @return int rc != 0: some error, see pbl_errno
 */

int pblKfClose(
pblKeyFile_t * k   /** key file to close */
)
{
    PBLKFILE_t * kf = ( PBLKFILE_t * ) k;
    int rc = 0;

    pbl_errno = 0;

    if( kf->update )
    {
        rc = pblBlockFlush( kf->bf, 1 );
    }
    else
    {
        pblBlocksRelease( kf->bf );
    }

    pbf_close( kf->bf );
    PBL_FREE( kf );

    pblnfiles--;

    if( pblBlockListTruncate())
    {
        rc = -1;
    }

    return( rc );
}

/**
 * set an application specific compare function for the keys of a key file
 *
 * an application specific compare function can be used in order to
 * implement special orderings of the values of an index, e.g.
 * because of the use of european "umlauts" in names
 *
 * the default compare function is the c-library memcmp function
 * the keycompare function should behave like memcmp
 *
 * @return void
 */

void pblKfSetCompareFunction(
pblKeyFile_t * k,             /** key file to set compare function for  */
int ( *keycompare )           /** compare function to set               */
    (
                void* left,   /** "left" buffer for compare             */
                size_t llen,  /** length of that buffer                 */
                void* right,  /** "right" buffer for compare            */
                size_t rlen   /** length of that buffer                 */
    )
)
{
    PBLKFILE_t * kf = ( PBLKFILE_t * ) k;

    kf->keycompare = keycompare;
}

/**
 * flush a key file
 *
 * all changes are flushed to disk,
 *
 * @return int rc == 0: call went ok
 * @return int rc != 0: some error, see pbl_errno
 */

int pblKfFlush(
pblKeyFile_t * k   /** key file to flush */
)
{
    PBLKFILE_t * kf = ( PBLKFILE_t * ) k;
    int rc;

    pbl_errno = 0;

    rc = pblBlockFlush( kf->bf, 0 );
    if( rc )
    {
        return( rc );
    }

    rc = pblBlockListTruncate();

    return( rc );
}

/**
 * commit or rollback changes done during a transaction.
 *
 * transactions can be nested, if so the commit
 * only happens when the outermost transaction
 * calls a commit.
 *
 * the commit only happens to process space buffer cache,
 * call \Ref{pblKfFlush}() after \Ref{pblKfCommit}() if you want to
 * flush to kernel space buffer cache.
 *
 * @return int rc == 0: the commit went ok
 * @return int rc >  0: a rollback happened, either because the caller
 *                      requested it or because an inner transaction resulted
 *                      in a rollback
 * @return int rc <  0: some error, see pbl_errno
 */

int pblKfCommit(
pblKeyFile_t * k, /** key file to commit                                      */
int rollback      /** != 0: roll back the changes, == 0: commit the changes   */
)
{
    PBLKFILE_t * kf = ( PBLKFILE_t * ) k;
    PBLBLOCK_t * block;
    PBLBLOCK_t * b;

    pbl_errno = 0;

    /*
     * if there is no transaction active for the file
     */
    if( !rollback && ( kf->transactions < 1 ))
    {
        pbl_errno = PBL_ERROR_PROGRAM;
        return( -1 );
    }
    kf->transactions--;

    /*
     * if a rollback is requested for this commit
     */
    if( rollback )
    {
        /*
         * remember that at least one rollback is requested for the file
         */
        kf->rollback = 1;
    }

    /*
     * if there is an outer transaction active for the file
     */
    if( kf->transactions > 0 )
    {
        return( kf->rollback );
    }

    /*
     * there is no more transaction active, rollback or commit
     */
    if( kf->rollback )
    {
        /*
         * release all blocks that were changed without putting
         * them back into the blocklist buffer cache
         */
        while(( block = kf->writeableListTail ))
        {
            PBL_LIST_UNLINK( kf->writeableListHead, kf->writeableListTail,
                             block, next, prev );
            pblBlockKeysRelease( block );
            PBL_FREE( block );
            continue;
        }

        /*
         * reset the transaction and the rollback value
         */
        kf->transactions = 0;
        kf->rollback = 0;
        return( 1 );
    }

    /*
     * commit the changed blocks
     */
    while(( block = kf->writeableListTail ))
    {
        /*
         * commit all blocks that were changed by rechaining
         * them into the blocklist buffer cache
         */
        PBL_LIST_UNLINK( kf->writeableListHead, kf->writeableListTail,
                         block, next, prev );

        /*
         * find a potential copy of the block in the LRU list
         */
        b = pblBlockFind( kf, block->blockno );
        if( b )
        {
            /*
             * delete the copy from the LRU list
             */
            PBL_LIST_UNLINK( blockListHead, blockListTail, b, next, prev );
            pblBlockKeysRelease( b );
            PBL_FREE( b );
        }
        else
        {
            /*
             * we add a block to the LRU list
             */
            pblnblocks++;
        }

        /*
         * blocks in the buffer cache are not writeable
         */
        block->writeable = 0;
        PBL_LIST_PUSH( blockListHead, blockListTail, block, next, prev );

        /*
         * insert or update the reference in the hash table
         */
        if( pblBlockHashInsert( block->blockno, block->bf, block ) < 0 )
        {
            pbl_errno = PBL_ERROR_PROGRAM;
            return( 0 );
        }
    }

    /*
     * reset the transaction and the rollback value
     */
    kf->transactions = 0;
    kf->rollback = 0;

    return( 0 );
}

/**
 * start a transaction on a key file
 *
 * transactions can be nested
 *
 * @return int rc == 0: the transaction was started successfully
 * @return int rc >  0: the transaction was started
 *                      but another transaction has resulted in
 *                      a rollback request on the file already
 */

int pblKfStartTransaction(
pblKeyFile_t * k           /** key file to start transaction on               */
)
{
    PBLKFILE_t * kf = ( PBLKFILE_t * ) k;
    pbl_errno = 0;

    /*
     * if there is no transaction active for the file
     */
    if( kf->transactions < 1 )
    {
        kf->transactions = 1;
        kf->rollback = 0;
        return( 0 );
    }
    kf->transactions++;

    return( kf->rollback );
}

/**
 * save the position of the current record for later restore
 *
 * @return int rc == 0: the position was saved
 * @return int rc  < 0: an error, see pbl_errno
 */
int pblKfSavePosition(
pblKeyFile_t * k       /** key file to save position for                     */
)
{
    PBLKFILE_t * kf = ( PBLKFILE_t * ) k;

    pbl_errno = 0;

    /*
     * save the block number and the index
     */
    kf->saveblockno = kf->blockno;
    kf->saveindex   = kf->index;

    return( 0 );
}

/**
 * restore the position of the current record saved by the
 * last previous call to \Ref{pblKfSavePosition}().
 *
 * @return int rc == 0: the position was restored
 * @return int rc  < 0: an error, see pbl_errno
 */

int pblKfRestorePosition(
pblKeyFile_t * k       /** key file to restore position for                   */
)
{
    PBLKFILE_t * kf = ( PBLKFILE_t * ) k;

    /*
     * restore the block number and the index
     */
    kf->blockno = kf->saveblockno;
    kf->index   = kf->saveindex;

    return( 0 );
}

/*
 * INSERT functions
 */
static int pblItemSave( PBLITEM_t * item )
{
    PBLITEM_t * newitem  = ( PBLITEM_t * ) 0;

    newitem = pbl_malloc0( "pblItemSave ITEM", sizeof( PBLITEM_t ));
    if( !newitem )
    {
        return( -1 );
    }

    /*
     * save the values of the item
     */
    *newitem = *item;

    /*
     * save the key
     */
    if( newitem->keylen > 0 )
    {
        newitem->key = pbl_memdup( "pblItemSave item->key",
                                   item->key, newitem->keylen );
        if( !newitem->key )
        {
            PBL_FREE( newitem );
            return( -1 );
        }
    }
    else
    {
        newitem->key = 0;
    }

    /*
     * save the data
     */
    if( newitem->datalen < 1 || newitem->datalen > PBLDATALENGTH )
    {
        newitem->data = 0;
    }

    /*
     * push the new item to top of list
     */
    PBL_LIST_PUSH( itemListHead, itemListTail, newitem, next, prev );

    return( 0 );
}

static void pblItemRelease( void )
{
    PBLITEM_t * item;

    if(( item = itemListHead ))
    {
        PBL_LIST_UNLINK( itemListHead, itemListTail, item, next, prev );

        if( item->key )
        {
            PBL_FREE( item->key );
        }
        PBL_FREE( item );
    }
}

static void pblItemReleaseAll( void )
{
    while( itemListHead )
    {
        pblItemRelease( );
    }
}

#if 1

static int pblSplit( PBLKFILE_t *file, PBLBLOCK_t * block )
{
    unsigned int   index;
    PBLITEM_t      splititem;
    PBLITEM_t      item;
    PBLBLOCK_t     tmpblock;
    PBLBLOCK_t    *newblock;
    PBLBLOCK_t    *target;
    unsigned char *predkey = "";
    unsigned int   predkeylen = 0;
    long           newblockno;
    int            rc;

    /*
     * create a new block
     */
    newblockno = pblBlockAppend( file, block->level,
                                 block->nblock, block->blockno );
    if( newblockno < 0 )
    {
        return( -1 );
    }

    /*
     * set backward pointer in successor block of block
     */
    if( block->nblock )
    {
        newblock = pblBlockGetWriteable( file, block->nblock );
        if( !newblock )
        {
            return( -1 );
        }

        newblock->pblock = newblockno;
        newblock->dirty  = 1;
    }

    /*
     * get the new block to memory
     */
    newblock = pblBlockGetWriteable( file, newblockno );
    if( !newblock )
    {
        return( -1 );
    }

    /*
     * copy the block to split onto the stack
     */
    pblBlockKeysRelease( block );
    tmpblock = *block;

    /*
     * prepare the block for split
     */
    block->nblock   = newblockno;
    block->nentries = 0;
    block->free     = PBLHEADERSIZE;
    block->dirty    = 1;
    memset( block->data, 0, PBLDATASIZE );

    tmpblock.expandedkeys = 0;
    if( pblBlockKeysExpand( &tmpblock ))
    {
       return( -1 );
    }

    /*
     * copy the items from tmpblock to our two real blocks
     */
    for( target = block, index = 0; index < tmpblock.nentries; index++ )
    {
        rc = pblItemGet( &tmpblock, index, &item );
        if( rc )
        {
            pblBlockKeysRelease( &tmpblock );
            return( -1 );
        }

        /*
         * if first block we copy to is more than half full
         */
        if(  ( target == block )
         && (( PBLINDEXBLOCKNFREE( block ) < ( PBLDATASIZE / 2 ))
          || ( index == tmpblock.nentries - 1 )))
        {
            /*
             * the first item that goes to the new second block has to be
             * saved for later insert into the father block of the new block
             */
            splititem = item;
            splititem.datalen    = 0;
            splititem.datablock  = newblockno;
            splititem.dataoffset = 0;
            splititem.level++;

            rc = pblItemSave( &splititem );
            if( rc )
            {
                pblBlockKeysRelease( &tmpblock );
                return( -1 );
            }

            /*
             * for blocks of level greater than 0 the first item on each block
             * has keylength 0
             */
            if( tmpblock.level > 0 )
            {
                item.keylen = 0;
            }

            /*
             * from now on copy to the second block
             */
            target = newblock;
            predkeylen = 0;
        }

        rc = pblItemAppend( target, predkey, predkeylen, &item );
        if( rc < 0 )
        {
            pblBlockKeysRelease( &tmpblock );
            return( -1 );
        }

        predkeylen = item.keylen;
        if( predkeylen > 0 )
        {
            predkey = item.key;
        }
        else
        {
            predkey = "";
        }
    }

    pblBlockKeysRelease( &tmpblock );

    /*
     * set the parent pointers to the new block
     */
    newblock->parentblock = block->parentblock;
    newblock->parentindex = block->parentindex + 1;

    return( 0 );
}

#else

static int pblSplit( PBLKFILE_t * file, PBLBLOCK_t * block )
{
    unsigned int   index;
    int            splitindex = 0;
    PBLITEM_t      splititem;
    PBLITEM_t      item;
    PBLBLOCK_t    *newblock;
    PBLBLOCK_t    *target;
    unsigned char *predkey = "";
    unsigned int   predkeylen = 0;
    long           newblockno;
    int            rc;
    int            blockfree = PBLHEADERSIZE;

    /*
     * create a new block
     */
    newblockno = pblBlockAppend( file, block->level,
                                 block->nblock, block->blockno );
    if( newblockno < 0 )
    {
        return( -1 );
    }

    /*
     * set backward pointer in successor block of block
     */
    if( block->nblock )
    {
        newblock = pblBlockGetWriteable( file, block->nblock );
        if( !newblock )
        {
            return( -1 );
        }

        newblock->pblock = newblockno;
        newblock->dirty  = 1;
    }

    /*
     * get the new block to memory
     */
    newblock = pblBlockGetWriteable( file, newblockno );
    if( !newblock )
    {
        return( -1 );
    }

    /*
     * expand the keys of the block to split
     */
    if( pblBlockKeysExpand( block ))
    {
       return( -1 );
    }

    /*
     * prepare the block for split
     */
    block->nblock = newblockno;

    /*
     * copy the items from the block to our new block
     */
    for( target = block, index = 0; index < block->nentries; index++ )
    {
        rc = pblItemGet( block, index, &item );
        if( rc )
        {
            return( -1 );
        }

        if( target == block )
        {
            /*
             * calculate the size the items need on the block so far
             */
            blockfree += pblItemSize( block, &item );

            /*
             * if first block we copy to is more than half full
             */
            if( ( blockfree > ( PBLDATASIZE / 2 ))
             || ( index == block->nentries - 1 ))
            {
                /*
                 * the first item that goes to the new second block has to be
                 * saved for later insert into the father block of the new block
                 */
                splitindex = index;
                splititem  = item;
                splititem.datalen    = 0;
                splititem.datablock  = newblockno;
                splititem.dataoffset = 0;
                splititem.level++;

                rc = pblItemSave( &splititem );
                if( rc )
                {
                    return( -1 );
                }

                /*
                 * for blocks of level greater than 0 the first item
                 * on each block has keylength 0
                 */
                if( block->level > 0 )
                {
                    item.keylen = 0;
                }

                /*
                 * from now on copy to the second block
                 */
                target = newblock;
                predkeylen = 0;
            }
        }

        /*
         * only copy to the new block if the target is right
         */
        if( target == newblock )
        {
            rc = pblItemAppend( target, predkey, predkeylen, &item );
            if( rc < 0 )
            {
                return( -1 );
            }

            /*
             * remember the key of the predecessor item
             */
            predkeylen = item.keylen;
            if( predkeylen > 0 )
            {
                predkey = item.key;
            }
            else
            {
                predkey = "";
            }
        }
    }
    
    /*
     * delete the items that were copied from the block
     */
    for( index = block->nentries - 1; index >= splitindex; index-- )
    {
        rc = pblItemDelete( block, index );
        if( rc )
        {
            return( -1 );
        }
    }

    /*
     * set the parent pointers to the new block
     */
    newblock->parentblock = block->parentblock;
    newblock->parentindex = block->parentindex + 1;

    return( 0 );
}

#endif

static int pblSplitRoot( PBLKFILE_t *file )
{
    PBLBLOCK_t * rootblock;
    PBLITEM_t    item;
    PBLBLOCK_t * newblock;
    long         newblockno;
    int          rc;

    /*
     * get the root block to memory
     */
    rootblock = pblBlockGetWriteable( file, 0 );
    if( !rootblock )
    {
        return( -1 );
    }
    pblBlockKeysRelease( rootblock );

    /*
     * create a new block and get it to memory
     */
    newblockno = pblBlockAppend( file, rootblock->level, 0, 0 );
    if( newblockno < 0 )
    {
        return( -1 );
    }

    newblock = pblBlockGetWriteable( file, newblockno );
    if( !newblock )
    {
        return( -1 );
    }

    /*
     * copy some data of the root block to the new block
     */
    newblock->nentries = rootblock->nentries;
    newblock->free     = rootblock->free;
    memcpy( newblock->data, rootblock->data, PBLDATASIZE );

    newblock->dirty    = 1;

    /*
     * get the root block to memory
     */
    rootblock = pblBlockGetWriteable( file, 0 );
    if( !rootblock )
    {
        return( -1 );
    }

    /*
     * clear the root block
     */
    rootblock->level   += 1;
    rootblock->nentries = 0;
    rootblock->free     = PBLHEADERSIZE;
    rootblock->dirty    = 1;
    memset( rootblock->data, 0, PBLDATASIZE );

    newblock = pblBlockGetWriteable( file, newblockno );
    if( !newblock )
    {
        return( -1 );
    }

    /*
     * copy the first item from new block to the root block
     */
    rc = pblItemGet( newblock, 0, &item );
    if( rc )
    {
        return( -1 );
    }

    item.level      = rootblock->level;
    item.keylen     = 0;
    item.datalen    = 0;
    item.datablock  = newblockno;
    item.dataoffset = 0;

    rc = pblItemAppend( rootblock, 0, 0, &item );
    if( rc < 0 )
    {
        return( -1 );
    }

    /*
     * set the parent pointers to the new block
     */
    newblock->parentblock = 0;
    newblock->parentindex = 0;

    /*
     * split the new block
     */
    return( pblSplit( file, newblock ));
}

/**
 * insert a new record with the given key and data into a key file,
 *
 * multiple records with the same key are allowed,
 * if there are already records with the same key the new
 * record will be inserted behind all records with the same key,
 *
 * the current record of the file will be set to the new record
 *
 * <P>
 * <B>RESTRICTIONS</B>:
 * <BR> - the file must be open for update,
 * <BR> - key must point to the key to be inserted,
 * <BR> - keylen must be bigger than 0 and smaller than 256,
 * <BR> - data must point to the data be inserted,
 * <BR> - datalen must not be negative,
 * <BR> - if datalen == 0, the pointer data is not evaluated at all
 *
 * @return int rc == 0: call went ok
 * @return int rc != 0: some error occured, see pbl_errno
 */

int pblKfInsert(
pblKeyFile_t  * k,      /** key file to insert to                             */
unsigned char * key,    /** key to insert                                     */
int             keylen, /** length of the key                                 */
unsigned char * data,   /** data to insert                                    */
long            datalen /** length of the data                                */
)
{
    PBLKFILE_t *  kf = ( PBLKFILE_t * ) k;
    PBLITEM_t     item;
    PBLITEM_t   * insertitem;
    PBLBLOCK_t  * block;
    long          blockno;
    int           index;
    int           saveerrno;
    int           rc;

    long          parentblock = -1;
    int           parentindex = -1;
    int           split = 0;

    pbl_errno = 0;

    /*
     * start a transaction on the key file
     */
    pblKfStartTransaction( k );
    if( pblBlockListTruncate())
    {
        pblKfCommit( k, 1 );
        return( -1 );
    }

    if( !kf->update )
    {
        pblKfCommit( k, 1 );
        pbl_errno = PBL_ERROR_NOT_ALLOWED;
        return( -1 );
    }

    rc = pblParamsCheck( key, keylen, data, datalen );
    if( rc )
    {
        pblKfCommit( k, 1 );
        return( -1 );
    }

    /*
     * only data that is longer than PBLDATALENGTH
     * bytes is written to data blocks
     */
    if( datalen > PBLDATALENGTH )
    {
        /*
         * append the data to the file
         */
        item.datablock = pblDataAppend( kf, data, datalen, &item.dataoffset );
        if( item.datablock < 1 )
        {
            pblKfCommit( k, 1 );
            return( -1 );
        }
        item.data = 0;
    }
    else
    {
        item.datablock = 0;
        item.dataoffset = 0;
        item.data = data;
    }

    /*
     * prepare the data item to be inserted
     */
    item.level      = 0;
    item.keylen     = keylen;
    item.keycommon  = 0;
    item.key        = key;
    item.datalen    = datalen;

    /*
     * push the item to the insert stack of the file
     */
    rc = pblItemSave( &item );
    if( rc )
    {
        pblKfCommit( k, 1 );
        return( -1 );
    }

    /*
     * insert all items that are on the insert stack
     */
    while( itemListHead )
    {
        insertitem = itemListHead;

        /*
         * we always start the insert at the root block
         */
        blockno = 0;
        parentblock = -1;
        parentindex = -1;

        /*
         * handle all levels of the tree
         */
        while( !pbl_errno )
        {
            block = pblBlockGet( kf, blockno );
            if( !block )
            {
                break;
            }

            /*
             * set the links to the parentblock of the block
             */
            block->parentblock = parentblock;
            block->parentindex = parentindex;

            /*
             * if the item has to be inserted in this level
             */
            if( block->level <= insertitem->level )
            {
                block = pblBlockGetWriteable( kf, blockno );
                if( !block )
                {
                    break;
                }

                index = pblItemAdd( kf, block, insertitem );
                if( index < 0 )
                {
                    if( pbl_errno == PBL_ERROR_NOFIT )
                    {
                        pbl_errno = 0;

                        /*
                         * split the root block or a normal block
                         */
                        if( blockno )
                        {
                            rc = pblSplit( kf, block );
                        }
                        else
                        {
                            rc = pblSplitRoot( kf );
                        }
                        if( !rc )
                        {
                            split = 1;
                        }
                    }
                    break;
                }

                /*
                 * insert was successful
                 */
                if( block->level == 0 )
                {
                    /*
                     * set values of current record
                     */
                    kf->blockno = block->blockno;
                    kf->index   = index;
                }

                /*
                 * release the item that was inserted
                 */
                pblItemRelease( );

                break;
            }

            for(;;)
            {
                block = pblBlockGet( kf, blockno );
                if( !block )
                {
                    break;
                }

                /*
                 * set the links to the parentblock of the block
                 */
                block->parentblock = parentblock;
                block->parentindex = parentindex;

                if( pblBlockKeysExpand( block ))
                {
                    break;
                }

                /*
                 * item has to be inserted on a lower level, find out where
                 *
                 * we either insert into the last subtree, or into the
                 * greatest smaller subtree
                 */
                index = pblItemFind( kf, block, insertitem, PBLLA );
                if( index < 0 )
                {
                    if( pbl_errno == PBL_ERROR_NOT_FOUND )
                    {
                        pbl_errno = 0;
                        index = pblItemFind( kf, block, insertitem, PBLLT );
                    }

                    if( index < 0 )
                    {
                        break;
                    }
                }

                rc = pblItemGet( block, index, &item );
                if( rc )
                {
                    break;
                }

                /*
                 * see if we can merge blocks before the insert
                 */
                if( !split )
                {
                    rc = pblBlockMerge( kf, blockno,
                                        index, item.datablock );
                    if( rc > 0 )
                    {
                        continue;
                    }
                    else if( rc < 0 )
                    {
                        break;
                    }
                }

                /*
                 * get the blockno of the relevant child block
                 */
                blockno = item.datablock;
                parentblock = block->blockno;
                parentindex = index;

                pbl_errno = 0;
                break;
            }
        }

        /*
         * if an error occurred during this insert
         */
        if( pbl_errno )
        {
            break;
        }
    }

    saveerrno = pbl_errno;

    pblItemReleaseAll( );

    if( saveerrno )
    {
        pbl_errno = saveerrno;
        kf->blockno = -1;
        kf->index = -1;
        pblKfCommit( k, 1 );
        return( -1 );
    }

    pblKfCommit( k, 0 );
    return( 0 );
}

/*
 * UPDATE functions
 */
static PBLBLOCK_t * pblPositionCheck( PBLKFILE_t *kf )
{
    PBLBLOCK_t * block;
    int          index;

    /*
     * check if the current block is set for the file
     */
    if( kf->blockno < 0 )
    {
        pbl_errno = PBL_ERROR_POSITION;
        return( 0 );
    }

    /*
     * get the current block to memory
     */
    block = pblBlockGet( kf, kf->blockno );
    if( !block )
    {
        return( 0 );
    }
    index = kf->index;

    /*
     * if we are positioned on our pseudo magic item, we set to next item
     */
    if(( index == 0 ) && ( !block->pblock || !block->blockno ))
    {
        index = 1;
    }

    /*
     * if the index is negative, we actually are set on the last item of
     * the predecessor of the current block, or if the there is no predecessor
     * on the first item of the current block
     */
    while( index < 0 )
    {
        /*
         * if we are on the first block, we need the second item, because the
         * the first item is our pseudo magic item
         */
        if( !block->pblock || !block->blockno )
        {
            index = 1;
        }
        else
        {
            block = pblBlockGet( kf, block->pblock );
            if( !block )
            {
                return( 0 );
            }

            if( block->nentries )
            {
                index = block->nentries - 1;
            }
        }
    }

    while( ( int )index >= ( int )block->nentries )
    {
        /*
         * if there is no successor of the current block, we have to stay here
         * the rootblock never has a successor !
         */
        if( !block->nblock || !block->blockno )
        {
            break;
        }
        else
        {
            block = pblBlockGet( kf, block->nblock );
            if( !block )
            {
                return( 0 );
            }

            if( block->nentries )
            {
                index = 0;
            }
        }
    }

    while( ( int )index >= ( int )block->nentries )
    {
        /*
         * if the block has entries, we take the last one
         */
        if( block->nentries )
        {
            index = block->nentries - 1;
        }
        else if( !block->pblock || !block->blockno )
        {
            /*
             * this is a structure error, because the first block always has
             * at least one item, our pseudo magic item
             */
            pbl_errno = PBL_ERROR_BAD_FILE;
            kf->blockno = -1;
            return( 0 );
        }
        else
        {
            block = pblBlockGet( kf, block->pblock );
            if( !block )
            {
                return( 0 );
            }

            if( block->nentries )
            {
                index = block->nentries - 1;
            }
        }
    }

    /*
     * if we ended up positioning at our pseudo item, the file does not
     * have any other items
     */
    if(( index == 0 ) && ( !block->pblock || !block->blockno ))
    {
        pbl_errno = PBL_ERROR_NOT_FOUND;
        kf->blockno = -1;
        return( 0 );
    }

    kf->blockno = block->blockno;
    kf->index = index;

    return( block );
}

static long pblDataWrite(
PBLKFILE_t * file,
char       * data,
long         blockno,
long         blockoffset,
long         datalen
)
{
    long         diff;
    long         bytesWritten = 0;
    int          nbytes;
    PBLBLOCK_t * datablock = (PBLBLOCK_t *) 0;

    while( bytesWritten < datalen )
    {
        datablock = pblBlockGetWriteable( file, blockno );
        if( !datablock )
        {
            return( -1 );
        }

        if( datablock->level != 255 )
        {
            pbl_errno = PBL_ERROR_BAD_FILE;
            return( -1 );
        }

        diff = datalen - bytesWritten;
        if( diff > ( PBLDATASIZE - blockoffset ) )
        {
            nbytes = PBLDATASIZE - blockoffset;
        }
        else
        {
            nbytes = ((int ) ( diff % PBLDATASIZE));
        }

        if( nbytes > 0 )
        {
            memcpy((void *) &(datablock->data[ blockoffset ]),
                   (void *) data,
                   nbytes );
            datablock->dirty = 1;

            bytesWritten += nbytes;
            data         += nbytes;
        }

        if( bytesWritten < datalen )
        {
            /*
             * get offset of next block and set blockoffset to beginning of
             * data
             */
            blockno     = datablock->nblock;
            blockoffset = PBLHEADERSIZE;
        }
    }

    return( bytesWritten );
}

/**
 * delete the current record of the key file.
 *
 * the current record of the file is set to the next record or
 * if the last record is deleted, to the previous record,
 *
 * if there are no more records in the file after the delete
 * the current record is of course unpositioned
 *
 * <P>
 * <B>RESTRICTIONS</B>:
 * <BR> - the file must be open for update,
 * <BR> - no space will be given back to the file system,
 * <BR> - if an index block and its successor or its predeccessor
 *        together use less than half of a block the two blocks are merged
 *
 * @return int rc == 0: call went ok
 * @return int rc != 0: some error occured, see pbl_errno
 */

int pblKfDelete(
pblKeyFile_t  * k       /** key file to delete record from                    */
)
{
    PBLKFILE_t *    kf = ( PBLKFILE_t * ) k;
    long            parentblock;
    PBLBLOCK_t    * block;
    int             rc;

    pbl_errno = 0;

    /*
     * start a transaction on the key file
     */
    pblKfStartTransaction( k );
    if( pblBlockListTruncate())
    {
        pblKfCommit( k, 1 );
        return( -1 );
    }

    if( !kf->update )
    {
        pblKfCommit( k, 1 );
        pbl_errno = PBL_ERROR_NOT_ALLOWED;
        return( -1 );
    }

    /*
     * make sure current record of the file is positioned
     */
    block = pblPositionCheck( kf );
    if( !block )
    {
        pblKfCommit( k, 1 );
        return( -1 );
    }

    /*
     * read the block the current item is on
     */
    block = pblBlockGetWriteable( kf, kf->blockno );
    if( !block )
    {
        pblKfCommit( k, 1 );
        return( -1 );
    }

    /*
     * delete the current item
     */
    rc = pblItemRemove( block, kf->index );
    if( rc )
    {
        pblKfCommit( k, 1 );
        return( rc );
    }

    /*
     * we deleted an item, now see if we can merge some blocks
     */
    for(;;)
    {
        if( block->parentblock < 0 || block->parentindex < 0 )
        {
            rc = 0;
            break;
        }

        /*
         * remember the blocknumber of the parent block
         */
        parentblock = block->parentblock;

        /*
         * see whether some blocks can be merged because of the delete
         */
        rc = pblBlockMerge( kf, block->parentblock,
                            block->parentindex, block->blockno );
        if( rc < 1 )
        {
            break;
        }

        /*
         * the merge deleted an item on the parent block, read that block
         */
        block = pblBlockGetWriteable( kf, parentblock );
        if( !block )
        {
            pblKfCommit( k, 1 );
            return( -1 );
        }
    }

    if( rc )
    {
        pblKfCommit( k, 1 );
        return( -1 );
    }

    pblKfCommit( k, 0 );
    return( rc );
}

/**
 * update the data of the current record
 *
 * the current record of the file is updated with the new data given
 *
 *
 * <P>
 * <B>RESTRICTIONS</B>:
 * <BR> - the file must be open for update,
 * <BR> - if the new datalen of the record is not bigger than the old datalen,
 *        the data will be updated in place, otherwise the new data of the
 *        record will be appended to the file, the space previously used for 
 *        the data of the record will not be reused in this case,
 * <BR> - data must point to the new data be inserted,
 * <BR> - datalen must not be negative,
 * <BR> - if datalen == 0, the pointer data is not evaluated at all
 *
 * @return int rc == 0: call went ok
 * @return int rc != 0: some error occured, see pbl_errno
 */

int pblKfUpdate(
pblKeyFile_t  * k,      /** key file to delete record from                    */
unsigned char * data,   /** new data to update with                           */
long            datalen /** length of the new data                            */
)
{
    PBLKFILE_t      * kf = ( PBLKFILE_t * ) k;
    PBLITEM_t         item;
    PBLBLOCK_t      * block;
    long              rc;
    unsigned char     key[ PBLKEYLENGTH ];

    pbl_errno = 0;

    /*
     * start a transaction on the key file
     */
    pblKfStartTransaction( k );
    if( pblBlockListTruncate())
    {
        pblKfCommit( k, 1 );
        return( -1 );
    }

    if( !kf->update )
    {
        pblKfCommit( k, 1 );
        pbl_errno = PBL_ERROR_NOT_ALLOWED;
        return( -1 );
    }

    if( pblParamsCheck( (char*)1, 1, data, datalen ))
    {
        pblKfCommit( k, 1 );
        return( -1 );
    }

    /*
     * make sure current record of the file is positioned
     */
    block = pblPositionCheck( kf );
    if( !block )
    {
        pblKfCommit( k, 1 );
        return( -1 );
    }

    /*
     * read the block the current item is on
     */
    block = pblBlockGetWriteable( kf, kf->blockno );
    if( !block )
    {
        pblKfCommit( k, 1 );
        return( -1 );
    }

    /*
     * read the item
     */
    rc = pblItemGet( block, kf->index, &item );
    if( rc )
    {
        pblKfCommit( k, 1 );
        return( -1 );
    }

    if( datalen == item.datalen )
    {
        /*
         * if the data is to be stored on an index block
         */
        if( datalen <= PBLDATALENGTH )
        {
            /*
             * update in place
             */
            if( datalen > 0 )
            {
                memcpy( item.data, data, datalen );
                block->dirty = 1;
            }

            pblKfCommit( k, 0 );
            return( 0 );
        }

        /*
         * update the data in place
         */
        rc = pblDataWrite( kf, data, item.datablock, item.dataoffset, datalen );
        if( rc != datalen )
        {
            pblKfCommit( k, 1 );
            return( -1 );
        }

        pblKfCommit( k, 0 );
        return( 0 );
    }

    if( item.keycommon )
    {
        if( pblBlockKeysExpand( block ))
        {
            pblKfCommit( k, 1 );
            return( -1 );
        }

        /*
         * read the item to get its real key
         */
        rc = pblItemGet( block, kf->index, &item );
        if( rc )
        {
            pblKfCommit( k, 1 );
            return( -1 );
        }
    }

    /*
     * we do a delete and an insert of the record
     */
    item.keylen &= 0xff;
    pbl_memlcpy( key, sizeof( key ), item.key, item.keylen );

    rc = pblKfDelete( k );
    if( rc )
    {
        pblKfCommit( k, 1 );
        return( rc );
    }

    rc = pblKfInsert( k, key, item.keylen, data, datalen );
    if( rc )
    {
        pblKfCommit( k, 1 );
        return( rc );
    }

    pblKfCommit( k, 0 );

    return( rc );
}

/*
 * READ functions
 */
/*
 * recursive find procedure for records
 */
static long pblFindRec(
PBLKFILE_t    * kf,
int             mode,
long            blockno,
long            parentblock,
int             parentindex,
PBLITEM_t     * item
)
{
    PBLITEM_t       curitem;
    PBLBLOCK_t    * block;
    int             index;
    long            rc;
    int             which;
    int             direction;

    /*
     * get the block to memory
     */
    block = pblBlockGet( kf, blockno );
    if( !block )
    {
        return( -1 );
    }

    block->parentblock = parentblock;
    block->parentindex = parentindex;

    if( pblBlockKeysExpand( block ))
    {
        return( -1 );
    }

    /*
     * level 0, terminate the recursion
     */
    if( block->level == 0 )
    {
        /*
         * find the item on the block, first that matches
         */
        index = pblItemFind( kf, block, item, mode );
        if( index < 0 )
        {
            return( -1 );
        }

        /*
         * make sure nobody is finding our pseudo record
         */
        if(( index == 0 ) && ( !block->pblock || !block->blockno ))
        {
            pbl_errno = PBL_ERROR_NOT_FOUND;
            return( -1 );
        }

        rc = pblItemGet( block, index, &curitem );
        if( rc )
        {
            return( -1 );
        }

        /*
         * find was successful set values of current record
         */
        kf->blockno = block->blockno;
        kf->index = index;

        /*
         * we return the datalength of the item
         */
        return( curitem.datalen );
    }

    direction = 1;
    switch( mode )
    {
      case PBLLT:
        which     = PBLLT;
        direction = -1;
        break;

      case PBLFI:
      case PBLEQ:
        which     = PBLLT;
        break;

      case PBLLA:
      case PBLGT:
        which     = PBLLA;
        break;

      default:
        pbl_errno  = PBL_ERROR_PARAM_MODE;
        return( -1 );
    }

    /*
     * find the subtree where to continue the find
     */
    index = pblItemFind( kf, block, item, which );
    if( index < 0 )
    {
        if( which == PBLLA )
        {
            if( pbl_errno == PBL_ERROR_NOT_FOUND )
            {
                pbl_errno = 0;
                index = pblItemFind( kf, block, item, PBLLT );
            }
        }
    }

    /*
     * search in all possible subtrees
     */
    for( ; index >= 0 && index < (int)block->nentries; index += direction )
    {
        /*
         * check if subtree can contain the item
         */
        rc = pblItemGet( block, index, &curitem );
        if( rc )
        {
            return( -1 );
        }

        rc = pblItemCompare( kf, &curitem, item );
        if(( rc > 0 ) && ( mode != PBLGT ))
        {
            pbl_errno = PBL_ERROR_NOT_FOUND;
            return( -1 );
        }

        /*
         * recursive call to find procedure
         */
        rc = pblFindRec( kf, mode, curitem.datablock, blockno, index, item );
        if( rc >= 0 )
        {
            /*
             * find was successful
             */
            return( rc );
        }

        /*
         * if an error other than PBL_ERROR_NOT_FOUND occured, we give up
         */
        if( pbl_errno != PBL_ERROR_NOT_FOUND )
        {
            return( -1 );
        }
        else
        {
            pbl_errno = 0;
        }

        /*
         * get the block to memory because during the recursive call
         * it might have become a victim
         */
        block = pblBlockGet( kf, blockno );
        if( !block )
        {
            return( -1 );
        }

        block->parentblock = parentblock;
        block->parentindex = parentindex;

        if( pblBlockKeysExpand( block ))
        {
            return( -1 );
        }
    }

    /*
     * couldn't find the item, tell the caller
     */
    pbl_errno = PBL_ERROR_NOT_FOUND;
    return( -1 );
}

static long pblDataGet(
PBLKFILE_t * file,
char       * data,
long         blockno,
long         blockoffset,
long         datalen
)
{
    long         diff;
    long         bytesRead = 0;
    int          nbytes;
    PBLBLOCK_t * datablock = (PBLBLOCK_t *) 0;

    while( bytesRead < datalen )
    {
        datablock = pblBlockGet( file, blockno );
        if( !datablock )
        {
            return( -1 );
        }

        if( datablock->level != 255 )
        {
            pbl_errno = PBL_ERROR_BAD_FILE;
            return( -1 );
        }

        diff = datalen - bytesRead;
        if( diff > ( PBLDATASIZE - blockoffset ) )
        {
            nbytes = PBLDATASIZE - blockoffset;
        }
        else
        {
            nbytes = ((int ) ( diff % PBLDATASIZE));
        }

        if( nbytes > 0 )
        {
            memcpy((void *) data,
                   (void *) &(datablock->data[ blockoffset ]),
                   nbytes );

            bytesRead += nbytes;
            data      += nbytes;
        }

        if( bytesRead < datalen )
        {
            /*
             * get number of next block and set blockoffset to beginning of
             * data
             */
            blockno     = datablock->nblock;
            blockoffset = PBLHEADERSIZE;
        }
    }

    return( bytesRead );
}

/**
 * find a record in a key file, set the current record
 *
 * parameter mode specifies which record to find relative
 * to the search key specified by skey and skeylen.
 * the following values for mode are possible
 *
 * <BR><B> PBLEQ </B> - find a record whose key is equal to skey
 * <BR><B> PBLFI </B> - find the first record that is equal
 * <BR><B> PBLLA </B> - find the last record that is equal
 * <BR><B> PBLGE </B> - find the last record that is equal or the smallest
 *                      record that is greater
 * <BR><B> PBLGT </B> - find the smallest record that is greater
 * <BR><B> PBLLE </B> - find the first record that is equal or the biggest
 *                      record that is smaller
 * <BR><B> PBLLT </B> - find the biggest record that is smaller
 *
 * keep in mind that PBL allows multiple records with the same key.
 *
 * <P>
 * <B>RESTRICTIONS</B>:
 * <BR> - the out parameter okey must point to a memory area that is
 *        big enough to hold any possible key, i.e 255 bytes
 *
 * @return long rc >= 0:
 * <UL>
 * <LI>                  call went ok,
 *                       the value returned is the length
 *                       of the data of the record found,
 * <LI>                  the length of the key of the record is set in
 *                       the out parameter okeylen,
 * <LI>                  the key of the record is copied to okey,
 * <LI>                  the current record of the file is set to the
 *                       record found
 * </UL>
 *
 * @return long rc <  0:
 * <UL>
 * <LI>                  some error occured, see pbl_errno
 *                       especially PBL_ERROR_NOT_FOUND, if there is no 
 *                       matching record
 * </UL>
 */
long pblKfFind(
pblKeyFile_t  * k,       /** key file to search in                            */
int             mode,    /** mode to use for search                           */
unsigned char * skey,    /** key to use for search                            */
int             skeylen, /** length of search key                             */
unsigned char * okey,    /** buffer for result key                            */
int           * okeylen  /** length of the result key after return            */
)
{
    PBLKFILE_t    * kf = ( PBLKFILE_t * ) k;
    PBLBLOCK_t    * block;
    PBLITEM_t       item;
    long            rc;
    int             which;

    pbl_errno = 0;

    rc = pblParamsCheck( skey, skeylen, (char*)0, 0 );
    if( rc )
    {
        return( -1 );
    }

    /*
     * prepare the data item to be found
     */
    memset( &item, 0, sizeof( item ));
    item.keylen = skeylen;
    item.key    = skey;

    if( mode == PBLLE )
    {
        which = PBLFI;
    }
    else if( mode == PBLGE )
    {
        which = PBLLA;
    }
    else
    {
        which = mode;
    }

    /*
     * we always start the find at the root block
     */
    rc = pblFindRec( kf, which, 0, -1, -1, &item );
    if( rc < 0 )
    {
        if( pbl_errno == PBL_ERROR_NOT_FOUND )
        {
            if( mode == PBLLE )
            {
                rc = pblFindRec( kf, PBLLT, 0, -1, -1, &item );
            }
            else if( mode == PBLGE )
            {
                rc = pblFindRec( kf, PBLGT, 0, -1, -1, &item );
            }
        }
    }

    if( rc < 0 )
    {
        return( -1 );
    }

    /*
     * get the current block to memory
     */
    block = pblBlockGet( kf, kf->blockno );
    if( !block )
    {
        return( -1 );
    }

    /*
     * if we need the key of the record
     */
    if( okey )
    {
        if( pblBlockKeysExpand( block ))
        {
            return( -1 );
        }
    }

    /*
     * read the item
     */
    rc = pblItemGet( block, kf->index, &item );
    if( rc )
    {
        return( -1 );
    }

    /*
     * set the out parameters, if a buffer is supplied
     */
    if( okey && okeylen )
    {
        *okeylen = item.keylen;
        pbl_memlcpy( okey, PBLKEYLENGTH, item.key, item.keylen );
    }

    return( item.datalen );
}

/**
 * read the data of the current record of the file
 *
 * the caller can restrict the number of bytes read by
 * specifying the maximum number of bytes to read by parameter
 * datalen, if datalen is 0, all bytes stored for the
 * current record are copied to the buffer pointed to by data.
 * <P>
 * <B>RESTRICTIONS</B>:
 * <BR> - data must point to an area of memory being big enough to hold
 *        the bytes copied
 * <BR> - datalen must not be negative, it is ignored otherwise
 *
 * @return int rc == 0: call went ok, rc is the number of bytes copied
 * @return int rc != 0: some error occured, see pbl_errno
 */

long pblKfRead(
pblKeyFile_t  * k,      /** key file to read from                             */
unsigned char * data,   /** data to insert                                    */
long            datalen /** length of the data                                */
)
{
    PBLKFILE_t    * kf = ( PBLKFILE_t * ) k;
    PBLITEM_t       item;
    PBLBLOCK_t    * block;
    long            rc;

    pbl_errno = 0;

    rc = pblParamsCheck( (char*)1, 1, (char*)data, datalen );
    if( rc )
    {
        return( -1 );
    }

    /*
     * check position of current record
     */
    block = pblPositionCheck( kf );
    if( !block )
    {
        return( -1 );
    }

    /*
     * read the item
     */
    rc = pblItemGet( block, kf->index, &item );
    if( rc )
    {
        return( -1 );
    }

    /*
     * the caller can restrict the number of bytes read
     */
    if( datalen > 0 )
    {
        if( datalen > item.datalen )
        {
            datalen = item.datalen;
        }
    }
    else
    {
        datalen = item.datalen;
    }

    /*
     * if the data is stored on an index block
     */
    if( datalen <= PBLDATALENGTH )
    {
        memcpy( data, item.data, datalen );
        return( datalen );
    }

    /*
     * the data is stored on a data block, read it from the file
     */
    rc = pblDataGet( kf, data, item.datablock, item.dataoffset, datalen );
    if( rc != datalen )
    {
        return( -1 );
    }

    return( datalen );
}

/**
 * set current record to a record with a relative position index
 *
 * this function is only to be used through the macro functions:
 *
 * <BR><B> \Ref{pblKfThis}( k, okey, okeylen ) </B> read key of current record
 * <BR><B> \Ref{pblKfNext}( k, okey, okeylen ) </B> read key of next record
 * <BR><B> \Ref{pblKfPrev}( k, okey, okeylen ) </B> read key of previous record
 *
 * @return long rc >= 0:
 * <UL>
 * <LI>                  call went ok,
 *                       the value returned is the length
 *                       of the data of the record found,
 * <LI>                  the length of the key of the record is set in
 *                       the out parameter okeylen,
 * <LI>                  the key of the record is copied to okey,
 * <LI>                  the current record of the file is set to the
 *                       record found
 * </UL>
 *
 * @return long rc <  0:
 * <UL>
 * <LI>                  some error occured, see pbl_errno
 * </UL>
 */

long pblKfGetRel(
pblKeyFile_t  * k,        /** key file to position in                */
long            relindex, /** index relative to current record       */
char          * okey,     /** buffer for result key                  */
int           * okeylen   /** length of the result key after return  */
)
{
    PBLKFILE_t * kf = ( PBLKFILE_t * ) k;
    PBLITEM_t    item;
    PBLBLOCK_t * block;
    int          index;
    long         rc;

    pbl_errno = 0;

    /*
     * check position of current record
     */
    block = pblPositionCheck( kf );
    if( !block )
    {
        return( -1 );
    }

    /*
     * start searching at current block and current index
     */
    index = kf->index;

    /*
     * if we want an item that is to the left of the current item
     */
    while( !pbl_errno && relindex < 0 )
    {
        relindex++;

        /*
         * as long as we can go to the left on current block
         */
        if( index > 0 )
        {
            index--;
            continue;
        }

        /*
         * find a block that has entries
         */
        for(;;)
        {
            /*
             * go to previous block
             */
            if( !block->pblock || !block->blockno )
            {
                pbl_errno = PBL_ERROR_NOT_FOUND;
                break;
            }

            block = pblBlockGet( kf, block->pblock );
            if( !block )
            {
                break;
            }

            if( block->nentries )
            {
                index = block->nentries - 1;
                break;
            }
        }
    }

    /*
     * if we want an item that is to the right of the current item
     */
    while( !pbl_errno && relindex > 0 )
    {
        relindex--;

        /*
         * as long as we can go to the right on this block
         */
        if(( int )( index + 1 ) < ( int )block->nentries )
        {
            index++;
            continue;
        }

        /*
         * find a block that has at least one entry
         */
        for(;;)
        {
            /*
             * go to next block, but beware that rootblock has no next
             */
            if( !block->nblock || !block->blockno )
            {
                pbl_errno = PBL_ERROR_NOT_FOUND;
                break;
            }

            block = pblBlockGet( kf, block->nblock );
            if( !block )
            {
                break;
            }

            if( block->nentries )
            {
                index = 0;
                break;
            }
        }
    }

    /*
     * if an error occured we tell the caller
     */
    if( pbl_errno )
    {
        return( -1 );
    }

    /*
     * if we need the key of the record
     */
    if( okey )
    {
        if( pblBlockKeysExpand( block ))
        {
            return( -1 );
        }
    }

    /*
     * read the item
     */
    rc = pblItemGet( block, index, &item );
    if( rc )
    {
        return( -1 );
    }

    /*
     * if the item we are standing on is our first record, we have reached the
     * end of file while reading backward
     */
    if( item.keylen == 0 )
    {
        pbl_errno = PBL_ERROR_NOT_FOUND;
        return( -1 );
    }

    /*
     * set the out parameters, if a buffer is supplied
     */
    if( okey && okeylen )
    {
        *okeylen = item.keylen;
        pbl_memlcpy( okey, PBLKEYLENGTH, item.key, item.keylen );
    }

    kf->blockno = block->blockno;
    kf->index = index;

    return( item.datalen );
}
 
/**
 * set current record to a record with an absolute position index
 *
 * this function is only to be used through the macro functions:
 *
 * <BR><B> \Ref{pblKfFirst}( k, okey, okeylen ) </B> read key of first record
 * <BR><B> \Ref{pblKfLast}( k, okey, okeylen ) </B> read key of last record
 *
 * @return long rc >= 0:
 * <UL>
 * <LI>                  call went ok,
 *                       the value returned is the length
 *                       of the data of the record found,
 * <LI>                  the length of the key of the record is set in
 *                       the out parameter okeylen,
 * <LI>                  the key of the record is copied to okey,
 * <LI>                  the current record of the file is set to the
 *                       record found
 * </UL>
 *
 * @return long rc <  0:
 * <UL>
 * <LI>                  some error occured, see pbl_errno
 * </UL>
 */
 
long pblKfGetAbs(
pblKeyFile_t  * k,        /** key file to position in                */
long            absindex, /** index of record to positon to          */
char          * okey,     /** buffer for result key                  */
int           * okeylen   /** length of the result key after return  */
)
{
    PBLKFILE_t * kf = ( PBLKFILE_t * ) k;
    PBLITEM_t    item;
    PBLBLOCK_t * block;
    int          index;
    long         rc;

    pbl_errno = 0;

    /*
     * start searching at rootblock
     */
    block = pblBlockGet( kf, 0 );
    if( !block )
    {
        return( -1 );
    }

    /*
     * step down through tree to level 0
     */
    while( !pbl_errno && block->level )
    {
        if( absindex >= 0 )
        {
           index = 0;
        }
        else
        {
           index = block->nentries - 1;
        }

        rc = pblItemGet( block, index, &item );
        if( rc )
        {
            break;
        }

        /*
         * get the relevant child block
         */
        block = pblBlockGet( kf, item.datablock );
        if( !block )
        {
            return( -1 );
        }
    }

    /*
     * if no error yet, we do a relative get
     */
    if( !pbl_errno )
    {
        /*
         * prepare relative get
         */
        kf->blockno = block->blockno;

        if( absindex >= 0 )
        {
            kf->index = -1;
            return( pblKfGetRel( k, absindex, okey, okeylen ));
        }
        else
        {
            kf->index = block->nentries;
            return( pblKfGetRel( k, absindex + 1, okey, okeylen ));
        }
    }

    return( -1 );
}

/*
------------------------------------------------------------------------------
  FUNCTION:     pblKfXXX

  DESCRIPTION:  These macros allow to position the current record and
                to read the key, the keylen and the datalen of the new
                current record

                The following macros exist:

                pblKfFirst: set the current record to the first record of the
                            file

                pblKfLast:  set the current record to the last record of the
                            file

                pblKfNext:  set the current record to the record of the
                            file after the old current record, of course the
                            record of the file must be positioned before that

                pblKfPrev:  set the current record to the record of the
                            file before the old current record, of course the
                            record of the file must be positioned before that

                pblKfThis:  this function can be used to read the key, keylen
                            and datalen of the current record, the current
                            record is not changed by this

  RESTRICTIONS: the out parameter okey must point to a memory area that is
                big enough to hold any possible key, i.e 255 bytes

  RETURNS:      long rc >= 0: call went ok, the value returned is the length
                              of the data of the current record,
                              the length of the key of the record is set in
                              the out parameter okeylen,
                              the key of the record is copied to okey,

                long rc <  0: some error occured, see pbl_errno
                              PBL_ERROR_NOT_FOUND, there is no matching record
                              PBL_ERROR_POSITION, current record not set yet
------------------------------------------------------------------------------
*/


int pblKfBlockPrint(
char * path,       /** path of file to create                                 */
long blockno       /** number of block to print                               */
)
{
    PBLITEM_t     item;
    PBLKFILE_t  * kf;
    PBLBLOCK_t  * block;
    int           bf;
    int           i;

    pbl_errno = 0;

    printf( "FILE %s, BLOCK %ld\n", path, blockno );

    bf = pbf_open( path, 0, PBLFILEBLOCKS, PBLDATASIZE );
    if( -1 == bf )
    {
        printf( "pbf_open failed, pbl_errno %d\n", pbl_errno );
        return( -1 );
    }

    /*
     * get and init file structure
     */
    kf = pbl_malloc0( "pblKfBlockPrint FILE", sizeof( PBLKFILE_t ));
    if( !kf )
    {
        printf( "pbl_malloc0 failed, pbl_errno %d\n", pbl_errno );
        pbf_close( bf );
        return( -1 );
    }
    kf->magic      = rcsid;
    kf->bf         = bf;
    kf->update     = 0;
    kf->filesettag = NULL;
    kf->blockno    = -1;
    kf->index      = -1;

    pblnfiles++;

    /*
     * get the block
     */
    block = pblBlockGet( kf, blockno );
    if( !block )
    {
        printf( "pblBlockGet failed, pbl_errno %d\n", pbl_errno );
        pblKfClose( ( pblKeyFile_t * ) kf );
        return( -1 );
    }

    if( block->level == 255 )
    {
        printf( "datablock\n" );
        pblKfClose( ( pblKeyFile_t * ) kf );
        return( 0 );
    }

    printf( "level %d, pblock %ld, nblock %ld, nentries %d, free %d\n",
            block->level, block->pblock, block->nblock,
            (int)block->nentries, block->free );

    if( block->nentries < 1 )
    {
        pblKfClose( ( pblKeyFile_t * ) kf );
        return( 0 );
    }
    
    if( pblBlockKeysExpand( block ))
    {
        printf( "pblBlockKeysExpand failed, pbl_errno %d\n", pbl_errno );
        pblKfClose( ( pblKeyFile_t * ) kf );
        return( -1 );
    }

    for( i = 0; i < (int)block->nentries; i++ )
    {
        char * ptr;

        pblItemGet( block, i, &item );

        if( item.key )
        {
            ptr = item.key;
        }
        else
        {
            ptr = "NULL";
        }

        printf( "%03d %d %.*s, common %d, datalen %ld, block %ld, offset %ld\n",
                i, item.keylen,
                item.keylen, ptr,
                item.keycommon, item.datalen,
                item.datablock, item.dataoffset );
    }

    pblKfClose( ( pblKeyFile_t * ) kf );
    return( 0 );
}

