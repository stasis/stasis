#ifndef _PBL_H_
#define _PBL_H_
/*
 pbl.h - external include file of library

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
    Revision 1.3  2006/04/11 02:20:21  sears
    removed memcpy() calls from inMemoryLog; added "const" qualifier to many LogEntry pointers.

    Revision 1.2  2005/03/02 05:46:29  sears
    Compiles on FreeBSD!

    Revision 1.1.1.1  2004/06/24 21:11:33  sears
    Need to send laptop in for warranty service, so it's time to put this code into CVS. :)

    Vs. the paper version of LLADD, this version has a re-written logger + recovery system.  It also includes unit tests and API documentation.

    Revision 1.4  2004/06/09 21:27:40  sears
    Final CVS checkin before major refactoring.

    Revision 1.3  2003/12/11 10:48:16  jim
    compiles, not link. added quasi-pincount, shadow pages

    Revision 1.2  2003/12/11 09:21:20  jim
    update includes

    Revision 1.1  2003/12/11 09:10:48  jim
    pbl

    Revision 1.2  2002/09/12 20:47:18  peter
    added the isam file handling to the library

    Revision 1.1  2002/09/05 13:44:12  peter
    Initial revision


*/

#ifdef __cplusplus
extern "C" {
#endif

/*
 * make sure "strings <exe> | grep Id | sort -u" shows the source file versions
 */
static char* _PBL_H_id = "$Id$";
static int   _PBL_H_fct() { return( _PBL_H_id ? 0 : _PBL_H_fct() ); }

#include <dirent.h>
  // for size_t
#include <stdio.h> 
/*****************************************************************************/
/* #defines                                                                  */
/*****************************************************************************/

#ifdef _WIN32

/*
 * some functions have strange names on windows
 */
#define strcasecmp   _stricmp
#define strncasecmp  _strnicmp
#define snprintf     _snprintf

#else

#ifndef O_BINARY
#define O_BINARY     0
#endif

#endif

#define PBL_ERRSTR_LEN                    2048

/** @name B: Files
 *  list of files of component
 *  <P>
 *  <B>FILES</B>
 *  <UL>
 *  <LI> <a href="../pbl.h">pbl.h</a> - the include file of the library
 *  <LI> <a href="../pbl.c">pbl.c</a> - source for the base functions
 *  <LI> <a href="../pblhash.c">pblhash.c</a> - source file for the
 *                                              hash functions
 *  <LI> <a href="../pblhttst.c">pblhttst.c</a> - source file for the
 *                                              hash function test frame
 *  <LI> <a href="../pblkf.c">pblkf.c</a> - source file for the key
 *                                              file functions
 *  <LI> <a href="../pblkftst.c">pblkftst.c</a> - source file for the
 *                                              key file handling test frame
 *  <LI> <a href="../pblisam.c">pblisam.c</a> - source file for the isam
 *                                              file functions
 *  <LI> <a href="../pbliftst.c">pbliftst.c</a> - source file for the
 *                                              isam file handling test frame
 *  <LI> <a href="../makefile">makefile</a> - a Unix makefile for the
 *                                              component
 *  <LI> <a href="../pblhttstdeb.dsp">pblhttstdeb.dsp</a> - a Microsoft Visual
 *                                              Studio 6.0 project file for
 *                                              hash table debug
 *  <LI> <a href="../pblkftstdeb.dsp">pblkftstdeb.dsp</a> - a Microsoft
 *                                              Visual Studio 6.0 project file
 *                                              for key file debug
 *  <LI> <a href="../pbliftstdeb.dsp">pbliftstdeb.dsp</a> - a Microsoft Visual
 *                                              Studio 6.0 project file for
 *                                              isam file debug
 *  <LI> <a href="../ISAM0001.LOG">ISAM0001.LOG</a> - a test case for the 
 *                                              isam file handling test frame
 *  <LI> <a href="../pbl.dxx">pbl.dxx</a> - the source for this document
 *  </UL>
 */

#define PBL_FILE_LIST

/** @name C: Error codes
 *  error codes of the pbl library
 *
 *  @field PBL_ERROR_OUT_OF_MEMORY out of memory
 *  @field PBL_ERROR_EXISTS        record already exists
 *  @field PBL_ERROR_NOT_FOUND     record not found
 *  @field PBL_ERROR_BAD_FILE      file structure damaged
 *  @field PBL_ERROR_PARAM_MODE    parameter mode is not valid
 *  @field PBL_ERROR_PARAM_KEY     parameter key is not valid
 *  @field PBL_ERROR_PARAM_KEYLEN  parameter keylen is not valid
 *  @field PBL_ERROR_PARAM_DATA    parameter data is not valid
 *  @field PBL_ERROR_PARAM_DATALEN parameter datalen is not valid
 *  @field PBL_ERROR_PARAM_INDEX   parameter index is not valid
 *  @field PBL_ERROR_CREATE        file system create error, see errno
 *  @field PBL_ERROR_OPEN          file system open error, see errno
 *  @field PBL_ERROR_SEEK          file system seek error, see errno
 *  @field PBL_ERROR_READ          file system read error, see errno
 *  @field PBL_ERROR_WRITE         file system write error, see errno
 *  @field PBL_ERROR_PROGRAM       an internal error in the code, debug it!!
 *  @field PBL_ERROR_NOFIT         internal error forcing a block split
 *  @field PBL_ERROR_NOT_ALLOWED file not open for update, operation not allowed
 *  @field PBL_ERROR_POSITION    current record is not positioned
 */
#define PBL_ERROR_BASE                    1000

#define PBL_ERROR_OUT_OF_MEMORY           ( PBL_ERROR_BASE + 1 )
#define PBL_ERROR_EXISTS                  ( PBL_ERROR_BASE + 2 )
#define PBL_ERROR_NOT_FOUND               ( PBL_ERROR_BASE + 3 )
#define PBL_ERROR_BAD_FILE                ( PBL_ERROR_BASE + 4 )
#define PBL_ERROR_PARAM_MODE              ( PBL_ERROR_BASE + 5 )
#define PBL_ERROR_PARAM_KEY               ( PBL_ERROR_BASE + 6 )
#define PBL_ERROR_PARAM_KEYLEN            ( PBL_ERROR_BASE + 7 )
#define PBL_ERROR_PARAM_DATA              ( PBL_ERROR_BASE + 8 )
#define PBL_ERROR_PARAM_DATALEN           ( PBL_ERROR_BASE + 9 )
#define PBL_ERROR_PARAM_INDEX             ( PBL_ERROR_BASE + 10 )

#define PBL_ERROR_CREATE                  ( PBL_ERROR_BASE + 20 )
#define PBL_ERROR_OPEN                    ( PBL_ERROR_BASE + 21 )
#define PBL_ERROR_SEEK                    ( PBL_ERROR_BASE + 22 )
#define PBL_ERROR_READ                    ( PBL_ERROR_BASE + 23 )
#define PBL_ERROR_WRITE                   ( PBL_ERROR_BASE + 24 )

#define PBL_ERROR_PROGRAM                 ( PBL_ERROR_BASE + 30 )
#define PBL_ERROR_NOFIT                   ( PBL_ERROR_BASE + 31 )

#define PBL_ERROR_NOT_ALLOWED             ( PBL_ERROR_BASE + 40 )
#define PBL_ERROR_POSITION                ( PBL_ERROR_BASE + 41 )

/** @name D: Definitions for Key File Parameters
  * DEFINES FOR PARAMETER <B> mode </B> OF \Ref{pblKfFind}()
  * @field PBLEQ                   any record that is equal
  * @field PBLFI                   first record that is equal
  * @field PBLLA                   last record that is equal
  * @field PBLGE                   last equal or first that is greater
  * @field PBLGT                   first that is greater
  * @field PBLLE                   first equal or last that is smaller
  * @field PBLLT                   last that is smaller
  */
#define PBLEQ               1
#define PBLFI               2
#define PBLLA               3
#define PBLGE               4
#define PBLGT               5
#define PBLLE               6
#define PBLLT               7

/** @name E: Definitions for ISAM Parameters
  * DEFINES FOR PARAMETER <B> which </B> OF \Ref{pblIsamGet}()
  * @field PBLTHIS                  get key and keylen of current record
  * @field PBLNEXT                  get key and keylen of next record
  * @field PBLPREV                  get key and keylen of previous record
  * @field PBLFIRST                 get key and keylen of first record
  * @field PBLLAST                  get key and keylen of last record
 */
#define PBLTHIS             1
#define PBLNEXT             2
#define PBLPREV             3
#define PBLFIRST            4
#define PBLLAST             5

/**
 * the maximum length of a key of the key file component,
 * @doc maximum length of a key, 255 for now
 */
#define PBLKEYLENGTH      255

/**
 * maximum data length of data being stored on index blocks of key files,
 * @doc maximum length of data stored with an item on the level 0 block, 1024
 * @doc data that is longer is stored on data blocks.
 */
#define PBLDATALENGTH    1024

/*****************************************************************************/
/* macros                                                                    */
/*****************************************************************************/

/*
 * The PBL_MEMTRACE define can be used for debugging the library,
 * if defined the library will log a line for all memory chunks
 * that are allocated for more than 3 minutes into the file ./pblmemtrace.log
 *
 * This can be used to detect heap memory lost by the code.
 * See also function pbl_memtrace_out in pbl.c
 */
 
/* #define PBL_MEMTRACE */
#ifdef  PBL_MEMTRACE

extern void pbl_memtrace_delete( void * data );
extern void pbl_memtrace_out( int checktime );

#define PBL_FREE( ptr ) if( ptr ){ pbl_memtrace_delete( ptr );\
                                   free( ptr ); ptr = 0; }

#else

/**
 * make free save against NULL pointers,
 * @doc also the parameter ptr is set to NULL
 */
#define PBL_FREE( ptr ) if( ptr ){ free( ptr ); ptr = 0; }

#endif

/**
 * macros for linear list handling,
 */
#define PBL_LIST_( Parameters )

/**
  * push an element to the beginning of a linear list
  */
#define PBL_LIST_PUSH( HEAD, TAIL, ITEM, NEXT, PREV )\
{\
    (ITEM)->PREV = 0;\
    if(( (ITEM)->NEXT = (HEAD) ))\
        { (ITEM)->NEXT->PREV = (ITEM); }\
    else\
        { (TAIL) = (ITEM); }\
    (HEAD) = (ITEM);\
}

/**
  * append an element to the end of a linear list
  */
#define PBL_LIST_APPEND( HEAD, TAIL, ITEM, NEXT, PREV )\
                         PBL_LIST_PUSH( TAIL, HEAD, ITEM, PREV, NEXT )

/**
  * remove an element from a linear list
  */
#define PBL_LIST_UNLINK( HEAD, TAIL, ITEM, NEXT, PREV )\
{\
    if( (ITEM)->NEXT )\
        { (ITEM)->NEXT->PREV = (ITEM)->PREV; }\
    else\
        { (TAIL) = (ITEM)->PREV; }\
    if( (ITEM)->PREV )\
        { (ITEM)->PREV->NEXT = (ITEM)->NEXT; }\
    else\
        { (HEAD) = (ITEM)->NEXT; }\
}

/*
 * SOME MACROS FOR KEY FILE READ FUNCTIONS
 */
/**
 * set the current record to the first record of the file
 */
#define pblKfFirst( KF, K, L ) pblKfGetAbs( KF,  0, K, L )

/**
 * set the current record to the last record of the file
 */
#define  pblKfLast( KF, K, L ) pblKfGetAbs( KF, -1, K, L )

/**
 * set the current record to the next record of the file
 */
#define  pblKfNext( KF, K, L ) pblKfGetRel( KF,  1, K, L )

/**
 * set the current record to the previous record of the file
 */
#define  pblKfPrev( KF, K, L ) pblKfGetRel( KF, -1, K, L )

/**
 * return the datalen of the current record
 */
#define  pblKfThis( KF, K, L ) pblKfGetRel( KF,  0, K, L )

/*****************************************************************************/
/* typedefs                                                                  */
/*****************************************************************************/

struct pblHashTable_s
{
    char * magic;
};

/**
  * the hash table type the pblHt* functions are dealing with,
  * @doc the details of the structure are hidden from the user
  */
typedef struct pblHashTable_s pblHashTable_t;

struct pblKeyFile_s
{
    char * magic;
};

/**
  * the key file type the pblKf* functions are dealing with,
  * @doc the details of the structure are hidden from the user
  */
typedef struct pblKeyFile_s pblKeyFile_t;

struct pblIsamFile_s
{
    char * magic;
};

/**
  * the ISAM file type the pblIsam* functions are dealing with,
  * @doc the details of the structure are hidden from the user
  */
typedef struct pblIsamFile_s pblIsamFile_t;

/*****************************************************************************/
/* variable declarations                                                     */
/*****************************************************************************/
/**
  * integer value used for returning error codes
  */
extern int    pbl_errno;

/**
  * character buffer used for returning error strings
  */
extern char * pbl_errstr;

/*****************************************************************************/
/* function declarations                                                     */
/*****************************************************************************/
extern void * pbl_malloc( char * tag, size_t size );
extern void * pbl_malloc0( char * tag, size_t size );
extern void * pbl_memdup( char * tag, void * data, size_t size );
extern void * pbl_mem2dup( char * tag, void * mem1, size_t len1,
                           void * mem2, size_t len2 );
extern int    pbl_memcmplen( void * left, size_t llen,
                             void * right, size_t rlen );
extern int    pbl_memcmp( void * left, size_t llen, void * right, size_t rlen );
extern size_t pbl_memlcpy( void * to, size_t tolen, void * from, size_t n );

extern void   pbl_ShortToBuf( unsigned char * buf, int s );
extern int    pbl_BufToShort( unsigned char * buf );
extern void   pbl_LongToBuf( unsigned char * buf, long l );
extern long   pbl_BufToLong( unsigned char * buf );
extern int    pbl_LongToVarBuf( unsigned char * buffer, unsigned long value );
extern int    pbl_VarBufToLong( unsigned char * buffer, long * value );
extern int    pbl_LongSize( unsigned long value );
extern int    pbl_VarBufSize( unsigned char * buffer );

extern pblHashTable_t * pblHtCreate( );
extern int    pblHtInsert  ( pblHashTable_t * h, const void * key, size_t keylen,
                             void * dataptr);
extern void * pblHtLookup  ( pblHashTable_t * h, const void * key, size_t keylen );
extern void * pblHtFirst   ( pblHashTable_t * h );
extern void * pblHtNext    ( pblHashTable_t * h );
extern void * pblHtCurrent ( pblHashTable_t * h );
extern void * pblHtCurrentKey ( pblHashTable_t * h );
extern int    pblHtRemove  ( pblHashTable_t * h, const void * key, size_t keylen );
extern int    pblHtDelete  ( pblHashTable_t * h );

/*
 * FUNCTIONS ON KEY FILES
 */
 int            pblKfInit  ( int nblocks );
extern pblKeyFile_t * pblKfCreate( char * path, void * filesettag );
extern pblKeyFile_t * pblKfOpen  ( char * path, int update, void * filesettag );
extern int            pblKfClose ( pblKeyFile_t * k );
extern int            pblKfFlush ( pblKeyFile_t * k );
extern int            pblKfStartTransaction( pblKeyFile_t * k );
extern int            pblKfCommit( pblKeyFile_t * k, int rollback );
extern int            pblKfSavePosition( pblKeyFile_t * k );
extern int            pblKfRestorePosition( pblKeyFile_t * k );

extern void pblKfSetCompareFunction(
pblKeyFile_t * k,             /** key file to set compare function for  */
int ( *keycompare )           /** compare function to set               */
    (
                void* left,   /** "left" buffer for compare             */                      size_t llen,  /** length of that buffer                 */                      void* right,  /** "right" buffer for compare            */                      size_t rlen   /** length of that buffer                 */
    )                
);

/*
 * WRITE FUNCTIONS ON RECORDS, DELETE AND UPDATE WORK ON CURRENT RECORD
 */
extern int pblKfInsert(
pblKeyFile_t   * k,
unsigned char  * key,
int              keylen,
unsigned char  * data,
long             datalen
);

extern int pblKfDelete( pblKeyFile_t * k );

extern int pblKfUpdate(
pblKeyFile_t   * k,
unsigned char  * data,
long             datalen
);

/*
 * KEY FILE READ FUNCTIONS ON RECORDS
 */
extern long pblKfFind(
pblKeyFile_t   * k,
int              mode,
unsigned char  * skey,
int              skeylen,
unsigned char  * okey,
int            * okeylen
);

extern long pblKfRead(
pblKeyFile_t  * k,
unsigned char * data,
long            datalen
);

/*
 * FUNCTIONS ACTUALLY ONLY TO BE USED THROUGH THE MAKROS DEFINED BELOW
 *
 * however, the functions work, but they are not very fast
 *
 * pblKfGetRel - positions relative to the current record to any other
 *               record of the file, interface is like pblKfNext
 *
 * pblKfGetAbs - positions absolute to the absindex 'th record of the file,
 *               -1L means last, interface is like pblKfFirst
 */
extern long pblKfGetRel( pblKeyFile_t * k, long relindex,
                         char *okey, int *okeylen);
extern long pblKfGetAbs( pblKeyFile_t * k, long absindex,
                         char *okey, int *okeylen);

/*
 * FUNCTIONS ON ISAM FILES
 */
extern int pblIsamClose( pblIsamFile_t * isamfile );
extern int pblIsamFlush( pblIsamFile_t * isamfile );
extern int pblIsamDelete( pblIsamFile_t * isamfile );

extern pblIsamFile_t * pblIsamOpen(
char        * path,
int           update,
void        * filesettag,
int           nkeys,
char       ** keyfilenames,
int         * keydup
);

extern int pblIsamInsert(
pblIsamFile_t * isamfile,
unsigned char * allkeys,
int             allkeyslen,
unsigned char * data,
long            datalen
);

extern int pblIsamFind(
pblIsamFile_t  * isamfile,
int              mode,
int              index,
unsigned char  * skey,
int              skeylen,
unsigned char  * okey
);

extern int pblIsamGet(
pblIsamFile_t  * isamfile,
int              which,
int              index,
unsigned char  * okey
);

extern int pblIsamReadKey(
pblIsamFile_t  * isamfile,
int              index,
unsigned char  * okey
);

extern long pblIsamReadDatalen( pblIsamFile_t * isamfile );

extern long pblIsamReadData(
pblIsamFile_t * isamfile,
unsigned char * buffer,
long            bufferlen
);

extern long pblIsamUpdateData(
pblIsamFile_t * isamfile,
unsigned char * data,
long            datalen
);

extern int pblIsamUpdateKey(
pblIsamFile_t  * isamfile,
int              index,
unsigned char  * ukey,
int              ukeylen
);

extern int pblIsamStartTransaction( int nfiles, pblIsamFile_t ** isamfiles );
extern int pblIsamCommit( int nfiles, pblIsamFile_t ** isamfiles, int rollback);

#ifdef __cplusplus
}
#endif

#endif
