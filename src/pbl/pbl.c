/*
 pbl.c - basic library functions

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
   Revision 1.1  2004/06/24 21:11:34  sears
   Initial revision

   Revision 1.3  2004/05/26 09:55:31  sears
   Misc bugfixes / optimizations.

   Revision 1.2  2003/12/11 09:21:20  jim
   update includes

   Revision 1.1  2003/12/11 09:10:48  jim
   pbl

   Revision 1.2  2002/09/12 20:47:13  peter
   added the isam file handling to the library


*/
/*
 * make sure "strings <exe> | grep Id | sort -u" shows the source file versions
 */
static char* _PBL_id = "$Id$";
static int   _PBL_fct() { return( _PBL_id ? 0 : _PBL_fct() ); }

#include <stdio.h>
#include <string.h>
#include <memory.h>
#include <stdlib.h>
#include <time.h>

#include <pbl/pbl.h>

/*****************************************************************************/
/* #defines                                                                  */
/*****************************************************************************/

/*****************************************************************************/
/* typedefs                                                                  */
/*****************************************************************************/

/*
 * the type is only needed if we keep a heap memory trace
 */
typedef struct pbl_memtrace_s
{
    char *      tag;          /* tag used by calling function                */
    time_t      time;         /* time when the chunk of memory was requested */
    void *      data;         /* pointer to data that was allocated          */
    size_t      size;         /* number of bytes allocated                   */

    struct pbl_memtrace_s * next;    /* memory chunks are kept in a linear   */
    struct pbl_memtrace_s * prev;    /* list                                 */

} pbl_memtrace_t;

/*****************************************************************************/
/* globals                                                                   */
/*****************************************************************************/

#ifdef PBL_MEMTRACE

/*
 * head and tail of known memory chunks
 */
static pbl_memtrace_t * pbl_memtrace_head;
static pbl_memtrace_t * pbl_memtrace_tail;

/*
 * number of memory chunks known
 */
static long pbl_nmem_chunks = 0;

/*
 * total size of all chunks known
 */
static long pbl_nmem_size   = 0;

#endif

static char pbl_errbuf[ PBL_ERRSTR_LEN + 1 ];

int    pbl_errno;
char * pbl_errstr = pbl_errbuf;

/*****************************************************************************/
/* functions                                                                 */
/*****************************************************************************/

#ifdef PBL_MEMTRACE

/*
 * log a line for all memory chunks that are allocated for more than
 * 3 minutes, or if call at the end of the program, log all chunks known
 */
void pbl_memtrace_out( int checktime )
{
    static int first = 1;
    pbl_memtrace_t * memtrace;
    pbl_memtrace_t * tmp;

    char * outpath = "pblmemtrace.log";
    FILE * outfile = NULL;
    time_t now = time( 0 );
    char * nowstr = NULL;
    char * timestr = NULL;

    if( !pbl_memtrace_head )
    {
        return;
    }

    memtrace = pbl_memtrace_head;
    while( memtrace )
    {
        if( checktime && ( now - memtrace->time < 180 ))
        {
            break;
        }

        if( !outfile )
        {
            if( first )
            {
                first = 0;
                outfile = fopen( outpath, "w" );
                if( outfile )
                {
                    fprintf( outfile, ">>memtrace at %s", ctime( &now ));
                }
            }
            else
            {
                outfile = fopen( outpath, "a" );
            }

            if( !outfile )
            {
                break;
            }
        }

        tmp = memtrace;
        memtrace = memtrace->next;

        if( !nowstr )
        {
            nowstr = strdup( ctime( &now ));
        }
        timestr = ctime( &(tmp->time));

        fprintf( outfile, "%s %.*s: %.*s %06ld %x %ld %ld \"%s\"\n",
                          checktime ? ">" : "e",
                          8, nowstr ? nowstr + 11 : "unknown",
                          8, timestr ? timestr + 11 : "unknown",
                          (long)tmp->size,
                          tmp->data - NULL,
                          pbl_nmem_chunks, pbl_nmem_size, tmp->tag
               );

        PBL_LIST_UNLINK( pbl_memtrace_head, pbl_memtrace_tail,
                         tmp, next, prev );
        free( tmp );
    }

    if( nowstr )
    {
        free( nowstr );
    }

    if( outfile )
    {
        fclose( outfile );
    }
}

/*
 * remember a memory chunk that was allocated by some function
 */
void pbl_memtrace_create( 
char * tag,
void * data,
size_t size
)
{
    pbl_memtrace_t * memtrace;

    memtrace = malloc( sizeof( pbl_memtrace_t ));
    if( !memtrace )
    {
        return;
    }

    memtrace->tag  = tag;
    memtrace->time = time( 0 );
    memtrace->data = data;
    memtrace->size = size;

    PBL_LIST_APPEND( pbl_memtrace_head, pbl_memtrace_tail,
                     memtrace, next, prev );

    pbl_nmem_chunks++;
    pbl_nmem_size += size;

    pbl_memtrace_out( 1 );
}

/*
 * remove a memory from the chunk list, the caller freed the memory
 */
void pbl_memtrace_delete(
void * data
)
{
    pbl_memtrace_t * memtrace;
    
    for( memtrace = pbl_memtrace_head; memtrace; memtrace = memtrace->next )
    {
        if( memtrace->data == data )
        {
            pbl_nmem_chunks--;
            pbl_nmem_size -= memtrace->size;

            PBL_LIST_UNLINK( pbl_memtrace_head, pbl_memtrace_tail,
                             memtrace, next, prev );
            free( memtrace );
            break;
        }
    }
}

#endif /* PBL_MEMTRACE */

/**
  * replacement for malloc
  * 
  * @return  void * retptr == NULL: OUT OF MEMORY
  * @return  void * retptr != NULL: pointer to buffer allocated
  */
void * pbl_malloc(
char   * tag,        /** tag used for memory leak detection */
size_t   size        /** number of bytes to allocate        */
)
{
    void * ptr;

    if( !tag )
    {
        tag = "pbl_malloc";
    }

    ptr = malloc( size );
    if( !ptr )
    {
        snprintf( pbl_errstr, PBL_ERRSTR_LEN,
                  "%s: failed to malloc %d bytes\n", tag, size );
        pbl_errno = PBL_ERROR_OUT_OF_MEMORY;
        return( 0 );
    }

#ifdef PBL_MEMTRACE
    pbl_memtrace_create( tag, ptr, size );
#endif
    
    return( ptr );
}

/**
  * replacement for malloc, initializes the memory to 0
  * 
  * @return  void * retptr == NULL: OUT OF MEMORY
  * @return  void * retptr != NULL: pointer to buffer allocated
  */
void * pbl_malloc0(
char   * tag,        /** tag used for memory leak detection */
size_t   size        /** number of bytes to allocate        */
)
{
    void * ptr = malloc( size );
    if( !ptr )
    {
        snprintf( pbl_errstr, PBL_ERRSTR_LEN,
                  "failed to malloc %d bytes\n", size );
        pbl_errno = PBL_ERROR_OUT_OF_MEMORY;
        return( 0 );
    }

    memset( ptr, 0, size );

#ifdef PBL_MEMTRACE
    assert(0);
    pbl_memtrace_create( tag, ptr, size );
#endif
    
    return( ptr );
}


/**
  * duplicate a buffer, similar to strdup
  *
  * @return  void * retptr == NULL: OUT OF MEMORY
  * @return  void * retptr != NULL: pointer to buffer allocated
  */
void * pbl_memdup(
char * tag,        /** tag used for memory leak detection */
void * data,       /** buffer to duplicate                */
size_t size        /** size of that buffer                */
)
{
    void * ptr = malloc( size );
    if( !ptr )
    {
        snprintf( pbl_errstr, PBL_ERRSTR_LEN,
                  "failed to malloc %d bytes\n", size );
        pbl_errno = PBL_ERROR_OUT_OF_MEMORY;
        return( 0 );
    }

    memcpy( ptr, data, size );

#ifdef PBL_MEMTRACE
    pbl_memtrace_create( tag, ptr, size );
#endif
    
    return( ptr );
}

/**
  * duplicate and concatenate two memory buffers
  *
  * @return  void * retptr == NULL: OUT OF MEMORY
  * @return  void * retptr != NULL: pointer to new buffer allocated
  */

void * pbl_mem2dup(
char * tag,        /** tag used for memory leak detection */
void * mem1,       /** first buffer to duplicate          */
size_t len1,       /** length of first buffer             */
void * mem2,       /** second buffer to duplicate         */
size_t len2        /** length of second buffer            */
)
{
    void * ret;

    if( !tag )
    {
        tag = "pbl_mem2dup";
    }

    ret = pbl_malloc( tag, len1 + len2 );
    if( !ret )
    {
        return( 0 );
    }

    if( len1 )
    {
        memcpy( ret, mem1, len1 );
    }

    if( len2 )
    {
        memcpy( ((char*)ret) + len1, mem2, len2 );
    }

    return( ret );
}

/**
 * memcpy with target length check
 *
 * @return   size_t rc: number of bytes copied
 */
size_t pbl_memlcpy(
void * to,          /** target buffer to copy to                             */
size_t tolen,       /** number of bytes in the target buffer                 */
void * from,        /** source to copy from                                  */
size_t n            /** length of source                                     */
)
{
    size_t l = n > tolen ? tolen : n;

    memcpy( to, from, l );
    return( l );
}

/**
 * find out how many starting bytes of two buffers are equal
 *  
 * @return   int rc: number of equal bytes
 */ 
        
int pbl_memcmplen(
void * left,    /** first buffer for compare               */
size_t llen,    /** length of that buffer                  */
void * right,   /** second buffer for compare              */
size_t rlen     /** length of that buffer                  */
)       
{           
    unsigned int i;
    unsigned char * l = ( unsigned char * )left;
    unsigned char * r = ( unsigned char * )right;

    if( llen > rlen )
    {
        llen = rlen;
    }

    for( i = 0; i < llen; i++ )
    {
        if( *l++ != *r++ )
        {
            break;
        }
    }

    return( i );
}

/**
 * compare two memory buffers, similar to memcmp
 *
 * @return   int rc  < 0: left is smaller than right
 * @return   int rc == 0: left and right are equal
 * @return   int rc  > 0: left is bigger than right
 */

int pbl_memcmp(
void * left,    /** first buffer for compare               */
size_t llen,    /** length of that buffer                  */
void * right,   /** second buffer for compare              */
size_t rlen     /** length of that buffer                  */
)
{
    size_t len;
    int    rc;

    /*
     * a buffer with a length 0 is logically smaller than any other buffer
     */
    if( !llen )
    {
        if( !rlen )
        {
            return( 0 );
        }
        return( -1 );
    }
    if( !rlen )
    {
        return( 1 );
    }

    /*
     * use the shorter of the two buffer lengths for the memcmp
     */
    if( llen <= rlen )
    {
        len = llen;
    }
    else
    {
        len = rlen;
    }

    /*
     * memcmp is used, therefore the ordering is ascii
     */
    rc = memcmp( left, right, len );
    if( rc )
    {
        return( rc );
    }

    /*
     * if the two buffers are equal in the first len bytes, but don't have
     * the same lengths, the longer one is logically bigger
     */
    return( (int) ( ((int)llen) - ((int)rlen) ));
}

/**
 * copy a two byte short to a two byte buffer
 */
void pbl_ShortToBuf(
unsigned char * buf,        /** buffer to copy to                 */
int s                       /** short value to copy               */
)
{
    *buf++ = ( unsigned char ) ( s >> 8 );
    *buf   = ( unsigned char ) ( s );
}

/**
 * read a two byte short from a two byte buffer
 *
 * @return int rc: the short value read
 */
int pbl_BufToShort(
unsigned char * buf            /** buffer to read from      */
)
{
    unsigned int s  = (( unsigned int ) ( *buf++ )) << 8;

    s |= *buf;
    return( s );
}

/**
 * copy a four byte long to a four byte buffer
 */
void pbl_LongToBuf(
unsigned char * buf,        /** buffer to copy to                 */
long l                      /** long value to copy                */
)
{

    *buf++ = (unsigned char ) ( ( l >> 24 ));
    *buf++ = (unsigned char ) ( ( l >> 16 ));
    *buf++ = (unsigned char ) ( ( l >>  8 ));
    *buf   = (unsigned char ) ( l );
}

/**
 * read a four byte long from a four byte buffer
 *
 * @return long ret: the long value read
 */
long pbl_BufToLong(
unsigned char * buf        /** the buffer to read from   */
)
{
    unsigned long l  = ((( unsigned long ) ( *buf++ ) )) << 24;

    l |= ((( unsigned long ) ( *buf++ ) ) ) << 16;
    l |= ((( unsigned long ) ( *buf++ ) ) ) <<  8;
    l |= *buf;

    return( l );
}

/**
 * copy a four byte long to a variable length buffer
 *
 * @return int rc: the number of bytes used in the buffer
 */
int pbl_LongToVarBuf( unsigned char * buffer, unsigned long value )
{
    if( value <= 0x7f )
    {
        *buffer = (unsigned char)value;
        return( 1 );
    }

    if( value <= 0x3fff )
    {
        *buffer++ = (unsigned char)( value / 0x100 ) | 0x80;
        *buffer = (unsigned char)value & 0xff;
        return( 2 );
    }

    if( value <= 0x1fffff )
    {
        *buffer++ = (unsigned char)( value / 0x10000 ) | 0x80 | 0x40;
        *buffer++ = (unsigned char)( value / 0x100 );
        *buffer = (unsigned char)value & 0xff;
        return( 3 );
    }

    if( value <= 0x0fffffff )
    {
        *buffer++ = (unsigned char)( value / 0x1000000 ) | 0x80 | 0x40 | 0x20;
        *buffer++ = (unsigned char)( value / 0x10000 );
        *buffer++ = (unsigned char)( value / 0x100 );
        *buffer = (unsigned char)value & 0xff;
        return( 4 );
    }

    *buffer++ = (unsigned char)0xf0;
    pbl_LongToBuf( buffer, value );

    return( 5 );
}


/**
 * read a four byte long from a variable length buffer
 *
 * @return int rc: the number of bytes used in the buffer
 */
int pbl_VarBufToLong(
unsigned char * buffer,    /** buffer to read from                 */
long * value               /** long to read to                     */
)
{
    int c = 0xff & *buffer++;
    int val;

    if( !( c & 0x80 ))
    {
        *value = c;
        return( 1 );
    }

    if( !( c & 0x40 ))
    {
        *value = ( c & 0x3f ) * 0x100 + ( *buffer & 0xff );
        return( 2 );
    }
    if( !( c & 0x20 ))
    {
        val = ( c & 0x1f ) * 0x10000;
        val += (( *buffer++ ) & 0xff ) * 0x100;
        *value = val + (( *buffer ) & 0xff );
        return( 3 );
    }

    if( !( c & 0x10 ))
    {
        val = ( c & 0x0f ) * 0x1000000;
        val += (( *buffer++ ) & 0xff ) * 0x10000;
        val += (( *buffer++ ) & 0xff ) * 0x100;
        *value = val + (( *buffer ) & 0xff );
        return( 4 );
    }

    *value = pbl_BufToLong( buffer );
    return( 5 );
}

/**
 * find out how many bytes a four byte long would use in a buffer
 *
 * @return int rc: number of bytes used in buffer
 */

int pbl_LongSize(
unsigned long value               /** value to check          */
)
{
    if( value <= 0x7f )
    {
        return( 1 );
    }

    if( value <= 0x3fff )
    {
        return( 2 );
    }

    if( value <= 0x1fffff )
    {
        return( 3 );
    }

    if( value <= 0x0fffffff )
    {
        return( 4 );
    }

    return( 5 );
}

/**
 * find out how many bytes a four byte long uses in a buffer
 *
 * @return int rc: number of bytes used in buffer
 */

int pbl_VarBufSize(
unsigned char * buffer   /** buffer to check                  */
)
{
    int c = 0xff & *buffer;

    if( !( c & 0x80 ))
    {
        return( 1 );
    }

    if( !( c & 0x40 ))
    {
        return( 2 );
    }

    if( !( c & 0x20 ))
    {
        return( 3 );
    }

    if( !( c & 0x10 ))
    {
        return( 4 );
    }

    return( 5 );
}

