/*
 pbliftst.c - interactive ISAM file test frame

 Copyright (C) 2002    Peter Graf

   This file is part of PBL - The Program Base Library.
   PBL is free software.

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

   For more information on the Program Base Library or Peter Graf,
   please see: http://mission.base.com/.

    $Log$
    Revision 1.1  2004/06/24 21:11:54  sears
    Initial revision

    Revision 1.1  2003/12/11 09:10:48  jim
    pbl

    Revision 1.2  2002/09/12 20:57:15  peter
    fixed a documentation bug

    Revision 1.1  2002/09/12 20:47:05  peter
    Initial revision


------------------------------------------------------------------------------
*/

/* 
 * make sure "strings <exe> | grep Id | sort -u" shows the source file versions
 */
static char * rcsid = "$Id$";
static int    rcsid_fkt() { return( rcsid ? 0 : rcsid_fkt() ); }

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>

#include "pbl.h"

#define PBL_ISAMTEST_BUFLEN              2048

static FILE * logfile;
static FILE * infile;

static void pblsay();

static void putChar( int c )
{
   static int last = 0;

   if( last == '\n' && c == '\n' )
   {
       return;
   }

   last = c;
   putc( last, logfile );
}

static int getChar( void )
{
    int c;
    c = getc( infile );

    /*
     * a '#' starts a comment for the rest of the line
     */
    if( c == '#')
    {
        /*
         * comments starting with ## are duplicated to the output
         */
        c = getc( infile );
        if( c == '#' )
        {
            putChar( '#' );
            putChar( '#' );

            while( c != '\n' && c != EOF )
            {   
                c = getc( infile );
                if( c != EOF )
                {
                    putChar( c );
                }
            }
        }
        else
        {
            while( c != '\n' && c != EOF )
            {
                c = getc( infile );
            }
        }
    }

    if( c != EOF )
    {
        putChar( c );
    }

    return( c );
}

static void getWord( char * buffer )
{
    int c;
    int i;

    /*
     * skip preceeding blanks
     */
    c = ' ';
    while( c == '\t' || c == ' ' || c == '\n' || c == '\r' )
    {
        c = getChar();
    }

    /*
     * read one word
     */
    for( i = 0; i < PBL_ISAMTEST_BUFLEN - 1; i++, c = getChar() )
    {
        if( c == EOF )
        {
            exit( 0 );
        }


        if( c == '\r' )
        {
            continue;
        }

        if( c == '\t' || c == ' ' || c == '\n' || c == '\r' )
        {
            *buffer = '\0';
            return;
        }

        *buffer++ = c;
    }

    *buffer = '\0';
}

static int commastrlen( char * s )
{
    char *ptr = s;

    while( ptr && *ptr && *ptr != ',' )
    {
        ptr++;
    }

    return( ptr - s );
}

/*
 * if given a string of the form ",key1,key2,key3" etc,
 * replaces the "," commas with the length of the following word
 */
static void commatolength( char * s )
{
    while( s && *s )
    {
        if( *s == ',' )
        {
            *s = 0xff & commastrlen( s + 1 );
        }

        s++;
    }
}
    
/**
 * test frame for the ISAM file library
 *
 * This test frame calls the PBL ISAM file library,
 * it is an interactive test frame capable of regression tests.
 *
 * <B>Interactive mode:</B>
 * <UL>
 * Call the program pbliftst from a UNIX or DOS shell.
 * <BR>
 * The following commands to test the PBL ISAM File Library are supplied:
 * <UL>
 * <PRE>
 q       FOR QUIT
 open filename keyfile1,dupfile2,... update
 transaction < START | COMMIT | ROLLBACK >
 close
 flush
 insert ,key1,key2... data
 ninsert n key1,key2,... data
 find index key < LT | LE | FI | EQ | LA | GE | GT >
 nfind n index key < LT | LE | FI | EQ | LA | GE | GT >
 get index < NEXT | PREV | FIRST | LAST | THIS >
 datalen
 readdata
 readkey index
 updatedata data
 updatekey index key
 ndelete n
 </PRE>
 * </UL>
 * See \Ref{pblKEYFILE_TestFrame} for an example to interactively use the
 * test frame.
 * </UL>
 * <B>Regression mode:</B>
 * <UL>
 * Five regression test cases are supplied with the PBL ISAM library.
 *
 * ISAM0001.TST, ISAM0002.TST, ISAM0003.TST, ISAM0004.TST and ISAM0005.TST.
 * 
 * ISAM0001.TST and ISAM0004.TST are run when the "make test" 
 * is done. Do the following if you want to run the test cases per hand
 * <PRE>
   1. Build the pbliftst executable.          make all
   2. Create the sub directory isamtest.      mkdir isamtest
   3. Clear the sub directory isamtest.       rm imamtest/0*
   4. Run the test frame on this file.        pbliftst ISAM0001.TST
   5. Compare ISAM0001.TST and pbliftst.log   diff ISAM0001.TST pbliftst.log
 </PRE>
 * There should be no differences reported, if so your build of the
 * PBL library is most likely ok!
 *
 * </UL>
 */

int pblISAMFILE_TestFrame( int argc, char * argv[] )
{
    char     command  [ PBL_ISAMTEST_BUFLEN ];
    char     filename [ PBL_ISAMTEST_BUFLEN ];
    char     buffer   [ PBL_ISAMTEST_BUFLEN ];
    int      update;
    int      ival;
    int      dowork = 1;
    long     rc = 0;
    int      len;
    char   * ptr;

    pblIsamFile_t * isam = ( pblIsamFile_t *) 0;

    /*
     * if an argument is given it is treated as a command file
     */
    infile = stdin;
    if( argc > 1 )
    {
        infile = fopen( argv[ 1 ], "r" );
        if( !infile )
        {
            fprintf( stderr, "Failed to open %s, %s\n",
                     argv[ 1 ], strerror( errno ));
            exit( -1 );
        }
    }

    /*
     * open the log file
     */
    logfile = fopen( "./pbliftst.log", "wb" );
    if( !logfile )
    {
        fprintf( stderr, "cant open logfile, ./pbliftst.log, %s\n",
                 strerror( errno ));
        exit( 1 );
    }

    /*
     * main command loop
     */
    while( dowork )
    {
        memset( command, 0, sizeof( command ));
        memset( filename, 0, sizeof( filename ));
        memset( buffer, 0, sizeof( buffer ));

        errno = 0;

        /*
         * read the next command
         */
        printf( "\n##command: \n" );
        getWord( command );

        /*
         * interpret the command given
         */
        if( command[0] == 'q' || command[0] == 'Q' )
        {
            dowork = 0;
        }

        else if( !strcmp( command, "open" ))
        {
            int    k;
            int    nkeys;
            char * keyfilenames[ PBL_ISAMTEST_BUFLEN ];
            int    keydup[ PBL_ISAMTEST_BUFLEN ];

            printf( "# open filename keyfile1,dkeyfile2,... update\n" );
            getWord( filename );

            getWord( buffer );
            ptr = buffer;

            for( nkeys = 0; nkeys < PBL_ISAMTEST_BUFLEN; nkeys++ )
            {
                keyfilenames[ nkeys ] = ptr;

                ptr = strchr( ptr, ',' );
                if( !ptr )
                {
                    break;
                }

                *ptr = 0;
                ptr++;
            }

            for( k = 0; k < nkeys; k++ )
            {
                if( strstr( keyfilenames[ k ], "dup" ))
                {
                    keydup[ k ] = 1;
                }
                else
                {
                    keydup[ k ] = 0;
                }
            }

            getWord( command );
            update = atoi( command );

            pblsay( "# pblIsamOpen( %s, %d, %d )\n",
                    filename, nkeys + 1, update );

            if( isam )
            {
                pblIsamClose( isam );
            }

            isam = pblIsamOpen( filename, update, NULL, nkeys + 1,
                                keyfilenames, keydup );
            if( isam )
            {
                pblsay( "# ok!\n" );
            }
            else
            {
                pblsay( "# not ok! pbl_errno %d, errno %d\n", pbl_errno, errno );
            }
        }

        else if( !strcmp( command, "transaction" ))
        {
            printf( "# transaction < START | COMMIT | ROLLBACK >\n" );
            if( !isam )
            {
                continue;
            }

            getWord( buffer );

            if( !strncmp( buffer, "ST", 2 ))
            {
                pblsay( "# pblIsamStartTransaction( )\n" );
                rc = pblIsamStartTransaction( 1, &isam );
                
            }
            else if( !strncmp( buffer, "CO", 2 ))
            {
                pblsay( "# pblIsamCommit( COMMIT )\n" );
                rc = pblIsamCommit( 1, &isam, 0 );
            }
            else if( !strncmp( buffer, "RO", 2 ))
            {
                pblsay( "# pblIsamCommit( ROLLBACK )\n" );
                rc = pblIsamCommit( 1, &isam, 1 );
            }
            else
            {
                rc = 0;
            }

            if( !rc )
            {
                pblsay( "# rc 0\n" );
                continue;
            }
            pblsay( "# rc %ld, pbl_errno %d, errno %d\n", rc, pbl_errno, errno );
        }

        else if( !strcmp( command, "close" ))
        {
            printf( "# close\n" );
            if( !isam )
            {
                continue;
            }

            pblsay( "# pblIsamClose( %d )\n", 1 );

            rc = pblIsamClose( isam );

            isam = 0;

            if( !rc )
            {
                pblsay( "# rc 0\n" );
                continue;
            }
            pblsay( "# rc %ld, pbl_errno %d, errno %d\n", rc, pbl_errno, errno );

        }

        else if( !strcmp( command, "flush" ))
        {
            printf( "# flush\n" );
            if( !isam )
            {
                continue;
            }

            pblsay( "# pblIsamFlush( %d )\n", 1 );

            rc = pblIsamFlush( isam );
            if( !rc )
            {
                pblsay( "# rc 0\n" );
                continue;
            }

            pblsay( "# rc %ld, pbl_errno %d, errno %d\n", rc, pbl_errno, errno );
        }

        else if( !strcmp( command, "readdata" ))
        {
            printf( "# readdata\n" );
            if( !isam )
            {
                continue;
            }

            pblsay( "# pblIsamReadData( currentrecord )\n" );

            rc = pblIsamReadData( isam, buffer, sizeof( buffer ) );
            if( rc < 0 )
            {
                pblsay( "# rd %ld, pbl_errno %d, errno %d\n",
                        rc, pbl_errno, errno );
            }
            else
            {
                pblsay( "# datalen %ld, data %s\n", rc, buffer );
            }
        }

        else if( !strcmp( command, "readkey" ))
        {
            int index;

            printf( "# readkey index\n" );
            if( !isam )
            {
                continue;
            }

            getWord( buffer );
            index = atoi( buffer );

            pblsay( "# pblIsamReadKey( currentrecord, %d )\n", index );

            rc = pblIsamReadKey( isam, index, buffer );
            if( rc < 0 )
            {
                pblsay( "# rc %ld, pbl_errno %d, errno %d\n",
                        rc, pbl_errno, errno );
            }
            else
            {
                pblsay( "# keylen %ld, key %s\n", rc, buffer );
            }
        }

        else if( !strcmp( command, "updatedata" ))
        {
            printf( "# updatedata data\n" );
            if( !isam )
            {
                continue;
            }

            getWord( buffer );
            ival = strlen( buffer ) + 1;

            pblsay( "# pblIsamUpdateData( %s, %d )\n", buffer, ival );

            rc = pblIsamUpdateData( isam, buffer, ival );
            if( rc < 0 )
            {
                pblsay( "# rc %ld, pbl_errno %d, errno %d\n",
                        rc, pbl_errno, errno );
            }
            else
            {
                pblsay( "# datalen %ld\n", rc );
            }
        }

        else if( !strcmp( command, "updatekey" ))
        {
            int index;

            printf( "# updatekey index key\n" );
            if( !isam )
            {
                continue;
            }

            getWord( buffer );
            index = atoi( buffer );

            getWord( buffer );
            ival = strlen( buffer );

            pblsay( "# pblIsamUpdateKey( %d, %s, %d )\n", index, buffer, ival );

            rc = pblIsamUpdateKey( isam, index, buffer, ival );
            if( rc < 0 )
            {
                pblsay( "# rc %ld, pbl_errno %d, errno %d\n",
                        rc, pbl_errno, errno );
            }
            else
            {
                pblsay( "# rc %ld\n", rc );
            }
        }

        else if( !strcmp( command, "ndelete" ))
        {
            int n;
            int i;

            printf( "# ndelete n\n" );
            if( !isam )
            {
                continue;
            }

            getWord( buffer );
            n = atoi( buffer );

            pblsay( "# pblIsamDelete( %d records )\n", n );

            for( i = 0; i < n; i++ )
            {
                rc = pblIsamGet( isam, PBLTHIS, 0, buffer );
                if( rc < 0 )
                {
                    pblsay( "# i %d, rc %ld, pbl_errno %d, errno %d\n",
                             i, rc, pbl_errno, errno );
                    break;
                }
 
                rc = pblIsamDelete( isam );
                if( rc < 0 )
                {
                    pblsay( "# i %d, rc %ld, pbl_errno %d, errno %d\n",
                             i, rc, pbl_errno, errno );
                    break;
                }
            }
            if( rc >= 0 )
            {
                pblsay( "# deleted %d records, rc %ld\n", i, rc );
            }
        }

        else if( !strcmp( command, "insert" ))
        {
            printf( "# insert ,key1,key2... data\n" );
            if( !isam )
            {
                continue;
            }

            getWord( buffer );
            getWord( filename );
            ival = strlen( buffer );
            len = strlen( filename ) + 1;

            pblsay( "# pblIsamInsert( %d, %s, %d, %s, %d )\n",
                     1, buffer, ival, filename, len );

            commatolength( buffer );

            rc = pblIsamInsert( isam, buffer, ival, filename, len );
            if( !rc )
            {
                pblsay( "# rc 0\n" );
                continue;
            }

            pblsay( "# rc %ld, pbl_errno %d, errno %d\n",
                    rc, pbl_errno, errno );
        }

        else if( !strcmp( command, "ninsert" ))
        {
            int    nkeys = 3;
            char * keys[ PBL_ISAMTEST_BUFLEN ];
            int    n;
            int    i;
            int    k;

            printf( "# ninsert n key1,key2,... data\n" );
            if( !isam )
            {
                continue;
            }

            getWord( buffer );
            n = atoi( buffer );

            getWord( buffer );
            getWord( filename );
            len = strlen( filename ) + 1;

            ptr = buffer;

            for( nkeys = 0; nkeys < PBL_ISAMTEST_BUFLEN; nkeys++ )
            {
                keys[ nkeys ] = ptr;

                ptr = strchr( ptr, ',' );
                if( !ptr )
                {
                    break;
                }

                *ptr = 0;
                ptr++;
            }

            for( i = 0; i < n; i++ )
            {
                ival = 0;
                command[ 0 ] = 0;
                for( k = 0; k <= nkeys; k++ )
                {
                    snprintf( command + ival, sizeof( command ) - ival,
                              ",%s%d", keys[ k ], i );

                    ival += strlen( command + ival );
                }
                
                if( i == 0 )
                {
                    ival = strlen( command );
                    pblsay( "# pblIsamInsert( %d, %s, %d, %s, %d )\n",
                             1, command, ival, filename, len );
                }

                commatolength( command );

                rc = pblIsamInsert( isam, command, ival, filename, len );
                if( rc < 0 )
                {
                    pblsay( "# i %d, rc %ld, pbl_errno %d, errno %d\n",
                            i, rc, pbl_errno, errno );
                    break;
                }
            }

            if( rc >= 0 )
            {
                pblsay( "# inserted %d records, rc %ld\n", i, rc );
            }
        }

        else if( !strcmp( command, "datalen" ))
        {
            printf( "# datalen\n" );
            if( !isam )
            {
                continue;
            }

            pblsay( "# pblIsamReadDatalen( currentrecord )\n" );

            rc = pblIsamReadDatalen( isam );
            if( rc < 0 )
            {
                pblsay( "# rc %ld, pbl_errno %d, errno %d\n",
                        rc, pbl_errno, errno );
            }
            else
            {
                pblsay( "# datalen %ld\n", rc );
            }
        }

        else if( !strcmp( command, "get" ))
        {
            int index;

            printf( "# get index < NEXT | PREV | FIRST | LAST | THIS >\n" );
            if( !isam )
            {
                continue;
            }


            getWord( buffer );
            index = atoi( buffer );

            getWord( filename );
            if( !strncmp( filename, "NE", 2 ))
            {
                ival = PBLNEXT;
            }
            else if( !strncmp( filename, "PR", 2 ))
            {
                ival = PBLPREV;
            }
            else if( !strncmp( filename, "FI", 2 ))
            {
                ival = PBLFIRST;
            }
            else if( !strncmp( filename, "LA", 2 ))
            {
                ival = PBLLAST;
            }
            else
            {
                ival = PBLTHIS;
            }

            pblsay( "# pblIsamGet( %d, %d )\n", ival, index );
            
            rc = pblIsamGet( isam, ival, index, buffer );
            if( rc < 0 )
            {
                pblsay( "# rc %ld, pbl_errno %d, errno %d\n",
                        rc, pbl_errno, errno );
            }
            else
            {
                pblsay( "# keylen %ld, key %.*s\n", rc, rc, buffer );
            }
        }

        else if( !strcmp( command, "find" ))
        {
            int index = 0;

            printf( "# find index key < LT | LE | FI | EQ | LA | GE | GT >\n" );
            if( !isam )
            {
                continue;
            }

            getWord( buffer );
            index = atoi( buffer );

            getWord( buffer );
            getWord( filename );

            if( !strncmp( filename, "GT", 2 ))
            {
                ival = PBLGT;
            }
            else if( !strncmp( filename, "FI", 2 ))
            {
                ival = PBLFI;
            }
            else if( !strncmp( filename, "LA", 2 ))
            {
                ival = PBLLA;
            }
            else if( !strncmp( filename, "GE", 2 ))
            {
                ival = PBLGE;
            }
            else if( !strncmp( filename, "LE", 2 ))
            {
                ival = PBLLE;
            }
            else if( !strncmp( filename, "LT", 2 ))
            {
                ival = PBLLT;
            }
            else
            {
                strcpy( filename, "EQ" );
                ival = PBLEQ;
            }
         
            len = strlen( buffer );
            pblsay( "# pblIsamFind( %s, %s, %d )\n",
                    filename, buffer, len );

            rc = pblIsamFind( isam, ival, index, buffer, len, filename );

            if( rc < 0 )
            {
                pblsay( "# rc %ld, pbl_errno %d, errno %d\n",
                        rc, pbl_errno, errno );
            }
            else
            {
                pblsay( "# keylen %ld, key %s\n", rc, filename );
            }
        }


        else if( !strcmp( command, "nfind" ))
        {
            int    index;
            int    n;
            int    i;

            printf( "# nfind n index key "
                    "< LT | LE | FI | EQ | LA | GE | GT >\n" );
            if( !isam )
            {
                continue;
            }

            getWord( buffer );
            n = atoi( buffer );

            getWord( buffer );
            index = atoi( buffer );

            /*
             * read the key to search for
             */
            getWord( buffer );

            getWord( filename );
            if( !strncmp( filename, "GT", 2 ))
            {
                ival = PBLGT;
            }
            else if( !strncmp( filename, "FI", 2 ))
            {
                ival = PBLFI;
            }
            else if( !strncmp( filename, "LA", 2 ))
            {
                ival = PBLLA;
            }
            else if( !strncmp( filename, "GE", 2 ))
            {
                ival = PBLGE;
            }
            else if( !strncmp( filename, "LE", 2 ))
            {
                ival = PBLLE;
            }
            else if( !strncmp( filename, "LT", 2 ))
            {
                ival = PBLLT;
            }
            else
            {
                strcpy( filename, "EQ" );
                ival = PBLEQ;
            }

            for( i = 0; i < n; i++ )
            {
				if( i == 127 )
				{
					len = strlen( command );
				}
                snprintf( command, sizeof( command ),
                          "%s%d", buffer,
                          ((( rand() << 16 ) ^ rand()) & 0x7fffffff ) % n );


                if( i == 0 )
                {
                    len = strlen( buffer );
                    pblsay( "# pblIsamFind( %d, %s, %d, %s, %d )\n",
                             1, filename, index, buffer, len );
                }

                len = strlen( command );

				if( i == 127 )
				{
					len = strlen( command );
				}
                rc = pblIsamFind( isam, ival, index, command, len, filename );

                if(( rc < 0 ) && ( pbl_errno != PBL_ERROR_NOT_FOUND ))
                {
                    pblsay( "# i %d, %s, %d, rc %ld, pbl_errno %d, errno %d\n",
                            i, command, len, rc, pbl_errno, errno );
                    break;
                }
            }

            if( i >= n )
            {
                pblsay( "# found %d records, rc %ld\n", i, 0 );
            }
        }

        else
        {
            printf( "# q       FOR QUIT\n" );
            printf( "# open filename keyfile1,dkeyfile2,... update\n" );
            printf( "# transaction < START | COMMIT | ROLLBACK >\n" );
            printf( "# close\n" );
            printf( "# flush\n" );
            printf( "# insert ,key1,key2... data\n" );
            printf( "# ninsert n key1,key2,... data\n" );
            printf( "# find index key < LT | LE | FI | EQ | LA | GE | GT >\n" );
            printf( "# nfind n index key "
                    "< LT | LE | FI | EQ | LA | GE | GT >\n" );
            printf( "# get index < NEXT | PREV | FIRST | LAST | THIS >\n" );
            printf( "# datalen\n" );
            printf( "# readdata\n" );
            printf( "# readkey index\n" );
            printf( "# updatedata data\n" );
            printf( "# updatekey index key\n" );
            printf( "# ndelete n\n" );
        }
    }

#ifdef PBL_MEMTRACE

    pbl_memtrace_out( 0 );

#endif

    return( 0 );
}

int main( int argc, char * argv[] )
{
    return( pblISAMFILE_TestFrame( argc, argv ));
}

static void pblsay(
char *p1,  char *p2,  char *p3,  char *p4,  char *p5,
char *p6,  char *p7,  char *p8,  char *p9,  char *p10,
char *p11, char *p12, char *p13, char *p14, char *p15
)
{
    fprintf( stdout, p1, p2, p3, p4, p5, p6, p7, p8,
             p9, p10, p11, p12, p13, p14, p15 );
    fprintf( logfile, p1, p2, p3, p4, p5, p6, p7, p8,
             p9, p10, p11, p12, p13, p14, p15 );
    //fflush( logfile );
    //fflush( stdout );
}

