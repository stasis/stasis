/*
 pblkftst.c - interactive PBL key file test frame

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
    Revision 1.1  2004/06/24 21:12:09  sears
    Initial revision

    Revision 1.1  2003/12/11 09:10:49  jim
    pbl

    Revision 1.1  2002/09/12 20:47:08  peter
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

static FILE * log;
static FILE * infile;

static void putChar( int c )
{
   static int last = 0;

   if( last == '\n' && c == '\n' )
       return;

   last = c;
   ( void ) putc( last, log );
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
       while( c != '\n' && c != EOF )
       {
           c = getc( infile );
       }
    }

    if( c != EOF )
        putChar( c );

    return( c );
}

    
static void getWord( char * buffer )
{
    int c;

    /*
     * skip preceeding blanks
     */
    c = ' ';
    while( c== '\t' || c == ' ' || c == '\n' )
        c = getChar();

    /*
     * read one word
     */
    for(;; c = getChar())
    {
        if( c == EOF )
        {
            exit( 0 );
        }

        if( c== '\t' || c == ' ' || c == '\n' )
        {
            *buffer = '\0';
            return;
        }

        *buffer++ = c;
    }
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
 * test frame for the key file library
 *
 * This test frame calls the key file library,
 * it is an interactive test frame capable of regression tests.
 *
 * <B>Interactive mode:</B>
 * <UL>
 * Call the program pblkftst from a UNIX or DOS shell.
 * <BR>
 * The following commands to test the PBL Key File Library are supplied:
 * <UL>
 * <PRE>
 q       FOR QUIT
 create  <filename>
 open    <filename> <update>
 close | flush | delete
 insert  <key> <data>
 ninsert <n> <key> <data>
 find    <key> < LT | LE | FI | EQ | LA | GE | GT >
 update  <data>
 ndelete <n>
 first | last | next | prev | this | read
 list    <n>
 </PRE>
 * </UL>
 * </UL>
 * <B>Interactive mode example:</B>
 * <UL>
 * The following short examples demonstrates how to use the interactive
 * test frame. Lines starting with a # are printed by the program
 * lines starting with other characters are user input.
 * <UL>
 * <PRE>
pblkftst

##command: 
create /tmp/keytest
# create filename 
# pblKfCreate( /tmp/keytest )
# ok!

##command: 
insert testkey testdata
# insert key, data
# pblKfInsert( 1, testkey, 8, testdata, 9 )
# rc 0, pbl_errno 0, errno 0

##command: 
first
# pblKfFirst( 1 )
# datalen 9, key testkey, keylen 8

##command: 
close
# pblKfClose( 1 )
# rc 0, pbl_errno 0, errno 0

##command: 
quit
   </PRE>
 * </UL>
 * </UL>

 * <B>Regression mode:</B>
 * <UL>
 * Put a sequence of test commands into a test batch file.
 * <P>
 * Example:
 * <UL>
 * Open a new file KEY0001.TST and add the following lines
 * <PRE>
create /tmp/keytest2
insert testkey testdata
first
close
quit
</PRE>
</UL>
 * Then run pblkftst KEY0001.TST
 *
 * The program creates a log file called <B>pblkftst.log</B> when run.
 * This log file can be used as regression input file for further
 * tests.
 * <UL>
<PRE>
##command:
create # create filename
/tmp/key0001
# pblKfCreate( /tmp/key0001 )
# ok!

##command:
insert # insert key, data
testkey testdata
# pblKfInsert( 1, testkey, 8, testdata, 9 )
# rc 0, pbl_errno 0, errno 0

##command:
first
# pblKfFirst( 1 )
# datalen 9, key testkey, keylen 8

##command:
close
# pblKfClose( 1 )
# rc 0, pbl_errno 0, errno 0

##command:
quit
</PRE>
</UL>
 * Keep the contents of this file and diff it with the outout of
 * the KEY0001.TST testcase you created whenever you change
 * the code of the library. 
 *
 * See \Ref{pblISAMFILE_TestFrame} for an example of regression
 * tests with a test frame. The regression test cases given with
 * \Ref{pblISAMFILE_TestFrame} of course also test the PBL Key File
 * library.
 * </UL>
 */

int pblKEYFILE_TestFrame( int argc, char * argv[] )
{
    char command[ 2048 ];
    char filename [ 2048 ];
    char buffer [ 2048 ];
    int  update;
    int  ival;
    int  dowork = 1;
    long rc;
    int  i;
    int  len;

    pblKeyFile_t * kf = ( pblKeyFile_t *) 0;

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

    log = fopen( "./pblkftst.log", "w" );
    if( !log )
    {
        fprintf( stderr, "cant open logfile, .\\pblkftst.log\n" );
        exit( 1 );
    }

    while( dowork )
    {
        memset( command, 0, sizeof( command ));
        memset( filename, 0, sizeof( filename ));
        memset( buffer, 0, sizeof( buffer ));

        errno = 0;

        printf( "\n##command: \n" );
        fprintf( log, "\n##command: \n" );

        getWord( command );

        if( command[0] == 'q' || command[0] == 'Q' )
        {
            dowork = 0;
        }
        else if( !strcmp( command, "create" ))
        {
            printf( "# create filename \n" );
            fprintf( log, "# create filename \n" );

            getWord( filename );

            printf( "# pblKfCreate( %s )\n", filename );
            fprintf( log, "# pblKfCreate( %s )\n", filename );

            if( kf )
                ( void ) pblKfClose( kf );

            kf = pblKfCreate( filename, NULL );

            if( kf )
            {
                printf( "# ok!\n" );
                fprintf( log, "# ok!\n" );
            }
            else
            {
                printf( "# pbl_errno %d, errno %d\n", pbl_errno, errno );
                fprintf( log, "# pbl_errno %d, errno %d\n", pbl_errno, errno );
            }
        }

        else if( !strcmp( command, "open" ))
        {
            printf( "# open filename, update \n" );
            fprintf( log, "# open filename, update \n" );
            getWord( filename );
            getWord( buffer );
            update = atoi( buffer );

            printf( "# pblKfOpen( %s, %d )\n", filename, update );
            fprintf( log, "# pblKfOpen( %s, %d )\n", filename, update );

            if( kf )
                ( void ) pblKfClose( kf );

            kf = pblKfOpen( filename, update, NULL );
            if( kf )
            {
                printf( "# ok!\n" );
                fprintf( log, "# ok!\n" );
            }
            else
            {
                printf( "# pbl_errno %d, errno %d\n", pbl_errno, errno );
                fprintf( log, "# pbl_errno %d, errno %d\n", pbl_errno, errno );
            }
        }

        else if( !strcmp( command, "close" ))
        {
            if( !kf )
                continue;

            printf( "# pblKfClose( %d )\n", 1 );
            fprintf( log, "# pblKfClose( %d )\n", 1 );

            rc = pblKfClose( kf );
            kf = ( pblKeyFile_t * ) 0;

            printf( "# rc %ld, pbl_errno %d, errno %d\n", rc, pbl_errno, errno );
            fprintf( log, "# rc %ld, pbl_errno %d, errno %d\n",
                     rc, pbl_errno, errno );
        }

        else if( !strcmp( command, "flush" ))
        {
            if( !kf )
                continue;

            printf( "# pblKfFlush( %d )\n", 1 );
            fprintf( log, "# pblKfFlush( %d )\n", 1 );

            rc = pblKfFlush( kf );

            printf( "# rc %ld, pbl_errno %d, errno %d\n", rc, pbl_errno, errno );
            fprintf( log, "# rc %ld, pbl_errno %d, errno %d\n", 
                     rc, pbl_errno, errno );
        }

        else if( !strcmp( command, "delete" ))
        {
            if( !kf )
                continue;

            printf( "# pblKfDelete( %d )\n", 1 );
            fprintf( log, "# pblKfDelete( %d )\n", 1 );

            rc = pblKfDelete( kf );

            printf( "# rc %ld, pbl_errno %d, errno %d\n", rc, pbl_errno, errno );
            fprintf( log, "# rc %ld, pbl_errno %d, errno %d\n", 
                     rc, pbl_errno, errno );
        }

        else if( !strcmp( command, "find" ))
        {
            if( !kf )
                continue;

            printf( "# find key < LT | LE | FI | EQ | LA | GE | GT >\n" );
            fprintf( log, "# find key < LT | LE | FI | EQ | LA | GE | GT >\n" );
            getWord( buffer );
            getWord( filename );
            if( !strcmp( filename, "GT" ))
            {
                ival = PBLGT;
            }
            else if( !strcmp( filename, "FI" ))
            {
                ival = PBLFI;
            }
            else if( !strcmp( filename, "LA" ))
            {
                ival = PBLLA;
            }
            else if( !strcmp( filename, "GE" ))
            {
                ival = PBLGE;
            }
            else if( !strcmp( filename, "LE" ))
            {
                ival = PBLLE;
            }
            else if( !strcmp( filename, "LT" ))
            {
                ival = PBLLT;
            }
            else
            {
                strcpy( filename, "EQ" );
                ival = PBLEQ;
            }
         
            len = strlen( buffer );

            printf( "# pblKfFind( %d, %s, %s, %d )\n",
                     1, filename, buffer, len + 1 );

            fprintf( log, "# pblKfFind( %d, %s, %s, %d )\n",
                     1, filename, buffer, len + 1 );

            rc = pblKfFind( kf, ival, buffer, len + 1, filename, &ival );

            if( rc < 0 )
            {
                printf( "# pbl_errno %d, errno %d\n", pbl_errno, errno );
                fprintf( log, "# pbl_errno %d, errno %d\n", pbl_errno, errno );
            }
            else
            {
                printf( "# datalen %ld, key %s, keylen %d\n",
                        rc, filename, ival);
                fprintf( log, "# datalen %ld, key %s, keylen %d\n",
                        rc, filename, ival);
            }
        }

        else if( !strcmp( command, "insert" ))
        {
            if( !kf )
                continue;

            printf( "# insert key, data\n" );
            fprintf( log, "# insert key, data\n" );
            getWord( buffer );
            len = strlen( buffer );

            getWord( filename );
            ival = strlen( filename );

            printf( "# pblKfInsert( %d, %s, %d, %s, %d )\n",
                     1, buffer, len + 1,
                     filename, ival + 1 );

            fprintf( log, "# pblKfInsert( %d, %s, %d, %s, %d )\n",
                     1, buffer, len + 1,
                     filename, ival + 1 );

            rc = pblKfInsert( kf, buffer, len + 1,
                            filename, (long) ival + 1 );

            printf( "# rc %ld, pbl_errno %d, errno %d\n", rc, pbl_errno, errno );
            fprintf( log, "# rc %ld, pbl_errno %d, errno %d\n", 
                     rc, pbl_errno, errno );
        }

        else if( !strcmp( command, "ninsert" ))
        {
            if( !kf )
                continue;

            printf( "# ninsert n, key, data\n" );
            fprintf( log, "# ninsert n, key, data\n" );
            getWord( command );
            ival = atoi( command );
            getWord( buffer );
            getWord( command );

            printf( "# ninsert( %d )\n", ival );
            fprintf( log, "# ninsert( %d )\n", ival );

            for( i = 0; i < ival; i++ )
            {
                sprintf( filename, "%s_%d", command, i );
          
                len = strlen( filename );
                ival = strlen( buffer );
                rc = pblKfInsert( kf, buffer, ival + 1,
                                  filename, (long) len + 1 );

                if( rc < 0 )
                {
                    printf( "# i %d, rc %ld, pbl_errno %d, errno %d\n",
                            i, rc, pbl_errno, errno );
                    fprintf( log, "# i %d, rc %ld, pbl_errno %d, errno %d\n",
                            i, rc, pbl_errno, errno );
                    break;
                }
            }
            printf( "# inserted %d records\n", i );
            fprintf( log, "# inserted %d records\n", i );

        }

        else if( !strcmp( command, "ndelete" ))
        {
            if( !kf )
                continue;

            printf( "# ndelete n\n" );
            fprintf( log, "# ndelete n\n" );
            getWord( buffer );
            ival = atoi( buffer );

            printf( "# ndelete( %d )\n", ival );
            fprintf( log, "# ndelete( %d )\n", ival );

            for( i = 0; i < ival; i++ )
            {
          
                rc = pblKfDelete( kf );

                if( rc < 0 )
                {
                    printf( "# i %d, rc %ld, pbl_errno %d, errno %d\n",
                            i, rc, pbl_errno, errno );
                    fprintf( log, "# i %d, rc %ld, pbl_errno %d, errno %d\n",
                            i, rc, pbl_errno, errno );
                    break;
                }
            }
            printf( "# deleted %d records\n", i );
            fprintf( log, "# deleted %d records\n", i );

        }

        else if( !strcmp( command, "update" ))
        {
            if( !kf )
                continue;

            printf( "# update data\n" );
            fprintf( log, "# update data\n" );
            getWord( buffer );
            len = strlen( buffer );

            printf( "# pblKfUpdate( %d, %s, %d )\n",
                     1, buffer, len + 1 );
            fprintf( log, "# pblKfUpdate( %d, %s, %d )\n",
                     1, buffer, len + 1 );

            rc = pblKfUpdate( kf, buffer, ( long ) len + 1 );

            printf( "# rc %ld, pbl_errno %d, errno %d\n", rc, pbl_errno, errno );
            fprintf( log, "# rc %ld, pbl_errno %d, errno %d\n",
                          rc, pbl_errno, errno );
        }

        else if( !strcmp( command, "read" ))
        {
            if( !kf )
                continue;

            printf( "# pblKfRead( %d )\n", 1 );
            fprintf( log, "# pblKfRead( %d )\n", 1 );

            rc = pblKfRead( kf, buffer, sizeof( buffer ) );

            if( rc < 0 )
            {
                printf( "# pbl_errno %d, errno %d\n", pbl_errno, errno );
                fprintf( log, "# pbl_errno %d, errno %d\n", pbl_errno, errno );
            }
            else
            {
                printf( "# datalen %ld, data %s\n", rc, buffer );
                fprintf( log, "# datalen %ld, data %s\n", rc, buffer );
            }
        }

        else if( !strcmp( command, "list" ))
        {
            if( !kf )
                continue;

            printf( "# list n\n" );
            fprintf( log, "# list n\n" );
            getWord( buffer );
            update = atoi( buffer );

            printf( "# list( %d )\n", update );
            fprintf( log, "# list( %d )\n", update );

            if( update > 0 )
            {
                rc = pblKfThis( kf, buffer, &ival );
                if( rc < 0 )
                {
                    printf( "# pbl_errno %d, errno %d\n", pbl_errno, errno );
                    fprintf( log, "# pbl_errno %d, errno %d\n", pbl_errno, errno);
                    continue;
                }
                else
                {
                    printf( "# datalen %ld, key %s, keylen %d\n",
                            rc, buffer, ival );
                    fprintf( log, "# datalen %ld, key %s, keylen %d\n",
                            rc, buffer, ival );
                }
            }
            for( i = 1; i < update; i++ )
            {
                rc = pblKfNext( kf, buffer, &ival );
                if( rc < 0 )
                {
                    printf( "# pbl_errno %d, errno %d\n", pbl_errno, errno );
                    fprintf( log, "# pbl_errno %d, errno %d\n", pbl_errno, errno);
                    break;
                }
                else
                {
                    printf( "# datalen %ld, key %s, keylen %d\n",
                            rc, buffer, ival );
                    fprintf( log, "# datalen %ld, key %s, keylen %d\n",
                            rc, buffer, ival );
                }
            }
        }

        else if( !strcmp( command, "first" ))
        {
            if( !kf )
                continue;

            printf( "# pblKfFirst( %d )\n", 1 );
            fprintf( log, "# pblKfFirst( %d )\n", 1 );

            rc = pblKfFirst( kf, buffer, &ival );

            if( rc < 0 )
            {
                printf( "# pbl_errno %d, errno %d\n", pbl_errno, errno );
                fprintf( log, "# pbl_errno %d, errno %d\n", pbl_errno, errno );
            }
            else
            {
                printf( "# datalen %ld, key %s, keylen %d\n",
                        rc, buffer, ival );
                fprintf( log, "# datalen %ld, key %s, keylen %d\n",
                        rc, buffer, ival );
            }
        }

        else if( !strcmp( command, "next" ))
        {
            if( !kf )
                continue;

            printf( "# pblKfNext( %d )\n", 1 );
            fprintf( log, "# pblKfNext( %d )\n", 1 );

            rc = pblKfNext( kf, buffer, &ival );

            if( rc < 0 )
            {
                printf( "# pbl_errno %d, errno %d\n", pbl_errno, errno );
                fprintf( log, "# pbl_errno %d, errno %d\n", pbl_errno, errno );
            }
            else
            {
                printf( "# datalen %ld, key %s, keylen %d\n", 
                        rc, buffer, ival );
                fprintf( log, "# datalen %ld, key %s, keylen %d\n", 
                        rc, buffer, ival );
            }
        }

        else if( !strcmp( command, "last" ))
        {
            if( !kf )
                continue;

            printf( "# pblKfLast( %d )\n", 1 );
            fprintf( log, "# pblKfLast( %d )\n", 1 );

            rc = pblKfLast( kf, buffer, &ival );
            
            if( rc < 0 )
            {
                printf( "# pbl_errno %d, errno %d\n", pbl_errno, errno );
                fprintf( log, "# pbl_errno %d, errno %d\n", pbl_errno, errno );
            }
            else
            {
                printf( "# datalen %ld, key %s, keylen %d\n", 
                        rc, buffer, ival );
                fprintf( log, "# datalen %ld, key %s, keylen %d\n", 
                        rc, buffer, ival );
            }
        }

        else if( !strcmp( command, "prev" ))
        {
            if( !kf )
                continue;

            printf( "# pblKfPrev( %d )\n", 1 );
            fprintf( log, "# pblKfPrev( %d )\n", 1 );
            
            rc = pblKfPrev( kf, buffer, &ival );
            
            if( rc < 0 )
            {
                printf( "# pbl_errno %d, errno %d\n", pbl_errno, errno );
                fprintf( log, "# pbl_errno %d, errno %d\n", pbl_errno, errno );
            }
            else
            {
                printf( "# datalen %ld, key %s, keylen %d\n", 
                        rc, buffer, ival );
                fprintf( log, "# datalen %ld, key %s, keylen %d\n", 
                        rc, buffer, ival );
            }
        }


        else if( !strcmp( command, "this" ))
        {
            if( !kf )
                continue;

            printf( "# pblKfThis( %d )\n", 1 );
            fprintf( log, "# pblKfThis( %d )\n", 1 );
            
            rc = pblKfThis( kf, buffer, &ival );
            
            if( rc < 0 )
            {
                printf( "# pbl_errno %d, errno %d\n", pbl_errno, errno );
                fprintf( log, "# pbl_errno %d, errno %d\n", pbl_errno, errno );
            }
            else
            {
                printf( "# datalen %ld, key %s, keylen %d\n", 
                        rc, buffer, ival );
                fprintf( log, "# datalen %ld, key %s, keylen %d\n", 
                        rc, buffer, ival );
            }
        }

        else
        {
            printf( "# q       FOR QUIT\n" );
            printf( "# create  <filename>\n" );
            printf( "# open    <filename> <update>\n" );
            printf( "# close | flush | delete\n" );
            printf( "# insert  <key> <data>\n" );
            printf( "# ninsert <n> <key> <data>\n" );
            printf( "# find    <key> < LT | LE | FI | EQ | LA | GE | GT >\n" );
            printf( "# update  <data>\n" );
            printf( "# ndelete <n>\n" );
            printf( "# first | last | next | prev | this | read\n" );
            printf( "# list    <n>\n" );
        }
    }

    return( 0 );
}

int main( int argc, char * argv[] )
{
    return( pblKEYFILE_TestFrame( argc, argv ));
}

