/*
 pblkfblockprint.c - shell program to print block layout

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
    Revision 1.1  2004/06/24 21:11:34  sears
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

extern int pblKfBlockPrint(
char * path,       /** path of file to create                                 */
long blockno       /** number of block to print                               */
);

int main( int argc, char * argv[] )
{
    long   blockno;

    if( argc != 3 )
    {
        fprintf( stderr, "Usage: %s path blockno\n", argv[ 0 ] );
        exit( 1 );
    }

    blockno = atoi( argv[ 2 ] );

    pblKfBlockPrint( argv[ 1 ], blockno );

    return( 0 );
}

