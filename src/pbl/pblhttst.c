/*
 pblhttst.c - hash table test frame

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

    Revision 1.2  2003/12/11 09:21:20  jim
    update includes

    Revision 1.1  2003/12/11 09:10:48  jim
    pbl

    Revision 1.2  2002/09/12 20:47:01  peter
    added the isam file handling to the library

    Revision 1.1  2002/09/05 13:45:02  peter
    Initial revision

*/

/*
 * make sure "strings <exe> | grep Id | sort -u" shows the source file versions
 */
static char* _PBL_id = "$Id$";
static int   _PBL_fct() { return( _PBL_id ? 0 : _PBL_fct() ); }

#include <stdio.h>
#include <memory.h>
#include <stdlib.h>

#include "pbl.h"

/*****************************************************************************/
/* #defines                                                                  */
/*****************************************************************************/

/*****************************************************************************/
/* typedefs                                                                  */
/*****************************************************************************/

/*****************************************************************************/
/* globals                                                                   */
/*****************************************************************************/

/*****************************************************************************/
/* functions                                                                 */
/*****************************************************************************/

/*
 * test frame for the hash table library
 *
 * this test frame calls the hash table library,
 * it does not have any parameters, it is meant for
 * debugging the hash table library
 */
int pblHASHTABLE_TestFrame( int argc, char * argv[ ] )
{
    pblHashTable_t * ht;
    int    rc;

    char * data;

    ht = pblHtCreate();
    fprintf( stdout, "pblHtCreate() ht = %p\n", ht );

    rc = pblHtInsert( ht, "123", 4, "123" );
    fprintf( stdout, "pblHtInsert( ht, 123, 4, 123 ) rc = %d\n", rc );

    rc = pblHtInsert( ht, "124", 4, "124" );
    fprintf( stdout, "pblHtInsert( ht, 124, 4, 124 ) rc = %d\n", rc );

    rc = pblHtInsert( ht, "125", 4, "125" );
    fprintf( stdout, "pblHtInsert( ht, 125, 4, 125 ) rc = %d\n", rc );

    rc = pblHtInsert( ht, "123", 4, "123" );
    fprintf( stdout, "pblHtInsert( ht, 123, 4, 123 ) rc = %d\n", rc );

    rc = pblHtInsert( ht, "123", 3, "123" );
    fprintf( stdout, "pblHtInsert( ht, 123, 3, 123 ) rc = %d\n", rc );

    data = pblHtLookup( ht, "123", 4 );
    fprintf( stdout, "pblHtLookup( ht, 123, 4 ) data = %s\n",
             data ? data : "NULL" );

    data = pblHtLookup( ht, "123", 3 );
    fprintf( stdout, "pblHtLookup( ht, 123, 3 ) data = %s\n",
             data ? data : "NULL" );

    data = pblHtLookup( ht, "124", 4 );
    fprintf( stdout, "pblHtLookup( ht, 124, 4 ) data = %s\n",
             data ? data : "NULL" );

    data = pblHtLookup( ht, "125", 4 );
    fprintf( stdout, "pblHtLookup( ht, 125, 4 ) data = %s\n",
             data ? data : "NULL" );

    data = pblHtLookup( ht, "126", 4 );
    fprintf( stdout, "pblHtLookup( ht, 126, 4 ) data = %s\n",
             data ? data : "NULL" );


    for( data = pblHtFirst( ht ); data; data = pblHtNext( ht ))
    {
        data = pblHtCurrent( ht );
        fprintf( stdout, "pblHtCurrent( ht ) data = %s\n",
                 data ? data : "NULL" );
    }

    rc = pblHtRemove( ht, "125", 4 );
    fprintf( stdout, "pblHtRemove( ht, 125, 4 ) rc = %d\n", rc );

    data = pblHtFirst( ht );
    fprintf( stdout, "pblHtFirst( ht ) data = %s\n", data ? data : "NULL" );

    rc = pblHtDelete( ht );
    fprintf( stdout, "pblHtDelete( ht, 125, 4 ) rc = %d\n", rc );

    while( !pblHtRemove( ht, 0, 0 ));

    rc = pblHtInsert( ht, "123", 4, "123" );
    fprintf( stdout, "pblHtInsert( ht, 123, 4, 123 ) rc = %d\n", rc );

    rc = pblHtInsert( ht, "124", 4, "124" );
    fprintf( stdout, "pblHtInsert( ht, 124, 4, 124 ) rc = %d\n", rc );

    rc = pblHtInsert( ht, "125", 4, "125" );
    fprintf( stdout, "pblHtInsert( ht, 125, 4, 125 ) rc = %d\n", rc );

    rc = pblHtInsert( ht, "123", 4, "123" );
    fprintf( stdout, "pblHtInsert( ht, 123, 4, 123 ) rc = %d\n", rc );

    rc = pblHtInsert( ht, "123", 3, "123" );
    fprintf( stdout, "pblHtInsert( ht, 123, 3, 123 ) rc = %d\n", rc );


    for( data = pblHtFirst( ht ); data; data = pblHtNext( ht ))
    {
        pblHtRemove( ht, 0, 0 );
    }

    rc = pblHtDelete( ht );
    fprintf( stdout, "pblHtDelete( ht ) rc = %d\n", rc );

    return( rc );
}

int main( int argc, char * argv[] )
{
    return( pblHASHTABLE_TestFrame( argc, argv ));
}

