/*
 pbltest.c - test functions

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
    Revision 1.1  2004/06/24 21:12:08  sears
    Initial revision

    Revision 1.1  2003/12/11 09:10:49  jim
    pbl

    Revision 1.2  2002/09/12 20:47:08  peter
    added the isam file handling to the library

    Revision 1.1  2002/09/05 13:45:03  peter
    Initial revision


*/

#ifdef __cplusplus
extern "C" {
#endif

/*
 * make sure "strings <exe> | grep Id | sort -u" shows the source file versions
 */
static char* _PBL_id = "$Id$";
static int   _PBL_fct() { return( _PBL_id ? 0 : _PBL_fct() ); }

#include <stdio.h>
#include <memory.h>
#include <malloc.h>

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

int main( int argc, char * argv[ ] )
{
    void * ht;
    int    rc;

    char * data;

    ht = pblHtCreate();

    rc = pblHtInsert( ht, "123", 4, "123" );
    rc = pblHtInsert( ht, "124", 4, "124" );
    rc = pblHtInsert( ht, "125", 4, "125" );
    rc = pblHtInsert( ht, "123", 4, "123" );
    rc = pblHtInsert( ht, "123", 3, "123" );

    data = pblHtLookup( ht, "123", 4 );
    data = pblHtLookup( ht, "123", 3 );
    data = pblHtLookup( ht, "124", 4 );
    data = pblHtLookup( ht, "125", 4 );
    data = pblHtLookup( ht, "126", 4 );

    for( data = pblHtFirst( ht ); data; data = pblHtNext( ht ))
    {
        data = pblHtCurrent( ht );
    }

    rc = pblHtRemove( ht, "125", 4 );

    data = pblHtFirst( ht );

    rc = pblHtDelete( ht );

    while( !pblHtRemove( ht, 0, 0 ));

    rc = pblHtInsert( ht, "123", 4, "123" );
    rc = pblHtInsert( ht, "124", 4, "124" );
    rc = pblHtInsert( ht, "125", 4, "125" );
    rc = pblHtInsert( ht, "123", 4, "123" );
    rc = pblHtInsert( ht, "123", 3, "123" );

    for( data = pblHtFirst( ht ); data; data = pblHtNext( ht ))
    {
        pblHtRemove( ht, 0, 0 );
    }

    rc = pblHtDelete( ht );

    return( rc );
}

