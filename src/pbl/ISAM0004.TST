###########################################################################
##
## PBL - Program Base Library, Copyright 2002 Peter Graf
##
##  This file is part of PBL - The Program Base Library.
##  PBL is free software.
##
##   This program is free software; you can redistribute it and/or modify
##   it under the terms of the GNU General Public License as published by
##   the Free Software Foundation; either version 2 of the License, or
##   (at your option) any later version.
##
##   This program is distributed in the hope that it will be useful,
##   but WITHOUT ANY WARRANTY; without even the implied warranty of
##   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
##   GNU General Public License for more details.
##
##   You should have received a copy of the GNU General Public License
##   along with this program; if not, write to the Free Software
##   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
##
##  For more information on the Program Base Library or Peter Graf,
##  please see: http://mission.base.com/.
##
## ISAM file implementation test case, this test case is set up for 
## a regression test of the ISAM library, don't change it unless you 
## know what you are doing!
##
## Usage: 
##
##   1. Build the pbliftst executable.          make all
##   2. Create the sub directory isamtest.      mkdir isamtest
##   3. Clear the sub directory isamtest.       rm imamtest/*
##   4. Run the test frame on this file.        pbliftst ISAM0004.TST
##   5. Compare ISAM0004.TST and pbliftst.log   diff ISAM0004.TST pbliftst.log
##
##   there should be no differences reported, if so your build of the
##   PBL library is most likely ok!
##
###########################################################################
##
## Test case 4, tests insert, find, get on duplicate keys
##
## Open the file isam file isamtest/0004main with three index files
##  0004key0, 0004dup1 and 0004key2
##  open the file for update, create if necessary
##   the index 0004key0 contains unique keys
##   the index 0004dup1 can contain duplicate keys, 
##    ( the test frame allows that because its name contains the string 'dup' )
##   the index 0004key2 contains unique keys
##
## open filename keyfile1,dkeyfile2,... update
##
open isamtest/0004main 0004key0,0004dup1,0004key2 1
# pblIsamOpen( isamtest/0004main, 3, 1 )
# ok!
##
## Start a transaction
##
transaction START
# pblIsamStartTransaction( )
# rc 0
##
## Get the first record according to index 0004key0, should report an error
##
## get index < NEXT | PREV | FIRST | LAST | THIS >
##
get 0 FIRST
# pblIsamGet( 4, 0 )
# rc -1, pbl_errno 1003, errno 0
##
## Delete 100000 records, should report an error
##
## ndelete n
##
ndelete 100000
# pblIsamDelete( 100000 records )
# i 0, rc -1, pbl_errno 1041, errno 0
ninsert 2000 key0,dup1,key2 data
# pblIsamInsert( 1, ,key00,dup10,key20, 18, data, 5 )
# inserted 2000 records, rc 0
##
## Read alphabetically last record according to index 0004dup1
## Try reading and searching beyond the last record
##
get 1 LAST
# pblIsamGet( 5, 1 )
# keylen 7, key dup1999
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 7, key key0999
get 1 NEXT
# pblIsamGet( 2, 1 )
# rc -1, pbl_errno 1003, errno 0
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 7, key key0999
find 1 dup1999 LA
# pblIsamFind( LA, dup1999, 7 )
# keylen 7, key dup1999
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 7, key key0999
find 1 dup1999 GE
# pblIsamFind( GE, dup1999, 7 )
# keylen 7, key dup1999
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 7, key key0999
find 1 dup1999 GT
# pblIsamFind( GT, dup1999, 7 )
# rc -1, pbl_errno 1003, errno 0
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 7, key key0999
##
## Read alphabetically first record according to index 0004dup1
## Try reading and searching beyond the first record
##
get 1 FIRST
# pblIsamGet( 4, 1 )
# keylen 5, key dup10
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 5, key key00
get 1 PREV
# pblIsamGet( 3, 1 )
# rc -1, pbl_errno 1003, errno 0
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 5, key key00
find 1 dup10 FI
# pblIsamFind( FI, dup10, 5 )
# keylen 5, key dup10
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 5, key key00
find 1 dup10 LE
# pblIsamFind( LE, dup10, 5 )
# keylen 5, key dup10
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 5, key key00
find 1 dup10 LT
# pblIsamFind( LT, dup10, 5 )
# rc -1, pbl_errno 1003, errno 0
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 5, key key00
##
## Find a record in the middle and read the records surounding it
##
find 1 dup12 LT
# pblIsamFind( LT, dup12, 5 )
# keylen 8, key dup11999
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 8, key key01999
get 1 NEXT
# pblIsamGet( 2, 1 )
# keylen 5, key dup12
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 5, key key02
get 1 NEXT
# pblIsamGet( 2, 1 )
# keylen 6, key dup120
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 6, key key020
get 1 NEXT
# pblIsamGet( 2, 1 )
# keylen 7, key dup1200
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 7, key key0200
##
## Test all possible find operations on an inner record
##
find 1 dup12 LT
# pblIsamFind( LT, dup12, 5 )
# keylen 8, key dup11999
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 8, key key01999
find 1 dup12 LE
# pblIsamFind( LE, dup12, 5 )
# keylen 5, key dup12
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 5, key key02
find 1 dup12 FI
# pblIsamFind( FI, dup12, 5 )
# keylen 5, key dup12
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 5, key key02
find 1 dup12 EQ
# pblIsamFind( EQ, dup12, 5 )
# keylen 5, key dup12
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 5, key key02
find 1 dup12 LA
# pblIsamFind( LA, dup12, 5 )
# keylen 5, key dup12
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 5, key key02
find 1 dup12 GE
# pblIsamFind( GE, dup12, 5 )
# keylen 5, key dup12
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 5, key key02
find 1 dup12 GT
# pblIsamFind( GT, dup12, 5 )
# keylen 6, key dup120
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 6, key key020
##
## Tnsert two records with a duplicate key
##
insert ,key1insert1,dup12,key2insert1 data1
# pblIsamInsert( 1, ,key1insert1,dup12,key2insert1, 30, data1, 6 )
# rc 0
insert ,key1insert2,dup12,key2insert2 data1
# pblIsamInsert( 1, ,key1insert2,dup12,key2insert2, 30, data1, 6 )
# rc 0
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 11, key key1insert2
##
## Test the find operations on the records with duplicate keys
##
find 1 dup12 LT
# pblIsamFind( LT, dup12, 5 )
# keylen 8, key dup11999
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 8, key key01999
find 1 dup12 LE
# pblIsamFind( LE, dup12, 5 )
# keylen 5, key dup12
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 5, key key02
find 1 dup12 FI
# pblIsamFind( FI, dup12, 5 )
# keylen 5, key dup12
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 5, key key02
get 1 NEXT
# pblIsamGet( 2, 1 )
# keylen 5, key dup12
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 11, key key1insert1
get 1 NEXT
# pblIsamGet( 2, 1 )
# keylen 5, key dup12
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 11, key key1insert2
get 1 NEXT
# pblIsamGet( 2, 1 )
# keylen 6, key dup120
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 6, key key020
find 1 dup12 EQ
# pblIsamFind( EQ, dup12, 5 )
# keylen 5, key dup12
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 5, key key02
find 1 dup12 LA
# pblIsamFind( LA, dup12, 5 )
# keylen 5, key dup12
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 11, key key1insert2
find 1 dup12 GE
# pblIsamFind( GE, dup12, 5 )
# keylen 5, key dup12
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 11, key key1insert2
find 1 dup12 GT
# pblIsamFind( GT, dup12, 5 )
# keylen 6, key dup120
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 6, key key020
##
## Delete the three records with duplicate key dup150
##
find 1 dup12 FI
# pblIsamFind( FI, dup12, 5 )
# keylen 5, key dup12
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 5, key key02
ndelete 1
# pblIsamDelete( 1 records )
# deleted 1 records, rc 0
find 1 dup12 FI
# pblIsamFind( FI, dup12, 5 )
# keylen 5, key dup12
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 11, key key1insert1
ndelete 1
# pblIsamDelete( 1 records )
# deleted 1 records, rc 0
find 1 dup12 FI
# pblIsamFind( FI, dup12, 5 )
# keylen 5, key dup12
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 11, key key1insert2
ndelete 1
# pblIsamDelete( 1 records )
# deleted 1 records, rc 0
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 8, key key01999
##
## Test the find operations on the deleted records with duplicate keys
##
find 1 dup12 LT
# pblIsamFind( LT, dup12, 5 )
# keylen 8, key dup11999
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 8, key key01999
find 1 dup12 LE
# pblIsamFind( LE, dup12, 5 )
# keylen 8, key dup11999
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 8, key key01999
find 1 dup12 FI
# pblIsamFind( FI, dup12, 5 )
# rc -1, pbl_errno 1003, errno 0
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 8, key key01999
find 1 dup12 EQ
# pblIsamFind( EQ, dup12, 5 )
# rc -1, pbl_errno 1003, errno 0
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 8, key key01999
find 1 dup12 LA
# pblIsamFind( LA, dup12, 5 )
# rc -1, pbl_errno 1003, errno 0
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 8, key key01999
find 1 dup12 GE
# pblIsamFind( GE, dup12, 5 )
# keylen 6, key dup120
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 6, key key020
find 1 dup12 GT
# pblIsamFind( GT, dup12, 5 )
# keylen 6, key dup120
get 0 THIS
# pblIsamGet( 1, 0 )
# keylen 6, key key020
##
## Delete 1999 records, i.e. all of them
##
ndelete 1999
# pblIsamDelete( 1999 records )
# deleted 1999 records, rc 0
##
## Verify the file is empty now
##
get 0 FIRST
# pblIsamGet( 4, 0 )
# rc -1, pbl_errno 1003, errno 0
get 1 FIRST
# pblIsamGet( 4, 1 )
# rc -1, pbl_errno 1003, errno 0
get 2 FIRST
# pblIsamGet( 4, 2 )
# rc -1, pbl_errno 1003, errno 0
##
## Rollback the transaction, the inserts that happened
## after the transaction started are NOT committed
##
transaction ROLLBACK
# pblIsamCommit( ROLLBACK )
# rc 1, pbl_errno 0, errno 0
##
## Close the file
##
close
# pblIsamClose( 1 )
# rc 0
quit
