###########################################################################
##
## PBL - Program Base Library, Copyright Peter Graf
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
##   4. Run the test frame on this file.        pbliftst ISAM0005.TST
##   5. Compare ISAM0005.TST and pbliftst.log   diff ISAM0005.TST pbliftst.log
##
##   There should be no differences reported, if so your build of the
##   PBL library is most likely ok!
##
###########################################################################
##
## Test case 5
##
## Create a file with 2000000 records,
## and then do some lookup requests
##
## !!!!! NOTE !!!! Test needs 700 Megabytes of disc space
##
open isamtest/0005main 0005key0,0005dup1,0005key2 1
# pblIsamOpen( isamtest/0005main, 3, 1 )
# ok!
##
## read the first record
##
get 0 FIRST
# pblIsamGet( 4, 0 )
# rc -1, pbl_errno 1003, errno 0
ndelete 2500000
# pblIsamDelete( 2500000 records )
# i 0, rc -1, pbl_errno 1041, errno 0
##
## Insert a lot of records
##
ninsert 2000000 key0,duplicatekey1,loooooooooooooooooooooongerkey2 dataaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
# pblIsamInsert( 1, ,key00,duplicatekey10,loooooooooooooooooooooongerkey20, 54, dataaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa, 61 )
# inserted 2000000 records, rc 0
get -1 FIRST
# pblIsamGet( 4, -1 )
# keylen 0, key 
datalen
# pblIsamReadDatalen( currentrecord )
# datalen 61
readkey 0
# pblIsamReadKey( currentrecord, 0 )
# keylen 5, key key00
get -1 PREV
# pblIsamGet( 3, -1 )
# rc -1, pbl_errno 1003, errno 0
datalen
# pblIsamReadDatalen( currentrecord )
# datalen 61
readkey 0
# pblIsamReadKey( currentrecord, 0 )
# keylen 5, key key00
get -1 NEXT
# pblIsamGet( 2, -1 )
# keylen 0, key 
datalen
# pblIsamReadDatalen( currentrecord )
# datalen 61
readkey 0
# pblIsamReadKey( currentrecord, 0 )
# keylen 5, key key01
get -1 THIS
# pblIsamGet( 1, -1 )
# keylen 0, key 
datalen
# pblIsamReadDatalen( currentrecord )
# datalen 61
readkey 0
# pblIsamReadKey( currentrecord, 0 )
# keylen 5, key key01
get -1 LAST
# pblIsamGet( 5, -1 )
# keylen 0, key 
datalen
# pblIsamReadDatalen( currentrecord )
# datalen 61
readkey 0
# pblIsamReadKey( currentrecord, 0 )
# keylen 11, key key01999999
get -1 NEXT
# pblIsamGet( 2, -1 )
# rc -1, pbl_errno 1003, errno 0
datalen
# pblIsamReadDatalen( currentrecord )
# datalen 61
readkey 0
# pblIsamReadKey( currentrecord, 0 )
# keylen 11, key key01999999
get -1 PREV
# pblIsamGet( 3, -1 )
# keylen 0, key 
datalen
# pblIsamReadDatalen( currentrecord )
# datalen 61
readkey 0
# pblIsamReadKey( currentrecord, 0 )
# keylen 11, key key01999998
get 0 FIRST
# pblIsamGet( 4, 0 )
# keylen 5, key key00
get 0 PREV
# pblIsamGet( 3, 0 )
# rc -1, pbl_errno 1003, errno 0
get 0 NEXT
# pblIsamGet( 2, 0 )
# keylen 5, key key01
get 0 NEXT
# pblIsamGet( 2, 0 )
# keylen 6, key key010
get 0 LAST
# pblIsamGet( 5, 0 )
# keylen 10, key key0999999
get 0 NEXT
# pblIsamGet( 2, 0 )
# rc -1, pbl_errno 1003, errno 0
get 0 PREV
# pblIsamGet( 3, 0 )
# keylen 10, key key0999998
get 0 PREV
# pblIsamGet( 3, 0 )
# keylen 10, key key0999997
get 1 FIRST
# pblIsamGet( 4, 1 )
# keylen 14, key duplicatekey10
get 1 PREV
# pblIsamGet( 3, 1 )
# rc -1, pbl_errno 1003, errno 0
get 1 NEXT
# pblIsamGet( 2, 1 )
# keylen 14, key duplicatekey11
get 1 NEXT
# pblIsamGet( 2, 1 )
# keylen 15, key duplicatekey110
get 1 LAST
# pblIsamGet( 5, 1 )
# keylen 19, key duplicatekey1999999
get 1 NEXT
# pblIsamGet( 2, 1 )
# rc -1, pbl_errno 1003, errno 0
get 1 PREV
# pblIsamGet( 3, 1 )
# keylen 19, key duplicatekey1999998
get 1 PREV
# pblIsamGet( 3, 1 )
# keylen 19, key duplicatekey1999997
get 2 FIRST
# pblIsamGet( 4, 2 )
# keylen 32, key loooooooooooooooooooooongerkey20
get 2 PREV
# pblIsamGet( 3, 2 )
# rc -1, pbl_errno 1003, errno 0
get 2 NEXT
# pblIsamGet( 2, 2 )
# keylen 32, key loooooooooooooooooooooongerkey21
get 2 NEXT
# pblIsamGet( 2, 2 )
# keylen 33, key loooooooooooooooooooooongerkey210
get 2 LAST
# pblIsamGet( 5, 2 )
# keylen 37, key loooooooooooooooooooooongerkey2999999
get 2 NEXT
# pblIsamGet( 2, 2 )
# rc -1, pbl_errno 1003, errno 0
get 2 PREV
# pblIsamGet( 3, 2 )
# keylen 37, key loooooooooooooooooooooongerkey2999998
get 2 PREV
# pblIsamGet( 3, 2 )
# keylen 37, key loooooooooooooooooooooongerkey2999997
##
## do some finds in the middle of the file
##
find 0 key02 LT
# pblIsamFind( LT, key02, 5 )
# keylen 11, key key01999999
get 0 PREV
# pblIsamGet( 3, 0 )
# keylen 11, key key01999998
get 0 NEXT
# pblIsamGet( 2, 0 )
# keylen 11, key key01999999
get 0 NEXT
# pblIsamGet( 2, 0 )
# keylen 5, key key02
get 0 NEXT
# pblIsamGet( 2, 0 )
# keylen 6, key key020
find 0 key02 LE
# pblIsamFind( LE, key02, 5 )
# keylen 5, key key02
find 0 key02 FI
# pblIsamFind( FI, key02, 5 )
# keylen 5, key key02
find 0 key02 EQ
# pblIsamFind( EQ, key02, 5 )
# keylen 5, key key02
find 0 key02 LA
# pblIsamFind( LA, key02, 5 )
# keylen 5, key key02
find 0 key02 GE
# pblIsamFind( GE, key02, 5 )
# keylen 5, key key02
find 0 key02 GT
# pblIsamFind( GT, key02, 5 )
# keylen 6, key key020
find 1 duplicatekey12 LT
# pblIsamFind( LT, duplicatekey12, 14 )
# keylen 20, key duplicatekey11999999
get 1 PREV
# pblIsamGet( 3, 1 )
# keylen 20, key duplicatekey11999998
get 1 NEXT
# pblIsamGet( 2, 1 )
# keylen 20, key duplicatekey11999999
get 1 NEXT
# pblIsamGet( 2, 1 )
# keylen 14, key duplicatekey12
get 1 NEXT
# pblIsamGet( 2, 1 )
# keylen 15, key duplicatekey120
find 1 duplicatekey12 LE
# pblIsamFind( LE, duplicatekey12, 14 )
# keylen 14, key duplicatekey12
find 1 duplicatekey12 FI
# pblIsamFind( FI, duplicatekey12, 14 )
# keylen 14, key duplicatekey12
find 1 duplicatekey12 EQ
# pblIsamFind( EQ, duplicatekey12, 14 )
# keylen 14, key duplicatekey12
find 1 duplicatekey12 LA
# pblIsamFind( LA, duplicatekey12, 14 )
# keylen 14, key duplicatekey12
find 1 duplicatekey12 GE
# pblIsamFind( GE, duplicatekey12, 14 )
# keylen 14, key duplicatekey12
find 1 duplicatekey12 GT
# pblIsamFind( GT, duplicatekey12, 14 )
# keylen 15, key duplicatekey120
find 2 loooooooooooooooooooooongerkey22 LT
# pblIsamFind( LT, loooooooooooooooooooooongerkey22, 32 )
# keylen 38, key loooooooooooooooooooooongerkey21999999
get 2 PREV
# pblIsamGet( 3, 2 )
# keylen 38, key loooooooooooooooooooooongerkey21999998
get 2 NEXT
# pblIsamGet( 2, 2 )
# keylen 38, key loooooooooooooooooooooongerkey21999999
get 2 NEXT
# pblIsamGet( 2, 2 )
# keylen 32, key loooooooooooooooooooooongerkey22
get 2 NEXT
# pblIsamGet( 2, 2 )
# keylen 33, key loooooooooooooooooooooongerkey220
find 2 loooooooooooooooooooooongerkey22 LE
# pblIsamFind( LE, loooooooooooooooooooooongerkey22, 32 )
# keylen 32, key loooooooooooooooooooooongerkey22
find 2 loooooooooooooooooooooongerkey22 FI
# pblIsamFind( FI, loooooooooooooooooooooongerkey22, 32 )
# keylen 32, key loooooooooooooooooooooongerkey22
find 2 loooooooooooooooooooooongerkey22 EQ
# pblIsamFind( EQ, loooooooooooooooooooooongerkey22, 32 )
# keylen 32, key loooooooooooooooooooooongerkey22
find 2 loooooooooooooooooooooongerkey22 LA
# pblIsamFind( LA, loooooooooooooooooooooongerkey22, 32 )
# keylen 32, key loooooooooooooooooooooongerkey22
find 2 loooooooooooooooooooooongerkey22 GE
# pblIsamFind( GE, loooooooooooooooooooooongerkey22, 32 )
# keylen 32, key loooooooooooooooooooooongerkey22
find 2 loooooooooooooooooooooongerkey22 GT
# pblIsamFind( GT, loooooooooooooooooooooongerkey22, 32 )
# keylen 33, key loooooooooooooooooooooongerkey220
##
## More finds at the start of the file
##
find 0 key00 LT
# pblIsamFind( LT, key00, 5 )
# rc -1, pbl_errno 1003, errno 0
find 0 key00 LE
# pblIsamFind( LE, key00, 5 )
# keylen 5, key key00
find 0 key00 FI
# pblIsamFind( FI, key00, 5 )
# keylen 5, key key00
get 0 PREV
# pblIsamGet( 3, 0 )
# rc -1, pbl_errno 1003, errno 0
close
# pblIsamClose( 1 )
# rc 0
quit
