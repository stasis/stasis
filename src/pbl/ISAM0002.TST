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
##   1. Build the pbliftst executable.          make all
##   2. Create the sub directory isamtest.      mkdir isamtest
##   3. Clear the sub directory isamtest.       rm imamtest/*
##   4. Run the test frame on this file.        pbliftst ISAM0002.TST
##   5. Compare ISAM0002.TST and pbliftst.log   diff ISAM0002.TST pbliftst.log
##
##   There should be no differences reported, if so your build of the
##   PBL library is most likely ok!
##
###########################################################################
##
## Test case 2
##
## Create a file with 200000 records, close and reopen the file
## and then do a lot of record lookup requests
##
open isamtest/0002main 0002key0,0002dup1,0002key2 1
# pblIsamOpen( isamtest/0002main, 3, 1 )
# ok!
##
## read the first record
##
get 0 FIRST
# pblIsamGet( 4, 0 )
# rc -1, pbl_errno 1003, errno 0
##
## make sure the file is empty
##
ndelete 250000
# pblIsamDelete( 250000 records )
# i 0, rc -1, pbl_errno 1041, errno 0
##
## Insert a lot of records
##
ninsert 200000 key0,duplicatekey1,loooooooooooooooooooooongerkey2 dataaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
# pblIsamInsert( 1, ,key00,duplicatekey10,loooooooooooooooooooooongerkey20, 54, dataaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa, 61 )
# inserted 200000 records, rc 0
get 0 FIRST
# pblIsamGet( 4, 0 )
# keylen 5, key key00
get 0 LAST
# pblIsamGet( 5, 0 )
# keylen 9, key key099999
get 1 FIRST
# pblIsamGet( 4, 1 )
# keylen 14, key duplicatekey10
get 1 LAST
# pblIsamGet( 5, 1 )
# keylen 18, key duplicatekey199999
get 2 FIRST
# pblIsamGet( 4, 2 )
# keylen 32, key loooooooooooooooooooooongerkey20
get 2 LAST
# pblIsamGet( 5, 2 )
# keylen 36, key loooooooooooooooooooooongerkey299999
close
# pblIsamClose( 1 )
# rc 0
##
## open the file for reading
##
open isamtest/0002main 0002key0,0002dup1,0002key2 0
# pblIsamOpen( isamtest/0002main, 3, 0 )
# ok!
##
## do a lot of finds
##
get 0 FIRST
# pblIsamGet( 4, 0 )
# keylen 5, key key00
get 0 LAST
# pblIsamGet( 5, 0 )
# keylen 9, key key099999
nfind 200000 0 key0 LT
# pblIsamFind( 1, LT, 0, key0, 4 )
# found 200000 records, rc 0
nfind 200000 0 key0 LE
# pblIsamFind( 1, LE, 0, key0, 4 )
# found 200000 records, rc 0
nfind 200000 0 key0 FI
# pblIsamFind( 1, FI, 0, key0, 4 )
# found 200000 records, rc 0
nfind 200000 0 key0 EQ
# pblIsamFind( 1, EQ, 0, key0, 4 )
# found 200000 records, rc 0
nfind 200000 0 key0 LA
# pblIsamFind( 1, LA, 0, key0, 4 )
# found 200000 records, rc 0
nfind 200000 0 key0 GE
# pblIsamFind( 1, GE, 0, key0, 4 )
# found 200000 records, rc 0
nfind 200000 0 key0 GT
# pblIsamFind( 1, GT, 0, key0, 4 )
# found 200000 records, rc 0
get 1 FIRST
# pblIsamGet( 4, 1 )
# keylen 14, key duplicatekey10
get 1 LAST
# pblIsamGet( 5, 1 )
# keylen 18, key duplicatekey199999
nfind 200000 1 duplicatekey1 LT
# pblIsamFind( 1, LT, 1, duplicatekey1, 13 )
# found 200000 records, rc 0
nfind 200000 1 duplicatekey1 LE
# pblIsamFind( 1, LE, 1, duplicatekey1, 13 )
# found 200000 records, rc 0
nfind 200000 1 duplicatekey1 FI
# pblIsamFind( 1, FI, 1, duplicatekey1, 13 )
# found 200000 records, rc 0
nfind 200000 1 duplicatekey1 EQ
# pblIsamFind( 1, EQ, 1, duplicatekey1, 13 )
# found 200000 records, rc 0
nfind 200000 1 duplicatekey1 LA
# pblIsamFind( 1, LA, 1, duplicatekey1, 13 )
# found 200000 records, rc 0
nfind 200000 1 duplicatekey1 GE
# pblIsamFind( 1, GE, 1, duplicatekey1, 13 )
# found 200000 records, rc 0
nfind 200000 1 duplicatekey1 GT
# pblIsamFind( 1, GT, 1, duplicatekey1, 13 )
# found 200000 records, rc 0
get 2 FIRST
# pblIsamGet( 4, 2 )
# keylen 32, key loooooooooooooooooooooongerkey20
get 2 LAST
# pblIsamGet( 5, 2 )
# keylen 36, key loooooooooooooooooooooongerkey299999
nfind 200000 2 loooooooooooooooooooooongerkey2 LT
# pblIsamFind( 1, LT, 2, loooooooooooooooooooooongerkey2, 31 )
# found 200000 records, rc 0
nfind 200000 2 loooooooooooooooooooooongerkey2 LE
# pblIsamFind( 1, LE, 2, loooooooooooooooooooooongerkey2, 31 )
# found 200000 records, rc 0
nfind 200000 2 loooooooooooooooooooooongerkey2 FI
# pblIsamFind( 1, FI, 2, loooooooooooooooooooooongerkey2, 31 )
# found 200000 records, rc 0
nfind 200000 2 loooooooooooooooooooooongerkey2 EQ
# pblIsamFind( 1, EQ, 2, loooooooooooooooooooooongerkey2, 31 )
# found 200000 records, rc 0
nfind 200000 2 loooooooooooooooooooooongerkey2 LA
# pblIsamFind( 1, LA, 2, loooooooooooooooooooooongerkey2, 31 )
# found 200000 records, rc 0
nfind 200000 2 loooooooooooooooooooooongerkey2 GE
# pblIsamFind( 1, GE, 2, loooooooooooooooooooooongerkey2, 31 )
# found 200000 records, rc 0
nfind 200000 2 loooooooooooooooooooooongerkey2 GT
# pblIsamFind( 1, GT, 2, loooooooooooooooooooooongerkey2, 31 )
# found 200000 records, rc 0
quit
