#!/usr/bin/perl -w

use strict;


## This perl script generates the input to timer.pl, which in turn
## generates the input to plotting.pl, which generates performance
## graphs. :)

open(LLADD_THREADED,     ">LLADD_THREADED.script"    );
open(LLADD_LOGICAL,      ">LLADD_LOGICAL.script"     );
open(LLADD_PHYSICAL,     ">LLADD_PHYSICAL.script"    );
open(LLADD_RAW_PHYSICAL, ">LLADD_RAW_PHYSICAL.script");
open(BDB_RAW_INSERT,     ">BDB_RAW_INSERT.script"  );
open(BDB_HASH_INSERT,    ">BDB_HASH_INSERT.script" );

for(my $i = 1; $i <= 10; $i += .5) {
    my $insert_count = $i * 100000;

    my $threaded_insert_count = $insert_count / 200;

    print LLADD_THREADED     "./logicalMultThreaded 200 $threaded_insert_count\n";
    print LLADD_LOGICAL      "./logicalHash           1 $insert_count\n";
    print LLADD_PHYSICAL     "./naiveHash             1 $insert_count\n";
    print LLADD_RAW_PHYSICAL "./arrayListSet          1 $insert_count\n";
    print BDB_RAW_INSERT     "./berkeleyDB/bdbRaw     1 $insert_count\n";
    print BDB_HASH_INSERT    "./berkeleyDB/bdbHash    1 $insert_count\n";
} 

close(LLADD_THREADED);
close(LLADD_LOGICAL);
close(LLADD_PHYSICAL);
close(LLADD_RAW_PHYSICAL);
close(BDB_RAW_INSERT);
close(BDB_HASH_INSERT);
