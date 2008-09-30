#!/usr/bin/perl -w

use strict;

my $num_threads = 50;

## This perl script generates the input to timer.pl, which in turn
## generates the input to plotting.pl, which generates performance
## graphs. :)

open(LLADD_THREADED,     ">LLADD_THREADED.script"    );
open(LLADD_LOGICAL,      ">LLADD_LOGICAL.script"     );
open(LLADD_PHYSICAL,     ">LLADD_PHYSICAL.script"    );
open(LLADD_RAW_PHYSICAL, ">LLADD_RAW_PHYSICAL.script");
open(BDB_RAW_INSERT,     ">BDB_RAW_INSERT.script"  );
open(BDB_HASH_INSERT,    ">BDB_HASH_INSERT.script" );

# New tests

open(LLADD_NTA,          ">LLADD_NTA.script");
open(LLADD_LINKED_LIST,  ">LLADD_LINKED_LIST.script");
open(LLADD_PAGED_LIST,   ">LLADD_PAGED_LIST.script");
open(BDB_HASH_THREADED,  ">BDB_HASH_THREADED.script");

open(EVERYTHING,         ">EVERYTHING.script"      );

for(my $i = 1; $i <= 10; $i += .5) {
    my $insert_count = $i * 100000;

    my $threaded_insert_count = $insert_count / $num_threads;

    print LLADD_THREADED     "./linearHashNTAThreaded $num_threads $threaded_insert_count\n";
    print LLADD_LOGICAL      "./logicalHash           1 $insert_count\n";
    print LLADD_PHYSICAL     "./naiveHash             1 $insert_count\n";
    print LLADD_RAW_PHYSICAL "./arrayListSet          1 $insert_count\n";
    print BDB_RAW_INSERT     "./berkeleyDB/bdbRaw     1 $insert_count\n";
    print BDB_HASH_INSERT    "./berkeleyDB/bdbHash    1 $insert_count\n";

    print LLADD_NTA          "./linearHashNTA         1 $insert_count\n";
    print LLADD_LINKED_LIST  "./linkedListNTA         1 $insert_count\n";
    print LLADD_PAGED_LIST   "./pageOrientedListNTA   1 $insert_count\n";

    print BDB_HASH_THREADED  "./berkeleyDB/bdbHashThreaded $num_threads $threaded_insert_count\n";

    
    print EVERYTHING         "./linearHashNTAThreaded $num_threads $threaded_insert_count\n";
    print EVERYTHING         "./logicalHash           1 $insert_count\n";
    print EVERYTHING         "./naiveHash             1 $insert_count\n";
    print EVERYTHING         "./arrayListSet          1 $insert_count\n";
    print EVERYTHING         "./berkeleyDB/bdbRaw     1 $insert_count\n";

    print EVERYTHING         "./linearHashNTA         1 $insert_count\n";
    print EVERYTHING         "./linkedListNTA         1 $insert_count\n";
    print EVERYTHING         "./pageOrientedListNTA   1 $insert_count\n";

    if($i < 4) {
        print EVERYTHING         "./berkeleyDB/bdbHashThreaded $num_threads $threaded_insert_count\n";
	print EVERYTHING         "./berkeleyDB/bdbHash    1 $insert_count\n";
    }

} 

close(LLADD_THREADED);
close(LLADD_LOGICAL);
close(LLADD_PHYSICAL);
close(LLADD_RAW_PHYSICAL);
close(BDB_RAW_INSERT);
close(BDB_HASH_INSERT);
close(EVERYTHING);
close(LLADD_NTA);
close(LLADD_LINKED_LIST);
close(LLADD_PAGED_LIST);
close(BDB_HASH_THREADED);
