#!/usr/bin/perl -w

use strict;

my $num_threads = 50;

#
#  Script to generate benchmarking scripts for the SOSP submission.
# 
#  This supports the following graphs:
#
#  * Comparison with Berkeley DB:
#
#    - Single-threaded raw throughput for LLADD's opeartions and 
#      Berkeley DB's storage methods.
#
#    - Multithreaded hash-based transactions per second.
#
#    - Multithreaded response time distributions
#
#  * Object serialization benchmarks:
#
#    - OASYS memory footprint  (@todo:  How to limit berkeley db's 
#      memory footprint?)
#        o Three graphs  (fixed memory pool size) 
#
#            100% object changed / update
#             50% 
#             10%
#
#        o Three lines per Graph:  LLADD diff, LLADD naive, Berkeley DB
#
#  * Validation of flexibilty / ability to perform optimizations
#
#    - Load burst absorption (LLADD w/ async vs LLADD w/out async)
#
#    - Transitive closure (oo7 subset) (LLADD naive vs LLADD 
#      optimized: async + TsetRange())
#
#    - CHT:  Throughput vs number of replica groups:  Disk Force vs 
#      Two sites in RAM.
#
#


## This perl script generates the input to timer.pl, which in turn
## generates the input to plotting.pl, which generates performance
## graphs. :)


## Single threaded raw-throughput graph:

open(LLADD_NTA_HASH,     ">LLADD_NTA_HASH.script");
open(LLADD_FAST_HASH,    ">LLADD_FAST_HASH.script");
open(LLADD_ARRAY_LIST,   ">LLADD_ARRAY_LIST.script");
open(LLADD_LINKED_LIST,  ">LLADD_LINKED_LIST.script");
open(LLADD_RECORDID,     ">LLADD_RECORDID.script");
open(BDB_HASH,           ">BDB_HASH.script");
open(BDB_RAW,            ">BDB_RAW.script");
open(BULK_LOAD,          ">BULK_LOAD.script");

for(my $i = 1; $i <= 10; $i ++) {
    my $insert_count = $i * 100000;

    my $thread_count = 200;
    my $threaded_insert_count = $insert_count / 200;
    
    print LLADD_NTA_HASH     "../linearHashNTAThreaded       1 $insert_count\n";
    print LLADD_FAST_HASH    "../logicalHash                 1 $insert_count\n";  # could do ./naiveHash instead...
    print LLADD_ARRAY_LIST   "../arrayListSet                1 $insert_count\n";
    print LLADD_LINKED_LIST  "../pageOrientedList            1 $insert_count\n";  # could do ./linkedList instead...
    print LLADD_RECORDID     "../rawSet                      1 $insert_count\n"; 
    print BDB_HASH           "../berkeleyDB/bdbHashThreaded  1 $insert_count 0 1\n";
    print BDB_RAW            "../berkeleyDB/bdbHashThreaded  1 $insert_count 0 0\n";

    print BULK_LOAD          "../linearHashNTAThreaded       1 $insert_count\n";
    print BULK_LOAD          "../logicalHash                 1 $insert_count\n";  # could do ./naiveHash instead...
    print BULK_LOAD          "../arrayListSet                1 $insert_count\n";
    print BULK_LOAD          "../pageOrientedList            1 $insert_count\n";  # could do ./linkedList instead...
    print BULK_LOAD          "../rawSet                      1 $insert_count\n"; 
    print BULK_LOAD          "../berkeleyDB/bdbHashThreaded  1 $insert_count 0 1\n";
    print BULK_LOAD          "../berkeleyDB/bdbHashThreaded  1 $insert_count 0 0\n";

}

close(LLADD_NTA_HASH);
close(LLADD_FAST_HASH);
close(LLADD_ARRAY_LIST);
close(LLADD_LINKED_LIST);
close(LLADD_RECORDID);
close(BDB_HASH);
close(BDB_RAW);
close(BULK_LOAD);

## Throughput vs. number of transactions

open(LLADD_HASH_TPS,          ">LLADD_HASH_TPS.script");
open(BDB_HASH_TPS,           ">BDB_HASH_TPS.script");

open(LLADD_RECNO_TPS,         ">LLADD_RECNO_TPS.script"); 
open(BDB_RECNO_TPS,           ">BDB_RECNO_TPS.script");

open(TPS,   ">TPS.script");

for(my $i = 0; $i <= 200; $i ++ ) {
    my $insert_threads = $i * 10;
    if($insert_threads == 0) {
	$insert_threads = 1;
    }

    my $insert_per_thread = 1000; # / $insert_threads;

    print LLADD_HASH_TPS     "../linearHashNTAThreaded        $insert_threads   $insert_per_thread 1\n";
    print BDB_HASH_TPS      "../berkeleyDB/bdbHashThreaded        $insert_threads   $insert_per_thread 1 1\n";

    print TPS     "../linearHashNTAThreaded        $insert_threads   $insert_per_thread 1\n";
    print TPS      "../berkeleyDB/bdbHashThreaded        $insert_threads   $insert_per_thread 1 1\n";

}

close(TPS);

## Response time degradation

open(LLADD_SEDA,         ">LLADD_SEDA.script");
open(BDB_SEDA,           ">BDB_SEDA.script");

open(SEDA,               ">SEDA.script");

for(my $i = 1; $i <= 20; $i ++) {
#    my $insert_count = $i * 100000;
    my $num_threads    = $i * 20;
    my $tps_per_thread = 10;
    print LLADD_SEDA         "../linearHashNTAWriteRequests      $num_threads $tps_per_thread 1\n";
    print BDB_SEDA           "../berkeleyDB/bdbHashWriteRequests $num_threads $tps_per_thread 1\n";

    print SEDA               "../linearHashNTAWriteRequests      $num_threads $tps_per_thread 1\n";
    print SEDA               "../berkeleyDB/bdbHashWriteRequests $num_threads $tps_per_thread 1\n";

}

close LLADD_SEDA;
close BDB_SEDA;
close SEDA;

exit;
## ---------------- OASYS STUFF ---------------------------

# blah blah blah

## ---------------- ASYNC STUFF --------------------------

# load bursts

# trans closure 


## ---------------- CHT STUFF -----------------------------

#cht vs # hosts.






#open(LLADD_THREADED,     ">LLADD_THREADED.script"    );
#open(LLADD_LOGICAL,      ">LLADD_LOGICAL.script"     );
#open(LLADD_PHYSICAL,     ">LLADD_PHYSICAL.script"    );
#open(LLADD_RAW_PHYSICAL, ">LLADD_RAW_PHYSICAL.script");
#open(BDB_RAW_INSERT,     ">BDB_RAW_INSERT.script"  );
#open(BDB_HASH_INSERT,    ">BDB_HASH_INSERT.script" );

# New tests

#open(LLADD_NTA,          ">LLADD_NTA.script");
#open(LLADD_LINKED_LIST,  ">LLADD_LINKED_LIST.script");
#open(LLADD_PAGED_LIST,   ">LLADD_PAGED_LIST.script");
#open(BDB_HASH_THREADED,  ">BDB_HASH_THREADED.script");

#open(EVERYTHING,         ">EVERYTHING.script"      );



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
