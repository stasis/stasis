#!/usr/bin/perl -w

use strict;


## This perl script generates the input to timer.pl, which in turn
## generates the input to plotting.pl, which generates performance
## graphs. :)

open(LOGICAL_THREADS,     ">LOGICAL_THREADS.script"    );

for(my $i = 1; $i < 10; $i ++) {
  #  my $total = 500000;
    my $total = 50000;

    my $thread_count = $i;

    my $insert_count = $total / $i;

#    print LOGICAL_THREADS         "./logicalMultThreaded       $thread_count $insert_count\n";
    print LOGICAL_THREADS         "./linearHashNTAMultiReader     $thread_count $insert_count\n";

} 


for(my $i = 10; $i <= 254; $i += 10) {
#    my $total = 500000;
    my $total = 50000;
    my $thread_count = $i;

    my $insert_count = $total / $i;

#    print LOGICAL_THREADS         "./logicalMultThreaded     $thread_count $insert_count\n";
    print LOGICAL_THREADS         "./linearHashNTAMultiReader     $thread_count $insert_count\n";
} 

close(LOGICAL_THREADS);
