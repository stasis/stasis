#!/usr/local/bin/perl -w
use strict;

$| = 1;

#for my $i (qw(100 500 1000 1500)) {  # ram size (mb)
#    for my $j (qw(10000 1000 100 10)) {  # tuple size (b)
#        system("../build/benchmarks/redBlackMemoryOverhead $i $j");
#    }
#}

for(my $i = 0; $i < 1000; $i++) {
     my $i = rand(4000);
     my $j = rand(10000) + 10;
     system("../build/benchmarks/redBlackMemoryOverhead $i $j");
}
