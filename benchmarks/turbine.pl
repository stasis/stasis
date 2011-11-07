#!/usr/bin/perl -w

use strict;

my @strides;
my $x = 1;
for(my $i = 0; $i < 100; $i++) {
  $strides[$i] = $i;
  $x *= 2;
}

$|=1;

foreach my $s (@strides) {
  print "$s\t";
  system qq(./turbine foo.out 3 1000 4096 $s); 
}
