#!/usr/bin/perl -w

use strict;

my @strides;
my $x = 1;
for(my $i = 0; $i < 400; $i++) {
  $strides[$i] = $i;
  $x *= 2;
}

$|=1;

foreach my $s (@strides) {
  print "$s\t";
  system qq(./stride foo.out 3 500 4096 $s); 
}
