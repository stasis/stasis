#!/usr/bin/perl -w
use strict;
use Stasis;

my $checking;

if(@ARGV && $ARGV[0] eq "--automated-test") {
  shift @ARGV;
  system ("rm storefile.txt logfile.txt");
  $checking = 1;
}

my %hash;

Stasis::open (\%hash);

for(my $i = 0; $i < 4; $i++) {
    $hash{$i} = $i * 10;
    if($i % 2) {
	tied(%hash)->commit();
    } else {
	tied(%hash)->abort();
    }
}
for(my $i = 0; $i < 4; $i++) {
    if($i % 2) {
	$hash{$i} == $i * 10 || die;
    } else {
	defined($hash{$i})   && die;
    }
}

print "Exiting";