#!/usr/bin/perl -w
use strict;
use Stasis;

my $checking;

if(@ARGV && $ARGV[0] eq "--automated-test") {
  shift @ARGV;
  system ("rm -rf storefile.txt logfile.txt stasis_log");
  $checking = 1;
}

Stasis::Tinit();

my %hash;

tie %hash, 'Stasis::Hash';

for(my $i = 0; $i < 4; $i++) {
    $hash{$i} = $i * 10;
    if($i % 2) {
	tied(%hash)->commit();
    } else {
	tied(%hash)->abort();
    }
}
Stasis::Tdeinit();
Stasis::Tinit();

tie %hash, 'Stasis::Hash';

for(my $i = 0; $i < 4; $i++) {
    if($i % 2) {
	$hash{$i} == $i * 10 || die;
    } else {
	defined($hash{$i})   && die;
    }
}

Stasis::Tdeinit();

print "Passed";
