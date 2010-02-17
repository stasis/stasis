#!/usr/bin/perl -w
use strict;
use Stasis;

my $checking;

if(@ARGV && $ARGV[0] eq "--automated-test") {
  shift @ARGV;
  system ("rm storefile.txt logfile.txt");
  $checking = 1;
}

my %h;
Stasis::open(\%h);

Stasis::Tbegin();

my %i;

$h{foo} = \%i;

tied(%h)->commit();

$h{foo}{bar} = "x";

my $i = $h{foo};

tied(%h)->commit();
$$i{baz} = "y";
$$i{bat} = "z";

Stasis::Tdeinit();

Stasis::open(\%h);

Stasis::Tbegin();
Stasis::Tbegin();

$h{foo}{baz} = "bat";
$h{foo}{bar} == "x" || die;

