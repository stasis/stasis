#!/usr/bin/perl -w
#use diagnostics -verbose;
use strict;
use Stasis;

my $expected = qq(0
rid is 1 0
thequickbrown
rid is 1 1
->3.14159
42
13
42
);
my $checking = 0;
my $out = "";

sub myprint {
  my $a = shift;
  if($checking) {
    $out .= $a;
  } else {
    print $a;
  }
}

if($ARGV[0] eq "--automated-test") {
  shift @ARGV;
  system ("rm -rf storefile.txt logfile.txt stasis_log");
  $checking = 1;
}

Stasis::Tinit();
my $x=Stasis::Tbegin();
myprint("$x\n");

my $rid1 = Stasis::TallocScalar($x, "thequickbrown"); #14);
myprint "rid is $$rid1[0] $$rid1[1]\n";
defined ($rid1) || die;

Stasis::Tset($x, $rid1, "thequickbrown");

my $thequickbrown = Stasis::Tread($x, $rid1);
myprint "$thequickbrown\n";

$rid1 = Stasis::TallocScalar($x, 3.14159); #14);
myprint "rid is $$rid1[0] $$rid1[1]\n";
defined ($rid1) || die;

Stasis::Tset($x, $rid1, 3.14159);

$thequickbrown = Stasis::Tread($x, $rid1);
myprint "->$thequickbrown\n";

my $rid2 = Stasis::TallocScalar($x, 42);
Stasis::Tset($x, $rid2, 42);

$thequickbrown = Stasis::Tread($x, $rid2);
myprint "$thequickbrown\n";

$thequickbrown == 42 || die;

Stasis::Tcommit($x);

$x = Stasis::Tbegin();

Stasis::Tset($x, $rid2, 13);
$thequickbrown = Stasis::Tread($x, $rid2);
myprint "$thequickbrown\n";

$thequickbrown == 13 || die;

Stasis::Tabort($x);
$x = Stasis::Tbegin();
$thequickbrown = Stasis::Tread($x, $rid2);
myprint "$thequickbrown\n";

$thequickbrown == 42 || die;

my $rid3 = Stasis::TallocScalar($x, $rid2);
Stasis::Tset($x,$rid3,$rid2);

my $rid2cpy = Stasis::Tread($x,$rid3);

($$rid2cpy[0] == $$rid2[0] && $$rid2cpy[1]==$$rid2[1]) || die;

Stasis::Tcommit($x);

Stasis::Tdeinit(); 


if($checking) {
  $out eq $expected || die "\nFAIL: Output did not match.  Expected\n$expected\nGot\n$out";
  print "\nPASS: Produced expected output:\n$out";
}

