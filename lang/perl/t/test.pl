#!/usr/bin/perl -w
#use diagnostics -verbose;
use strict;
use Stasis;

Stasis::Tinit();
my $x=Stasis::Tbegin();
print("$x\n");

my $rid1 = Stasis::TallocScalar($x, "thequickbrown"); #14);
print "rid is $$rid1[0] $$rid1[1]\n";
defined ($rid1) || die;

Stasis::Tset($x, $rid1, "thequickbrown");

my $thequickbrown = Stasis::Tread($x, $rid1);
print "$thequickbrown\n";

$rid1 = Stasis::TallocScalar($x, 3.14159); #14);
print "rid is $$rid1[0] $$rid1[1]\n";
defined ($rid1) || die;

Stasis::Tset($x, $rid1, 3.14159);

$thequickbrown = Stasis::Tread($x, $rid1);
print "->$thequickbrown\n";

my $rid2 = Stasis::TallocScalar($x, 42);
Stasis::Tset($x, $rid2, 42);

$thequickbrown = Stasis::Tread($x, $rid2);
print "$thequickbrown\n";

$thequickbrown == 42 || die;

Stasis::Tcommit($x);

$x = Stasis::Tbegin();

Stasis::Tset($x, $rid2, 13);
$thequickbrown = Stasis::Tread($x, $rid2);
print "$thequickbrown\n";

$thequickbrown == 13 || die;

Stasis::Tabort($x);
$x = Stasis::Tbegin();
$thequickbrown = Stasis::Tread($x, $rid2);
print "$thequickbrown\n";

$thequickbrown == 42 || die;

my $rid3 = Stasis::TallocScalar($x, $rid2);
Stasis::Tset($x,$rid3,$rid2);

my $rid2cpy = Stasis::Tread($x,$rid3);

($$rid2cpy[0] == $$rid2[0] && $$rid2cpy[1]==$$rid2[1]) || die;

Stasis::Tcommit($x);

Stasis::Tdeinit(); 
