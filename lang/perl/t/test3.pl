#!/usr/bin/perl -w
use strict;
use Stasis;

my $expected =
    "a1 b2 c3 d4 e5 f6 g7 h8 i9 j10 \n" .
    "k1 l2 m3 n4 o5 p6 q7 r8 s9 t10 \n"; 

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
  system ("rm storefile.txt logfile.txt");
  $checking = 1;
}

Stasis::Tinit();
my $xid = Stasis::Tbegin();
my $rid = Stasis::ThashCreate($xid);
Stasis::Tcommit($xid);
$xid = Stasis::Tbegin();
my %h;
tie %h, 'Stasis::Hash', $xid, $rid;

my $i = 0;
foreach my $x (qw(a b c d e f g h i j)) {
    $i++;
    $h{$x}=$i;
}
my @keys = sort keys %h;

for my $k (@keys) {
    myprint "$k$h{$k} ";
}
myprint "\n";

Stasis::Tabort($xid);

$xid = Stasis::Tbegin();
tie %h, 'Stasis::Hash', $xid, $rid;

my $i = 0;
foreach my $x (qw(k l m n o p q r s t)) {
    $i++;
    $h{$x}=$i;
}

@keys = sort keys %h;

for my $k (@keys) {
    # does not output the aborted pairs.
    myprint "$k$h{$k} ";
}
myprint "\n";
Stasis::Tcommit($xid);
Stasis::Tdeinit();

if($checking) {
  $out eq $expected || die "\nFAIL: Output did not match.  Expected\n{$expected}\nGot\n{$out}\n";
  print "\nPASS: Produced expected output:\n$out";
}
