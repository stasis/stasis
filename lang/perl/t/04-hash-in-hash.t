#!/usr/bin/perl -w
use strict;
use Stasis;

my $checking;

if(@ARGV && $ARGV[0] eq "--automated-test") {
  shift @ARGV;
  system ("rm storefile.txt logfile.txt");
  $checking = 1;
}
Stasis::Tinit();

my $xid = Stasis::Tbegin();
my $rid;

print("xid = $xid");

my %hash;
$| = 1;

if(Stasis::TrecordType($xid, Stasis::ROOT_RID()) == Stasis::INVALID_SLOT()) {
    $rid = Stasis::ThashCreate($xid);

    my $rid2 = Stasis::ThashCreate($xid);
    my $rid3 = Stasis::ThashCreate($xid);

    print("Created hashes\n");

    tie(%hash, 'Stasis::Hash', $xid, $rid);
    
    my %blessedhash;
    tie(%blessedhash, 'Stasis::Hash', $xid, $rid2);

    print("Tied hashes\n");

    $hash{blessed} = \%blessedhash;

    print("Inserted blessed\n");

    $hash{blessed}{foo} = "bar";
    $hash{blessed}{bar} = "baz";

    die unless $hash{blessed}{foo} eq "bar";
    die unless $hash{blessed}{bar} eq "baz";
    
    print("Inserted blessed keys\n");

    $hash{explicit} = $rid3;

    print("Inserted explicit\n");

    $hash{explicit}{foo} = "bar";
    $hash{explicit}{bar} = "baz";

    die unless $hash{explicit}{foo} eq "bar";
    die unless $hash{explicit}{bar} eq "baz";

    print("Inserted explicit keys\n");

    print("inserting the hash\n");

    my %h;
    $hash{implicit} = \%h;

    print("Inserted implicit\n");

    print "doing the set!\n";
    $hash{implicit}{foo} = "bar";
    print "the set happened!\n";
    $hash{implicit}{bar} = "baz";
    print "the next set happened!\n";
    
    print("Inserted implicitkeys\n");

    die unless $hash{implicit}{foo} eq "bar";
    die unless $hash{implicit}{bar} eq "baz";

    print ("now for auto\n");
    $hash{auto}{foo} = "bar";
    $hash{auto}{bar} = "baz";
    die unless $hash{auto}{foo} eq "bar";
    die unless $hash{auto}{bar} eq "baz";
    print ("done");

    Stasis::Tcommit($xid);

} else {

    $rid = Stasis::ROOT_RID();

    tie(%hash, 'Stasis::Hash', $xid, $rid);

    print "blessed\n";
    die unless $hash{blessed}{foo} eq "bar";
    die unless $hash{blessed}{bar} eq "baz";
    print "explicit\n";
    die unless $hash{explicit}{foo} eq "bar";
    die unless $hash{explicit}{bar} eq "baz";
    print "implicit\n";
    die unless $hash{implicit}{foo} eq "bar";
    die unless $hash{implicit}{bar} eq "baz";
    print "auto\n";
    die unless $hash{auto}{foo} eq "bar";
    die unless $hash{auto}{bar} eq "baz";

}

