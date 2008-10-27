#!/usr/bin/perl -w
use strict;
use Stasis;

Stasis::Tinit();

my $xid = Stasis::Tbegin();
my $rid;

sub walk {
    my $from = shift;
    my $level  = shift || 0;
    my $to = Stasis::ThashLookup($xid, $rid, $from);
    print $from;
    $level += (length($from) + 4);

    if(defined $to) {
        my @tok = split ',', $to;
        my $first = 1;
        foreach my $f (@tok) {
            if($first) { print " => "; } else { my $n = $level; while($n--) {print " ";} }
            $first = 0;
            walk($f,$level);
        }
    } else {
        print "\n";
    }
}

if(Stasis::TrecordType($xid, Stasis::ROOT_RID()) == Stasis::INVALID_SLOT()) {
    $rid = Stasis::ThashCreate($xid);
} else {
    $rid = Stasis::ROOT_RID();
}

while(my $line = <>) {
    chomp $line;
    my @tok = split '\s+', $line;
    if($tok[0] eq "c") {
        Stasis::ThashInsert($xid, $rid, $tok[1], $tok[2]);
    } elsif($tok[0] eq "q") {
        walk $tok[1];
    }

}

Stasis::Tcommit($xid);
Stasis::Tdeinit();
