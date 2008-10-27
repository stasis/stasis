#!/usr/bin/perl -w
use strict;

sub walk {
    my $mid =shift;
    my $low = shift;
    my $hi = shift;
    my $depth = (shift)-1;

    $depth || return;

    my $lmid = $mid + ($low - $mid) / 2;
    my $rmid = $mid + ($hi  - $mid) / 2;

    $lmid < $rmid || die;

    print "c $mid $lmid,$rmid\n";

    walk($lmid, $low, $mid, $depth);
    walk($rmid, $mid, $hi, $depth);
}

my $depth = $ARGV[0];

walk 0, -16, 16, $depth;

