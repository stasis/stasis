#!/usr/bin/perl -w
use strict;

use Referential;

my $r = new Referential($ARGV[0]);

print "Connected...\n";

my $tups = $r->query("{s (foo,*,*,*) TABLES}");
my @tups = @{$tups};

if(@tups != 1) {
    die("Expected one foo table, found ".(@tups+0));
}

#print "test $tups[0][3]\n";
$r->insert("foo a,b,c");
$r->insert("foo b,c,d");
$tups = $r->query("foo");

foreach my $t (@{$tups}) {
    print ((join ",",@{$t})."\n");
}

$r->delete("foo b");
$tups = $r->query("foo");

foreach my $t (@{$tups}) {
    print ((join ",",@{$t})."\n");
}

$r->close();

