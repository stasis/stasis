#!/usr/bin/perl -w

use Stasis @ARGV;
use strict;

my $prompt = "\nstasis: ";

my %s;
Stasis::open(\%s);

sub print_pad {
    my $level = shift;
    for(my $i = 0; $i < $level; $i++) {
	print ' ';
    }
}
sub print_hash {
    my $h = shift;
    my $level = shift || 0;
    my $STEP = 2;
    if (!defined $h) {
	print_pad $level;
	print '%s = ';
	$h = \%s;
    }
    print "{\n";
    foreach my $k (sort keys %$h) {
	print_pad $level;
	if(ref($$h{$k}) eq 'HASH') {
	    print " $k => ";
	    print_hash($$h{$k}, $level+$STEP);
	} else {
	    print " $k => $$h{$k}\n";
	}
    }
    print_pad $level;
    print "}\n";
    "";
}
sub p { print_hash @_; }
sub print_keys {
    my $h = shift || \%s;
    print "{ ";
    print join ", ", sort( keys %$h);
    print " }\n";
    "";
}
sub k { print_keys @_; }
sub commit { tied(%s)->commit(); }
sub c { commit; }
sub abort {tied(%s)->abort(); }
sub a { abort; }

sub help {
    print 
qq(This prompt is a perl toplevel.

The hash %s points to the hash at the root of the datastore.
The following helper functions may be useful:

 print_hash [\%hash] (or p [\%hash])  Recursively print database contents
 print_keys [\%hash] (or k [\%hash])  Print keys non-recursively
 commit (or c)                        Commit %s's current transaction
 abort  (or a)                        Abort the transaction

 help (or h)
);
    "";
}


$| = 1;
print "stasis toplevel.  'help' to get started ^D to exit.";
print $prompt;


while(my $line = <STDIN>) {
    print eval($line);
    print $prompt;
}
my $done = 0;
print "^D\n";
while(!$done) {
    print "Commit any uncommitted data [Y/n]? ";
    my $line = <STDIN>;
    if(!defined $line) {
	print "^D\n";
	$done = 1;
    } else {
	chomp $line;
	if($line eq "") { $line = "y"; }
	if($line =~ /^y/i) {
	    tied(%s)->commit();
	    $done = 1;
	} elsif($line =~ /^n/i) {
	    $done = 1;
	}
    }
}
