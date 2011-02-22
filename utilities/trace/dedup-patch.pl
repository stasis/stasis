#!/usr/bin/perl -w
use strict;

my %seen;
my $printing = 1;
while(my $line = <>) {
    if($line =~ /^\-\-\- (.+)$/) {
	my $section = $1;
	if($seen{$section}
	||($section !~ /^\/home\//)
#	||($section =~ /\/test\//)   # comment this line out for stress testing.
	){
	    $printing = 0;
	} else {
	    $printing = 1;
	}
	$seen{$section}++;
    }
    ($printing || $line =~ /^\-\-\-\-/) && print $line;
}
