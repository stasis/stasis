#! /usr/bin/perl  -w 

use strict;

my $usage = qq(
Usage: $0 [--force] 'expression to calculate x value' 'expression to calculate y value'

For example:  cat FOO.out | $0 "\\\$arg[0]/\\\$arg[1]" "\\\$time"

will parse lines like this, dividing the first argument to the command
by the second.

CI mean was: 26.3276666666667   ../linearHashNTAThreaded        1   1000 1

Which would produce this output:
0.001\t26.3276666666667

);

my $ci_failed = qq(
The input contains a failed confidence interval.  (--force will allow the script to continue)
);

my $force;

if($ARGV[0] eq "--force") {
    $force = shift @ARGV;
    
}

my $eval_x = shift @ARGV;
my $eval_y = shift @ARGV;

if(!defined $eval_x || !defined $eval_y) {
    die $usage;
}

my $line;

while($line = <STDIN>) {
    if ($line =~ /failed/i) {
	if(!$force) {
	    die $ci_failed;
	} else {
	    warn $ci_failed;
	    warn "Discarding failed CI data point.\n";
	}
    } else {
	## Remove annotation from line.
	$line =~ s/^.+\:\s+//;
	
	my @tok  = split /\s+/, $line;
	my $time = shift @tok;
	my $cmd  = shift @tok;
	my @arg  = @tok;
	
	print ((eval $eval_x) . "\t" . (eval $eval_y) . "\n");
    }
}
