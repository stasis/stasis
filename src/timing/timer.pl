#! /usr/bin/perl -w

use strict;


my $CI80 = 1;
my $CI90 = 2;
my $CI95 = 3;
my $CI98 = 4;
my $CI99 = 5;
my $CI99_8 = 6;
my $CI99_9 = 7;

### How tight should the CI be?
my $myCI = $CI95;

my $maxRuns = 10;
my $plusMinus = 0.05;


my @ttbl;

sub parse_t_distribution {
    ## Takes the t-distribution.tbl file, and parses it into a nice, fat array.

    open (T, "/home/sears/bin/t-distribution.tbl") || die "No T dist! $!";
    my $i =0;
    while(my $line = <T>) {
	my @tok = split /\s+/, $line;
	pop @tok;  ## Remove trailing n.

	while($i < $tok[0]) {
	    push @ttbl, \@tok;
	    $i++;
#	    print("$i " . join ("-", @tok) . "\n");
	}
    }

}

sub runit {
    my $cmd = shift;

    `rm -rf storefile.txt logfile.txt blob0_file.txt blob1_file.txt TXNAPP; sync; sleep 1`;

    my $start_sec = `getTimeOfDay`;

    system($cmd);
    
    my $end_sec = `getTimeOfDay`;
    
    chomp($start_sec);
    chomp($end_sec);
    
    my $time = ($end_sec - $start_sec) / 1000.0;
    
    return $time;
    
}

parse_t_distribution;


while (my $cmd = <>) {

    my $sum_x = 0;
    my $sum_x_squared = 0;
    my $n = 0;
    
    my $variance = 100000;
    my $mean = 0;
    
    my $x = 0;
    my $s = 10;

   
    my $max_range = 0;
    my $cur_range = 1;
    while( 1 ) {
	if ($n > 2) {
	    if($maxRuns < $n) {
		last;
	    }
	    if($cur_range < $max_range) {
		last;
	    }
	}
	$x = runit($cmd);
	
	$n++;
	$sum_x += $x;
	$sum_x_squared += ($x * $x);
	
	$variance = ($sum_x_squared/$n) - ($sum_x/$n)*($sum_x/$n);
	$mean = $sum_x / $n;
	
	my $s = sqrt($variance/$n);

	my $sigma = sqrt($variance);

	print("Time: $x  s: $s Mean: $mean Stdev: $sigma\n");

	$cur_range = $s * $ttbl[$n-1][$myCI];
	$max_range = $plusMinus * $mean;

    }
    if($cur_range > $max_range) {
	printf("CI FAILED. mean was: $mean\t$cmd\n");
    } else {
	printf("CI mean was: $mean\t$cmd\n");
    }
}


