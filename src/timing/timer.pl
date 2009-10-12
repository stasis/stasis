#! /usr/bin/perl -w

use strict;

my $infile;

my $CI80 = 1;
my $CI90 = 2;
my $CI95 = 3;
my $CI98 = 4;
my $CI99 = 5;
my $CI99_8 = 6;
my $CI99_9 = 7;
my $tdist = "/home/sears/bin/t-distribution.tbl";

### How tight should the CI be?
my $myCI = $CI95;

my $maxRuns = 10;
my $plusMinus = 0.05;
my $use_gettimeofday = 1;

my @ttbl;

sub parse_t_distribution {
    ## Takes the t-distribution.tbl file, and parses it into a nice, fat array.

    open (T, $tdist) || die "No T dist! $!";
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

sub runit_gettimeofday {
    my $cmd = shift;

    `rm -rf storefile.txt logfile.txt blob0_file.txt blob1_file.txt TXNAPP bdb; sync; sleep 1`;

    my $start_sec = `getTimeOfDay`;

    system($cmd);
    
    my $end_sec = `getTimeOfDay`;
    
    chomp($start_sec);
    chomp($end_sec);
    
    my $time = ($end_sec - $start_sec) / 1000.0;
    
    return $time;
    
}

sub runit_returntime {
    my $cmd = shift;

    `rm -rf storefile.txt logfile.txt blob0_file.txt blob1_file.txt TXNAPP bdb; sync; sleep 1`;

    my $time = `$cmd`;
    
    return $time;
    
}

sub runbatch {
    my $cmd = shift;

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
	if ($n > 5) {
	    if($maxRuns < $n) {
		last;
	    }
	    if($cur_range < $max_range) {
		last;
	    }
	}

	if ($use_gettimeofday) {
	    $x = runit_gettimeofday($cmd);
	} else {
	    $x = runit_returntime($cmd);
	}
	
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


while (@ARGV) {
    if ($ARGV[0] eq "-return") {
	$use_gettimeofday = 0;
	shift @ARGV;
    }

    elsif ($ARGV[0] eq "-tdist") {
	$tdist = $ARGV[1];
	print "tdist: $tdist\n";
	shift @ARGV;
	shift @ARGV;
    } elsif (-f $ARGV[0] ) {
	$infile = $ARGV[0];
	shift @ARGV;
    }

    else {
	die "unknown argument";
    }
}

parse_t_distribution;


if($infile) {
  open IN, $infile;
  while (my $cmd = <IN>) {
    if($cmd !~ /^#/) {
	print $cmd;
        runbatch($cmd);
    }
  }
  close IN;
} else {
  while (my $cmd = <>) {
    if($cmd !~ /^#/) {
	print $cmd;
        runbatch($cmd);
    }
  }
}
