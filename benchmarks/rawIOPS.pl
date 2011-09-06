#!/usr/bin/perl -w

use strict;

## Driver program for rawIOPS.c

my $usage = "$0 filename {fix_threads|fix_size} slice_number [page_size] [op_count] [start_off]\n";

my @thread_counts;
my @partition_sizes;
my $c = 1;

my $ITER = 3;

for(my $i = 0; $i < 11; $i++) {
    $thread_counts[$i] = $c;
    $c *= 2;
}
$c = 1;
for(my $i = 0; $i < 9; $i++) {
    $partition_sizes[$i] = $c;
    $c *= 2;
}
print ("Slice number:    " . join("\t", (0..10))."\n");
print ("Thread counts:   " . join("\t", @thread_counts) . "\n");
print ("Partition sizes: " . join("GB\t", @partition_sizes) . "GB\n");

my $exe          = './rawIOPS';
my $device       = $ARGV[0] || die $usage;
my $mode         = $ARGV[1] || die $usage;
my $slice_number = $ARGV[2];
defined($slice_number) || die $usage;
my $page_size    = $ARGV[3] || 512;
my $opcount      = $ARGV[4] || 5000;
my $start_off    = $ARGV[5] || 0;

sub run_cmd {
    my $num_threads = shift || die;
    my $size = shift || die;
    my $end_off = $size + $start_off;
    my @a = `$exe $device $page_size $num_threads, $opcount, $start_off, $end_off`;
    @a==1 || die "Could not understand output" . join("",@a);
    return $a[0];
}
sub parse_iops {
    my $a = shift;
    $a =~ /\s+([\S]+)\s+IOPS/||die "Couldnt parse $a\n";
    return $1;
}
my @results;

for(my $iter = 0; $iter < $ITER; $iter++) {
    if($mode eq 'fix_threads') {
	for(my $i = 0; $i < @partition_sizes; $i++) {
	    $results[$iter][$i] = run_cmd($thread_counts[$slice_number],
				   $partition_sizes[$i]*1024);
	    print $results[$iter][$i];
	}
    } elsif($mode eq 'fix_size') {
	for(my $i = 0; $i < @thread_counts; $i++) {
	    $results[$iter][$i] = run_cmd($thread_counts[$i],
				   $partition_sizes[$slice_number]*1024);
	    print $results[$iter][$i];
	}
    } else {
	die "Unknown mode.\n$usage";
    }
}

my @mean_iops;
if($mode eq 'fix_threads') {
    print "Ran with $thread_counts[$slice_number] threads:\n";
} else {
    print "Ran with $partition_sizes[$slice_number]GB partition:\n";
}
for(my $i = 0; $i < @{$results[0]}; $i++) {
    for(my $iter = 0; $iter < $ITER; $iter++) {
	$mean_iops[$i] += parse_iops($results[$iter][$i]);
    }
    $mean_iops[$i] /= $ITER;
}

if($mode eq 'fix_threads') {
    print("Partition-size(GB)\tIOPS\n");
    for(my $i = 0; $i < @mean_iops; $i++) {
	print("$partition_sizes[$i]\t$mean_iops[$i]\n");
    }
} else {
    print("Thread-count\tIOPS\n");
    for(my $i = 0; $i < @mean_iops; $i++) {
	print("$thread_counts[$i]\t$mean_iops[$i]\n");
    }
}

