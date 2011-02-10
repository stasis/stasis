#!/usr/bin/perl -w
use strict;

if(!defined($ARGV[0])) { $ARGV[0] = 'garbage'; }

my $enable;

if($ARGV[0] eq '--disable') {
    $enable = 0;
} elsif($ARGV[0] eq '--enable') {
    $enable = 1;
} else {
    die "usage: $0 [--enable|--disable]\n";
}
 #Tupdate TupdateWithPage TupdateStr TreorderableUpdate TwritebackUpdate TreorderableWritebackUpdate
 #Tcommit TsoftCommit TforceCommits Tabort Tbegin  Tforget Tprepare  Trevive
	#	 TnestedTopAction TbeginNestedTopAction TendNestedTopAction
my @funcs = qw ( Tinit 
		 Tread TreadWithpage TreadRaw TreadStr 
		 Tdeinit TuncleanShutdown 
		 loadPage loadPageOfType loadUninitializedPage loadPageForOperation releasePage getCachedPage
		 stasis_log_force stasis_log_begin_transaction stasis_log_prepare_transaction stasis_log_commit_transaction 
		 stasis_log_abort_transaction stasis_log_end_aborted_transaction stasis_log_write_update stasis_log_write_clr
		 stasis_log_write_dummy_clr stasis_log_begin_nta stasis_log_end_nta
		);

my %opts;

my @opts  = qw (CMAKE_EXE_LINKER_FLAGS
                CMAKE_SHARED_LINKER_FLAGS
                CMAKE_MODULE_LINKER_FLAGS);

foreach my $opt (@opts) {
    $opts{$opt} = 1;
}

my $prefix = ' -Wl,--wrap,';

my $val = $prefix . join $prefix, @funcs;

if(!-f 'CMakeCache.txt') { die 'CMakeCache not found\n'; }

my @file = `cat CMakeCache.txt`;

my $hits = 0;

open OUT, ">CMakeCache.txt.toggle~";

foreach my $line (@file) {
    chomp $line;
    if($line =~ /^\s*([^\:]+)\:([^\=]+)\=(.*)$/) {
        my ($k,$t,$v) = ($1,$2,$3);

        if($opts{$k}) {
            $hits++;
            if($enable) {
                print OUT "$k:$t=$val\n";
            } else {
                print OUT "$k:$t=\n";
            }
        } else {
            print OUT "$line\n";
        }
    } else {
        print OUT "$line\n";
    }
}
my $continue = 1;
if($hits != @opts+0) {
    warn "replaced $hits lines, but expected " . (@opts+0) . ".\n";
    $continue = 0;
}
close OUT;
$continue || die "Hit trobule. Bailing out.\n";

`mv CMakeCache.txt.toggle~ CMakeCache.txt`;
`make clean`;
