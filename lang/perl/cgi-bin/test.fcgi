#!/usr/bin/perl -w 

use strict;

BEGIN {
    $ENV{STASIS_DIR} = '/home/sears/stasis';
    $ENV{STASIS_INLINE_DIRECTORY} = '/home/sears/stasis/www-data2';
    $ENV{PATH} = '/usr/local/bin:/usr/bin:/bin';
#    $ENV{LANG} = 'en_US.UTF-8';
#    $ENV{SHELL} = '/bin/sh';

#    warn `env`;
    chdir "$ENV{STASIS_DIR}/www-data2" || die;
#    warn "begin succeeded";
    push @INC, "$ENV{STASIS_DIR}/lang/perl";
}

use threads;
use threads::shared;

use Stasis;

use IO::Handle;
use FCGI;

my $count = 0;
my $tot :shared = 0;
my $concurrent :shared = 0;

my $in = new IO::Handle;
my $out = new IO::Handle;
my $err = new IO::Handle;

my $num_procs = 25;


Stasis::Tinit();
my $xid = Stasis::Tbegin();
if(Stasis::TrecordType($xid, Stasis::ROOT_RID()) == Stasis::INVALID_SLOT()) {
    warn "Creating new database\n";
    Stasis::ThashCreate($xid);
}
Stasis::Tcommit($xid);
warn "Stasis bootstrap completed; accepting FastCGI requests\n";

my @thrs;
for(my $i = 0; $i < $num_procs; $i++) {
    push @thrs, threads->create(\&event_loop, $i);
}
sub event_loop {
    my $i = shift;
#    warn "Event loop $i started\n";
    my $request = FCGI::Request($in, $out, $err);
    while($request->Accept() >= 0) {
	my $s;
	my $tot2;
	{ lock $concurrent; $concurrent ++; }
	{
	    lock($tot);
	    $tot ++;
	    $s = $tot; 
	    my $xid = Stasis::Tbegin();

	    my %h;
	    tie %h, 'Stasis::Hash', $xid, Stasis::ROOT_RID();

	    $h{foo}++;

	    $tot2 = $h{foo};

	    Stasis::TsoftCommit($xid);
	}
	# Release lock before waiting for disk force.
	Stasis::TforceCommits();

	print $out "Content-type: text/html\r\n\r\n" . "$count of $s ($tot2) concurrent: $concurrent";
	$count ++;
	{ lock $concurrent; warn "concurr: $concurrent\n"; $concurrent --; }
#	print $err "Event loop $i accepted request\n";
    }
#    warn "Event loop $i done\n";
}

warn "Done spawning worker threads\n";

foreach my $t (@thrs) {
    $t->join();
}

Stasis::Tdeinit();

warn "Stasis cleanly shut down\n";
