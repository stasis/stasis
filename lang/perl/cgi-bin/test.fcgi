#!/usr/bin/perl -w 

use strict;

BEGIN {
    $ENV{STASIS_DIR} = '/home/sears/stasis';
    $ENV{STASIS_INLINE_DIRECTORY} = '/home/sears/stasis/www-data2';
    $ENV{PATH} = '/usr/local/bin:/usr/bin:/bin';

    chdir "$ENV{STASIS_DIR}/www-data2" || die;
    push @INC, "$ENV{STASIS_DIR}/lang/perl";
}

use threads ('stack_size' =>128*1024);;
use threads::shared;

use Stasis;

use IO::Handle;
use FCGI;
use CGI;
use CGI::Fast;

my $count = 0;
my $tot :shared = 0;
my $concurrent :shared = 0;

my $num_procs = 500; #25;


Stasis::Tinit();
my $xid = Stasis::Tbegin();
if(Stasis::TrecordType($xid, Stasis::ROOT_RID()) == Stasis::INVALID_SLOT()) {
    warn "Creating new database\n";
    Stasis::ThashCreate($xid);
}
Stasis::Tcommit($xid);
warn "Stasis bootstrap completed; accepting FastCGI requests\n";

sub event_loop {
    my $i = shift;

    my $in = new IO::Handle;
    my $out = new IO::Handle;
    my $err = new IO::Handle;
    my %env;
    my $request = FCGI::Request($in, $out, $err, \%env);

    while($request->Accept() >= 0) {
	my $q = new CGI($env{QUERY_STRING});
	my $s;
	my $tot2;

#	foreach my $i (sort keys (%env)) {
#	    warn("$i = $env{$i}\n");
#	}
	my $response;
	my $xid = Stasis::Tbegin(); 	{
	    my %h;
	    tie %h, 'Stasis::Hash', $xid, Stasis::ROOT_RID();

	    lock($tot);
	    $tot ++;

	    if(defined($q->param('get'))) {
		my $p = $q->param('get');

		if(defined($h{$p})) {
		    $response = $q->header("text/html") .  " query\n  params: ".  join ('\n', $q->param) . $h{$p};
		} else {
		    $response = $q->header("text/html") . "undefined: ->" .$p."<-";
		}
	    } elsif(defined($q->param('set'))) {
		my $p = $q->param('set');
		my @req = split ':', $p;
		@req == 2 || die;
		$h{$req[0]} = $req[1];
		$response = $q->header("text/html") . "did set |".$req[0]."| := |".$req[1]."|";
	    } elsif(defined($q->param('ls'))) {
		my $head = "<html><head><title>Page listing</title></head><body>";
		my $foot = "</body></html>";
		my @keys = sort keys %h;
		for(my $i = 0; $i < @keys; $i++) {
		    $keys[$i] 
			= "<a href='?get=$keys[$i]'>$keys[$i]</a> "
			. "(<a href='?edit=$keys[$i]'>edit</a>)";
		}
		my $body = join "<br>\n", @keys;
		$response = $q->header("text/html") . $head . $body . " query\n  params: ".  join '\n', $q->param . $foot;
	    } else {
		$response = $q->header("text/html") . "invalid " . " query\n  params: ".  join '\n', $q->param;
	    }
	    if(0) {
		$s = $tot; 
		$h{foo}++;
		$tot2 = $h{foo};

		$response = "Content-type: text/html\r\n\r\n" . "<html><head><title>foo</title><body>$count of $s ($tot2) concurrent: $concurrent<a href='http://127.0.0.1/stasis-cgi/test.fcgi?$tot2'>link</a></body></html>";
		$count ++;
	    }
	    Stasis::TsoftCommit($xid);
	    # Release lock before waiting for disk force.
	}
	Stasis::TforceCommits();
	print $out $response;
    }
}

my @thrs;
for(my $i = 0; $i < $num_procs; $i++) {
    push @thrs, threads->create(\&event_loop, $i);
}
#event_loop();

warn "Done spawning worker threads\n";

foreach my $t (@thrs) {
    $t->join();
}

Stasis::Tdeinit();

warn "Stasis cleanly shut down\n";
