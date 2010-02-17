#!/usr/bin/perl -w

use strict;

BEGIN {
      $ENV{STASIS_INLINE_DIRECTORY} = '/home/sears/stasis/www-data2';
      $ENV{PATH} = '/usr/local/bin:/usr/bin:/bin';
      chdir '/home/sears/stasis/www-site' || die;
      push @INC, '/home/sears/stasis/lang/perl';
}

use threads ('stack_size' => 128*1024);
use threads::shared;

use Stasis;

use IO::Handle;
use FCGI;
use CGI;

my $num_procs = 25;

Stasis::Tinit();

my @thrs;
for(my $i = 0; $i < $num_procs; $i++) {
    push @thrs, threads->create(\&event_loop);
}

warn "Done spawning worker threads\n";

foreach my $t (@thrs) {
    $t->join();
}

warn "Stasis cleanly shut down\n";

exit;

sub default_page {
    my $q = shift;
    my $page = shift;
    return $q->p("$page does not exist");
}

sub edit_link {
    my $q = shift;
    my $page = shift;
    my $url = $q->url(-relative=>1);
    return $q->p("<a href='$url?mode=edit&page=$page'>Click here to edit the page</a>");
}

sub event_loop {
#    my $in = new IO::Handle;
#    my $out = new IO::Handle;
#    my $err = new IO::Handle;
    *STDIN = new IO::Handle;
    *STDOUT = new IO::Handle;
    *STDERR = new IO::Handle;
    my %E;
    %ENV = %E;
#    my %env;
    my %h;

 #   my $request = FCGI::Request($in, $out, $err, \%env);
   my $request = FCGI::Request();

    Stasis::open(\%h);

    my $contents;

    while($request->Accept() >= 0) {
	my $q = new CGI(); #$env{QUERY_STRING});

	my $response;

	my $mode = $q->param('mode') || 'view';

	my $page = $q->param('page') || '/';

	if($mode eq 'view') {
	    $contents = ($h{$page} || default_page($q,$page)) . edit_link($q,$page);
	    
	} elsif($mode eq 'edit') {
	    my $page = $q->param('page') || '/';
	    my $a = $q->url(-relative=>1);
	    
	    $q->param(-name=>'mode',-value=>'set');
	    $q->param(-name=>'contents',-value=>$h{$page});
	    $contents = $q->start_form()
		. $q->hidden('mode')
		. $q->p($q->textarea(-name=>'contents',-cols=>80,-rows=>18))
		. $q->submit("Save $page")
		. $q->end_form();
	} elsif($mode eq 'set') {
	    $h{$page} = $q->param('contents');
	    $contents = $h{$page} . edit_link($q,$page);
	} else {
	    $contents = "unknown mode $mode";
	}
	$response = $q->header()
	    . $q->start_html($page)
 	    . $q->h1($page)
	    . $contents
	    . $q->end_html;

	tied(%h)->commit();
#	print $out $response;
	print $response;
    }
}
