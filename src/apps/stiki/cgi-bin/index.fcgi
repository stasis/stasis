#!/usr/bin/perl -w
BEGIN {
    $ENV{STASIS_DIR}="/home/sears/stasis";
}
use strict;
use Stasis;
use Carp;
use CGI::Fast;

sub bootstrap {
    my $xid = Stasis::Tbegin();
    my $rid;
    if(Stasis::TrecordType($xid, Stasis::ROOT_RID())
			   == Stasis::INVALID_SLOT()) {
	$rid = Stasis::ThashCreate($xid);
    } else {
	$rid = Stasis::ROOT_RID();
    }
    Stasis::Tcommit($xid);
    return $rid;
}

Stasis::Tinit();
my $hash = bootstrap();

while(my $q = new CGI::Fast()) {
    my $pagename = $q->param('set') || $q->param('get') || 'Main';

    if($q->param('set')) {
	my $xid = Stasis::Tbegin();
	Stasis::ThashInsert(($xid, $hash, $pagename, $q->param('content')));
	print $q->redirect("/stiki/$pagename");
	Stasis::Tcommit($xid);
    } else {
	my $template = `cat creole-test.html`;
	my $content = Stasis::ThashLookup(-1, $hash, $pagename) ||
	    "= $pagename\nClick edit (on the right) to create this new page);";
	$template =~ s/__TITLE__/$pagename/g;
	$template =~ s/__CONTENT__/$content/g;
	print $q->header . $template;
    }
}

Stasis::Tdeinit();

