#!/usr/bin/perl -w
use strict;

use CGI qw/:standard/;
use Referential;

sub printResponse {
    my $t = shift;

    print "<p>$$t[0]: ".($$t[1]?'Yes':'No')."</p><p>Interests: \n";
    my $int = $$t[2];
    my @tok = split '~', $int;
    print (join ", ", @tok);
    print("</p>\n");
    if(defined ($$t[3]) && $$t[3] =~ /\S/) {
	print "<b>Comment:</b><pre>\n".$$t[3]."</pre>";
    }

}

my $ref = new Referential("6667");

my $cgi = new CGI;

if(param('name')) {

    my $name = $ref->escape(param('name'));
    my $vote = $ref->escape(param('vote'));
    my $comments = $ref->escape(param('comments'));

    my @names = $cgi->param;
    my %interests;
    my $interests;
    my $first = 1;
    foreach my $i (@names) {
	if($i=~/^t_(.+)$/) {
	    $interests{$1}++;
	    if($first) { 
		$interests = $1;
		$first = 0;
	    } else {
		$interests .= "~$1";
	    }
	}
    }

    print header;
    print "\n\n<html><head><title>Survey repsonse completed</title></head></html>\n";
    if($vote) {
        print "<body bgcolor='#ffffee'><h1>Thanks for registering!</h1>\n";
	print "<p>Hope to see you there!</p>";
    } else {
        print "<body bgcolor='#ffffee'><h1>Thanks for your suggestions!</h1>\n";
    }
    my $tups = $ref->query("{s ($name,*,*,*) peeps}");

    if(defined $$tups[0][0]) {
	print "<h2>Deleted old response:</h2>";
	printResponse($$tups[0]);
    }

    $ref->insert("peeps $name,$vote,$interests,$comments");

    $tups = $ref->query("{s ($name,*,*,*) peeps}");

    if(defined $$tups[0][0]) {
	print "<h2>Recorded response:</h2>";
	printResponse($$tups[0]);
    } else {
	print "query failed!\n";
    }

    print "</body></html>";
} elsif(param('secret') eq 'wellkept') {
    my $format = param("format") || "html";
    my $tups = $ref->query("peeps");
    if($format ne 'html') {
	print header('text/plain')."\n\nVote tabulation\n";
	foreach my $t (@{$tups}) {
	    print "$$t[1],$$t[0]\n";
	}
	print "\n";
    } else {
	print header."\n\n<html><head><title>Responses</title></head><body><h1>Responses</h1>";
	foreach my $t (@{$tups}) {
	    printResponse($t);
	}
	print "</body></html>";
    }
} else {
    print header."\n\n".qq(
<html><head><title>Survey</title></head><body>
<h1>Survey</h1>
<form>
Name: <input type='text' name='name'/>
<br>
<p>Which topics are you interested in?</p>
<input type="checkbox" name="t_tpsReports">
TPS Reports
</input>
<br>
<input type="checkbox" name="t_efficiency">
Consultants
</input>
<br>
<input type="checkbox" name="t_family">
Family
</input>
<br>
<input type="checkbox" name="t_academic">
Academic
</input>
<br>
<input type="checkbox" name="t_balance">
Balance
</input>
<br>
<p>Will you attend?</p>
<input type="radio" name="vote" value="1">Yes</input>
<br>
<input type="radio" name="vote" value="0">No</input>
<p>Comments:</p>
<textarea name="comments" cols="72" rows="10">
</textarea><br>
<input type="submit" value="Submit">
<br>
</form>
</body>
</html>);
}
