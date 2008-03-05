#!/usr/bin/perl -w
use strict;

use CGI qw/:standard/;
use Referential;

sub printResponse {
    my $t = shift;

    print "<p>$$t[0]: ".($$t[1]?'Yes':'No')."</p>\n";
    if(defined ($$t[2]) && $$t[2] =~ /\S/) {
	print "<b>Comment:</b><pre>\n".$$t[2]."</pre>";
    }
}

my $ref = new Referential("6667");

my $cgi = new CGI;

if(param('name')) {

    my $name = $ref->escape(param('name'));
    my $vote = $ref->escape(param('vote'));
    my $comments = $ref->escape(param('comments'));
    print header;
    print "\n\n<html><head><title>Survey repsonse completed</title></head></html>\n";
    print "<body><h1>Thanks!</h1>\n";
    my $tups = $ref->query("{s ($name,*,*) peeps}");

    if(defined $$tups[0][0]) {
	print "<h2>Found and deleted old response:</h2>";
	printResponse($$tups[0]);
    }

    $ref->insert("peeps $name,$vote,$comments");

    $tups = $ref->query("{s ($name,*,*) peeps}");

    if(defined $$tups[0][0]) {
	print "<h2>Recorded your response:</h2>";
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
