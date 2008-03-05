#!/usr/bin/perl -w

package Referential;

use strict;

use Expect;
use IO::Socket::INET;
use URI::Escape;

sub new {
    my $class = shift;

    my %self;

    $self{port} = shift;
    $self{host} = shift || "localhost";
    $self{log} = 1;
    $self{socket} = IO::Socket::INET->new(PeerAddr => $self{host},
					   PeerPort => $self{port},
					   Proto    => "tcp",
					   Type     => SOCK_STREAM)
	or die "Couldn't connect to $self{host}:$self{port} : $!\n";

    $self{exp} = Expect->exp_init($self{socket});
    $self{exp}->raw_pty(1);

    my @prompt;
    push @prompt, '-re', '.*\>\s*';

    my $patidx = $self{exp}->expect(10, @prompt);

    if(!defined($patidx)) {
	die "Didn't get prompt\n";
    }
    return bless \%self, $class;
}

sub escape {
    my $self = shift;
    my $s = shift;
    return uri_escape($s, "^A-Za-z0-9\-_.!~'");
}
sub unescape {
    my $self = shift;
    my $s = shift;
    return uri_unescape($s);
}

sub query {
    my $self = shift;
    my $q = shift;

    #warn "REFERENTIAL QUERY: query $q\n";
    $self->{exp}->send("query $q\n");
    my @pat;

    my @tpls;
    push @tpls, '-re', '^[A-Za-z0-9\-_.!~\'%.,\ ]+[^\r\n]';
    push @tpls, '-re', '.*\>\s*';
    my @tups;
    while(my $idx = $self->{exp}->expect(10, @tpls)) {
	if($idx==1) {
	    my $match =  $self->{exp}->exp_match();
	    chomp $match;
	    my @tok = split ",", $match;
	    my @tok2;
	    for(my $i = 0; $i < @tok; $i++) {
		push @tok2, $self->unescape($tok[$i]);
	    }
	    push @tups, \@tok2;
	} elsif($idx == 2) {
	    last;
	}
    }
    return \@tups;
}

sub _cmd {
    my $self = shift;
    my $q = shift;
    my $m = shift;

    if($self->{log}) {
	my $d = `date`;
	chomp $d;
	warn "\n[$d] REFERENTIAL_CMD: $m $q\n";
    }

    $self->{exp}->send("$m $q\n");
    my @pats;
    push @pats, '-re', '.*\>\s*';
    my $idx = $self->{exp}->expect(10, @pats);
    if($idx != 1) {
	warn "No prompt?!?\n";
	return 0;
    }
    return 1;
}

sub insert {
    my $self = shift;
    my $q = shift;
    return _cmd($self, $q, "insert");
}
sub delete {
    my $self = shift;
    my $q = shift;
    return _cmd($self, $q, "delete");
}

sub close {
    my $self = shift;
    $self->{exp}->send("!exit\n");
    $self->{exp}->soft_close();
}
1;
