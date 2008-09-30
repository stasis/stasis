#!/usr/bin/perl -w
use strict;

my @winners;
my @losers;

#
#   query tests
#
my $fail = 0;

push @winners, 'query foo';
push @winners, 'query foo1';
push @winners, 'query {s ($1=2,$2="asd",$3=x) foo}';
push @winners, 'query {p ($1,$2) foo}';
push @winners, 'query {j ($1=$4,"foo"=$3,$5=x,3=$6) foo bar}';
push @winners, 'query {select ($1=2,$2="asd",$3=x) foo}';
push @winners, 'query {project ($1,$2) foo}';
push @winners, 'query {join ($1=$4,"foo"=$3,$5=x,3=$6) foo bar}';
push @winners, 'query {
                        j
                          ($1=$4,"foo"=$3,$5=x,3=$6)
                          {
                            s ($0="a",$2=1) foo
                          }
                          bar
                      }';

push @winners, 'query {select ($3 = "blue")|($3 = 10) questions_three}';
push @winners, 'query {project ($3){select($1="Galahad")questions_three}}';

push @winners, 'query {join ($MigratorID=$MigratorID) Migrators *}';
push @winners, 'query {join ($MigratorID) Migrators { join ($BirdId) Birds *} | Fruits }';
push @winners, 'query {join ($Type) * *}';
push @winners, 'query {project ($titl) {select ($author="sears") {join (docid) * *}}}';

push @losers, 'query 1';
push @losers, 'query foo 1';
push @losers, 'query {p (a,2) foo}';
push @losers, 'query {p (1,2) foo}';
push @losers, 'query {p ("a",$2) foo}';
push @losers, 'query {p ($1,"a") foo}';
push @losers, 'query {
                        j
                          ($1=$4,"foo"=$3,$5=x,3=$6)
                          {
                            s ("a",*,1) foo

                          bar
                      }';
#
#   insert tests
#

push @winners, 'insert foo (a,"1",1)';
push @winners, 'insert foo (a,"`1234567890-=~!@#$%^&*()+qwertyuiop[]\QWERTYUIOP{}|asdfghjkl;\\\'ASDFGHJKL:'."\n".'zxcvbnm,./ZXCVBNM<>?",1)';

push @winners, 'delete foo (a,"1",1)';
push @winners, 'delete foo (a,"`1234567890-=~!@#$%^&*()+qwertyuiop[]\QWERTYUIOP{}|asdfghjkl;\\\'ASDFGHJKL:'."\n".'zxcvbnm,./ZXCVBNM<>?",1)';

push @losers, 'delete foo (string,"1",1)';
push @losers, 'delete foo (int,"`1234567890-=~!@#$%^&*()+qwertyuiop[]\QWERTYUIOP{}|asdfghjkl;\\\'ASDFGHJKL:'."\n".'zxcvbnm,./ZXCVBNM<>?",1)';

push @winners, 'create foo (int, string, int)';
push @losers,  'create foo';
push @losers,  'create foo (int, string, "char")';

push @winners, 'create foo (int, string)|(string, int)';



foreach my $q (@winners) {
    open TST, "| ./parser";
    print TST $q;
    if(!close TST) {
	# command failed:
	print "unexpected fail: $q\n";
	$fail ++;
    }
}
foreach my $q (@losers) {
    open TST, "| ./parser";
    print TST $q;
    if(close TST) {
	# command succeeded:
	print "unexpected pass: $q\n";
	$fail ++;
    }
}
if($fail) {
    exit(1);
}
