#!/usr/bin/perl -w 
use strict;

while(my $line = <>) {
  $line =~ s/\@CMAKE_......_DIR\@\///g;
  print $line;
}
