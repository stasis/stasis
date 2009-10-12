#!/usr/bin/perl -w

use strict;

my @in = `cd $ARGV[0]; ls *.c`;
my $out = $ARGV[1];
my $N = $ARGV[2];

chomp $out;

foreach my $line (@in) {
	chomp $line;
	$line =~ s/\..+//g;

	open OUT, "> $out/$line.script";
	print OUT "#!/usr/bin/env timer.pl\n";
	for my $i (qw(1 2 4 8 16)) {
		print OUT "./$line $i $N\n";
		`chmod +x $out/$line.script`;	
	}	
	close OUT;
}
