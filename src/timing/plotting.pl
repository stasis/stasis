#! /usr/bin/perl -w 

use strict;


my $R_PREAMBLE = qq{
    postscript("__PLOT_NAME__.ps", width = 9.75, height = 6.375, 
	       horizontal = FALSE, onefile = FALSE, paper = "special",
	       family = "ComputerModern")
    };

my $R_READ = qq{
    __NAME__<-read.table("__FILE__",header=F,sep="\\t")
    };

my $R_FIRST_PLOT = qq{
    plot(__NAME__\$V1, __NAME__\$V2, type="b",
	 xlim=c(0,max(__NAME_X_LIST__)),
	 ylim=c(0, max(__NAME_Y_LIST__)), 
	 xlab="__XLAB__", ylab="__YLAB__", 
         main="__TITLE__", pch=1, col=1)
     };
    
my $R_NEXT_PLOT = qq{
    lines(__NAME__\$V1, __NAME__\$V2, type="b", pch=__N__+1, col=__N__+1)
    };

my $R_LEGEND = qq{
    legend((0 * max(__NAME_X_LIST__)), max(__NAME_Y_LIST__), legend=c(__NAME_LABEL_LIST__), bty="n", pch=c(1:__COUNT__), col=c(1:__COUNT__))
};

my $R_FINISH = qq{
    dev.off()
    };

sub replaceAll {
    my $cmd     = shift;
    my $arglist_ref = shift;
    my %arglist = %{$arglist_ref};

    foreach my $i (keys %arglist) {
	$cmd =~ s/$i/$arglist{$i}/g;
    }
    return $cmd;
}

sub getConfig {
    my $basename = shift;
    my $key = shift;
    my $value = `grep ^$key: $basename.def`;
    $value =~ s/^$key\:\s+//;
    chomp $value;
    return $value
}

sub filesToLabels {
    my $array_ref = shift;

    my @array = @{$array_ref};

    for(my $i = 0; $i < @array; $i++) {
	chomp $array[$i];
	$array[$i] =~ s/^[^\-]+\-//;
	$array[$i] =~ s/\.dat$//;
    }

    my $ret = '"' . join ('", "', @array). '"';

    return $ret;
}

sub labelsToVars {
    my $i = shift;

    $i =~ s/[^A-Za-z0-9\"\,]//g;

    $i =~ s/\,/, /g;

    return $i;
}

sub varsToXList {
    my $i = shift;
    
    $i =~ s/\"\,/\$V1\",/g;
    $i =~ s/\"$/\$V1\"/;

    $i =~ s/\"//g;

    return $i;
}
sub varsToYList {
    my $i = shift;
    
    $i =~ s/\"\,/\$V2\",/g;
    $i =~ s/\"$/\$V2\"/;
    $i =~ s/\"//g;

    return $i;
}

my %vals;

my $basename = $ARGV[0];

my @files = `ls $basename-*.dat`;

for(my $i =0; $i < @files; $i++) { 
    chomp $files[$i];
}


$vals{__PLOT_NAME__} = $basename;
$vals{__XLAB__} = getConfig($basename, "X-Label");
$vals{__YLAB__} = getConfig($basename, "Y-Label");
$vals{__TITLE__} = getConfig($basename, "Title");
$vals{__NAME_LABEL_LIST__} = filesToLabels(\@files);

my $vars = labelsToVars($vals{__NAME_LABEL_LIST__});

$vals{__NAME_X_LIST__} = varsToXList($vars);
$vals{__NAME_Y_LIST__} = varsToYList($vars);
$vals{__COUNT__} = @files;

my @names = split /[\"\,\s]+/, $vars;

if($names[0] =~ /^\s*$/) {
    shift @names;
}


$vals{__FILE__} = $files[0];
$vals{__NAME__} = $names[0];
$vals{__N__} = 0;

print replaceAll($R_PREAMBLE, \%vals);
print replaceAll($R_READ, \%vals);

for(my $n = 1 ; $n < @names; $n++) {
    $vals{__FILE__} = $files[$n];
    $vals{__NAME__} = $names[$n];
    print replaceAll($R_READ, \%vals);
}

$vals{__FILE__} = $files[0];
$vals{__NAME__} = $names[0];


print replaceAll($R_FIRST_PLOT, \%vals);

for(my $n = 1 ; $n < @names; $n++) {
    $vals{__FILE__} = $files[$n];
    $vals{__NAME__} = $names[$n];
    $vals{__N__} = $n;

    print replaceAll($R_NEXT_PLOT, \%vals);

}

print replaceAll($R_LEGEND, \%vals);
print replaceAll($R_FINISH, \%vals);

