#!/usr/bin/perl -w
use strict;

## sample invocation:
#perl tpc-ch-generator.pl --sf=30 | sort -S 2G -T /mnt/sdb1/tmp -t, -k9,9n -k7,7n -k8,8n -k2,2n -k1,1 -k3,3n -k4,4n -k5,5n -k6,6n -k10,10n -k11,11n -k12,12n > /home/sears/workload-vldb/sf30.sort


sub pickCountry {
    my $a = shift;
    my @big_fish = @{$a};

    my $p = rand(100);
    for(my $i = 0; $i < @big_fish; $i++) {
	if($p < $big_fish[$i]) {
	    return $i;
	}
	$p -= $big_fish[$i];
    }
    return int(rand(200-@big_fish)) + (@big_fish);
}


sub addtime {
    my $day = shift;
    my $week = shift;
    my $year = shift;
    my $delta = shift;

    my $delday = $day + $delta;
    my $delweek = $week;
    my $delyear = $year;

    while($delday > 6) {
	$delday-=7;
	$delweek++;
    }
    while($delweek > 51) {
	$delweek-=52;
	$delyear++;
    }
    return ($delday, $delweek, $delyear);
}

# Proportions based on canada import/export according to WTO, Oct 2007 report)
my @big_supp_fish = qw (54.9 12.3 8.7 4.0 3.9);
sub pickSupplierCountry {
    return pickCountry(\@big_supp_fish);
}
my @big_cust_fish = qw (81.6 6.6 2.1 1.7 1.0);
sub pickCustomerCountry {
    return pickCountry(\@big_cust_fish);
}
@ARGV==1||die;

sub pickYear {
    my $max_year = 10000;
    my $p = rand(100);
    if($p < 99) {
	# Pick w/in 1995-2005
	return 1995 + int(rand(10)); #(start of 1995-end of 2004)
    }
    my $year = int(rand($max_year - 10));
    if($year >= 1995) {
	$year += 10;
    }
    return $year;
}

# Magic incantations:
# ./database-generator.pl --test-supp-rng | sort -k1,1n | uniq -c | tac
# ./database-generator.pl --test-cust-rng | sort -k1,1n | uniq -c | tac
# ./database-generator.pl --test-year-rng | sort -k1,1n | uniq -c | tac
# ./database-generator.pl --test-week-rng | sort -k1,1n | uniq -c | tac

sub pickWeek {
    my $p = rand(100);
    if($p < 20) {
	# christmas
	if(rand(1) < 0.5) {
	    return 50;
	}
	return 51;
    }
    $p -= 20;
    if($p < 20) {
	# mother's day
	if(rand(1) < 0.5) {
	    return 18;
	}
	return 19;
    }
    my $week = int(rand(52-4));
    if($week > 17) {
	$week += 2;
    }
    if($week > 49) {
	warn("Invalid week!!!");
    }
    return $week;
}
sub pickDay {
    my $p = rand(100);
    if($p < 99) {
	return int(rand(5));
    } else {
	return 5+int(rand(2));
    }
}
sub choosePart {
    my $SF = shift || die "Expected scale factor";

    # TPC-H calls for SF * 200,000 for part range, but has a concept
    # of part suppliers, w/ 4 suppliers per part.  We treat
    # (part_id,supplier_id) as a single key here.
    my $p = int(rand($SF * 800000)); 
    return $p;
}
sub pricePart {
    my $partkey = shift;
    ## Mult tpc-h formula by 100 since we don't support floating point columuns
    return 100*((90000 + (($partkey/10) % 20001) + 100 * ($partkey % 1000))/100);
}
my %partSourceCountry;
sub suppliercountryPart {
    my $p = shift;
    if(!defined($partSourceCountry{$p})) {
	$partSourceCountry{$p} = pickSupplierCountry();
    }
    return $partSourceCountry{$p}
}
sub chooseQuantity {
    return int(rand(50))+1;
}
my $nextOrderNum = 0;
sub chooseOrderNum {
    my $n = $nextOrderNum;
    $nextOrderNum += 4;
    return $n;
}
sub choosePartcount {
    return 1 + int(rand(7));
}
my $nextlineNum = 0;
sub chooseLineitemNum {
    my $n = $nextlineNum;
    $nextlineNum += 4;
    return $n;
}
my $SF = 1;
sub extendedpricePriceQuantity {
    my $p = shift;
    my $q = shift;
    return $p * $q;
}

if($ARGV[0] eq "--test-supp-rng") {
    for(my $i = 0; $i < 10000; $i++) {
	print (pickSupplierCountry()."\n");
    }
} elsif($ARGV[0] eq "--test-cust-rng") {
    for(my $i = 0; $i < 10000; $i++) {
	print (pickCustomerCountry()."\n");
    }
} elsif($ARGV[0] eq "--test-year-rng") {
    for(my $i = 0; $i < 1000000; $i++) {
	print (pickYear()."\n");
    }
} elsif($ARGV[0] eq "--test-week-rng") {
    for(my $i = 0; $i < 10000; $i++) {
	print (pickWeek()."\n");
    }
} elsif($ARGV[0] eq "--test-day-rng") {
    for(my $i = 0; $i < 10000; $i++) {
	print (pickDay()."\n");
    }
} elsif($ARGV[0] eq "--test-part-rng") {
    for(my $i = 0; $i < 10000; $i++) {
	my $part = choosePart($SF);
	print("$part\t".pricePart($part)."\t");
	print(suppliercountryPart($part)."\t".suppliercountryPart($part)."\n");
    }
} elsif($ARGV[0] eq "--test-quant-rng") {
    for(my $i = 0; $i < 10000; $i++) {
	print(chooseQuantity()."\n");
    }
} elsif($ARGV[0] eq "--test-order-rng") {
    for(my $i = 0; $i < 10000; $i++) {
	print(chooseOrderNum()."\n");
    }
} elsif($ARGV[0] eq "--test-line-rng") {
    for(my $i = 0; $i < 10000; $i++) {
	print(chooseLineitemNum()."\n");
    }
} elsif($ARGV[0] =~ /^--sf=([0-9.]+)/o) {
    $SF = $1;
# generating projection of natural join of lineitem table and others.
    # preallocate array to hold temporary tuples
    my @tups;
    my $LINEITEM = 0;
    my $PARTKEY = 1;
    my $QUANTITY = 2;
    my $PARTPRICE = 3;
    my $SRCNAT = 4;
    my $EXTPRICE = 5;

    for(my $i = 0; $i < ($SF * 6000000.0);) {
	# This is the schema:
	#l_partkey	l_extendedprice	o_quantity	o_totalprice	o_orderdate_wk	o_orderdate_dayofwk	o_orderdate_yr	s_nationkey	c_nationkey
	my $partcount = choosePartcount();
	my $totalprice = 0;
	my $week = pickWeek();
	my $day  = pickDay();
	my $year = pickYear();
	my $cust_nation = pickCustomerCountry();

	for(my $j = 0; $j < $partcount; $j++) {
	    #my $lineitem = chooseLineitemNum();
	    my $partkey = choosePart($SF);
	    my $quantity = chooseQuantity();
	    my $partprice = pricePart($partkey);
	    my $src_nation = suppliercountryPart($partkey);
	    my $extendedprice = extendedpricePriceQuantity($partprice, $quantity);
	    my $totalprice += $extendedprice;
	    #$tups[$j][$LINEITEM] = $lineitem;
	    $tups[$j][$PARTKEY] = $partkey;
	    $tups[$j][$QUANTITY] = $quantity;
	    $tups[$j][$PARTPRICE] = $partprice;
	    $tups[$j][$SRCNAT] = $src_nation;
	    $tups[$j][$EXTPRICE] = $extendedprice;
	    #push @tups, \@tup;
	    $i++;
	}
	my $p = rand(100);

	my $deliver_time = 1+int(rand(14));

	if($p < 99) {
	    for(my $j = 0; $j < $partcount; $j++) {
		print("add,$i,$tups[$j][$PARTKEY],$tups[$j][$PARTPRICE],$tups[$j][$QUANTITY],$totalprice,$week,$day,$year,$tups[$j][$SRCNAT],$cust_nation,0\n");
		my ($delday, $delweek, $delyear) = addtime($day,$week,$year,($j == 0 ? $deliver_time : (1+int(rand($deliver_time)))));
		print("deliver,$i,$tups[$j][$PARTKEY],$tups[$j][$PARTPRICE],$tups[$j][$QUANTITY],$totalprice,$delweek,$delday,$delyear,$tups[$j][$SRCNAT],$cust_nation,1\n");
	    }
	} else {
	    my $rollbackidx = int(rand($partcount));
	    for(my $j = 0; $j < $partcount; $j++) {
		print("add,$i,$tups[$j][$PARTKEY],$tups[$j][$PARTPRICE],$tups[$j][$QUANTITY],$totalprice,$week,$day,$year,$tups[$j][$SRCNAT],$cust_nation,0\n");
		my ($delday, $delweek, $delyear) = addtime($day,$week,$year,$deliver_time);
		print("delete,$i,$tups[$j][$PARTKEY],$tups[$j][$PARTPRICE],$tups[$j][$QUANTITY],$totalprice,$delweek,$delday,$delyear,$tups[$j][$SRCNAT],$cust_nation,0\n");
	    }
	}
	my $order_status_count = 1+int(rand(4));
	my @status_time;
	$p = rand(100);
	if($p < 50) {
	    for(my $j = 0; $j < $order_status_count; $j++) {
		push @status_time, int(rand($deliver_time * 1.3));
	    }
	    foreach my $d (sort @status_time) {
#		($d >= 0) || die;
		my ($statday, $statweek, $statyear) = addtime($day, $week, $year, $d);
		my $off = int(rand($partcount));
#		($statday + 7 * ($statweek + 52 * $statyear) >=
#		 $day + 7 * ($week + 52 * $year)) || die "$statday $statweek $statyear < $day $week $year ($d)";
		print("status,$i,$tups[$off][$PARTKEY],$tups[$off][$PARTPRICE],$tups[$off][$QUANTITY],$totalprice,$statweek,$statday,$statyear,$tups[$off][$SRCNAT],$cust_nation,0\n");
	    }
	}
    }
}
