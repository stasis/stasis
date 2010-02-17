package Stasis;
require Inline;

use strict;
use warnings;

my $STASIS_DIR;
my $initted = 0;

sub import {
    my $modname = shift;
    my $inline_dir = shift;
    require Inline;
    
    $STASIS_DIR = $INC{"Stasis.pm"};
    $STASIS_DIR =~ s~/+lang/+perl/+Stasis.pm~~g;
    1;

    Inline->import (C => 'DATA' =>
		    LIBS => "-L$STASIS_DIR/build/src/stasis/ " .
		            "-lstasis -lpthread",
		    CCFLAGS => "-Wall -pedantic -Werror -std=c99  -DPERL_GCC_PEDANTIC",
		    INC => "-I $STASIS_DIR -I $STASIS_DIR/build/",
		    ENABLE => 'AUTOWRAP',
		    TYPEMAPS => "$STASIS_DIR/lang/perl/typemap",
		    PREFIX => 'stasis_perl_',
		    DIRECTORY => $inline_dir ||$ENV{STASIS_INLINE_DIRECTORY}
	);
    Inline->init;
}
sub version {
   return "Stasis 0.1";
}

sub ThashCreate {
    my $xid = shift;
    my $rid = ThashCreateHelper($xid);
    return bless $rid, 'Stasis::HashHeader';
}
sub Tinit {
    $initted && die "Called Tinit() when Stasis was initted!\n";
    $initted = 1;
    return TinitHelper();
}
sub Tdeinit {
    $initted || die "Called Tdeinit() when Stasis was not initted!\n";
    $initted = 0;
    return TdeinitHelper();

}
sub TbootstrapHash {
    my $xid = Tbegin();
    my $rid = ROOT_RID();
    if(TrecordType($xid, $rid) == INVALID_SLOT()) {
	$rid = Stasis::ThashCreate($xid);
    }
    Tcommit($xid);
    return $rid;
}

sub open {
    my $h = shift;
    $initted || Tinit();
    tie  %$h, 'Stasis::Hash';
}
END {
    if($initted) {
	print STDERR "Cleanly shutting Stasis down..."; $| = 1;
	Tdeinit();
	print STDERR "done.\n";
    }
}

package Stasis::Hash;
require Scalar::Util;

require Tie::Hash;

our @ISA = qw(Tie::Hash);

sub TIEHASH {
    my $class = shift;
    my $xid = shift ;
    my $rid = shift || Stasis::ROOT_RID(); 
    if(!defined($xid)) {
	$xid = -1;
    }
    defined ($xid) || die "need xid to tie hash";
    defined ($rid) || die "need rid to tie hash";

    my $this = {
	xid => $xid,
	rid => $rid,
    };
    if($xid == -1) {
	$this->{autoxact} = 1;
	$this->{rid} = Stasis::TbootstrapHash();
	$this->{xid} = Stasis::Tbegin();
    }

    return bless $this, $class;
}

sub getXid {
    my $this = shift;
    my $ret = $$this{xid};
    defined $ret || ($ret = ${$$this{root}}{xid});
    #warn "$ret ($$this{xid}) ($$this{root})\n";
    return $ret;		     
}

sub setRoot {
    my $this = shift;
    my $root = shift;

    #warn "setting root\n";
    $$this{xid} = undef;
    $$this{autoxact} = 1;
    $$this{root} = ($$root{root} || $root);
    #warn $$this{root}; # = ($$this{root} || $this);
    #warn $this->getXid();

}

sub FETCH {
    my $this = shift;
    my $key = shift;
    my $xid = $this->getXid();
    my $sv = Stasis::ThashLookup($xid, $$this{rid}, $key);
    if(Scalar::Util::blessed($sv)) {
	if($sv->isa('Stasis::HashHeader')) {
	    my %h;
	    tie(%h, 'Stasis::Hash', $xid, $sv);
	    tied(%h)->setRoot($this);
	    return \%h;
	} else {
	    die 'ThashLookup returned an object of unknown type';
	}
    } else {
	return $sv;
    }
}
sub STORE {
    my $this = shift;
    my $key = shift;
    my $val = shift;
    my $xid = $this->getXid();
    if('HASH' eq ref($val)) {
	if(Scalar::Util::blessed($val) && $val->isa('Stasis::Hash')) {
	    die "untested?";
	    my $obj = tied ($val);  # tied returns the object backing the hash.
	    Stasis::ThashInsert($xid, $$this{rid}, $key, $$obj{rid});
	    die "untested?";
	} else {
	    # Copy the hash into scratch space	
	    my %h;
	    foreach my $k (keys %$val) {
		$h{$k} = $$val{$k};
	    }	   

	    # Tie the hash that was passed to us
	    my $rid = Stasis::ThashCreate($xid);
	    tie %$val, 'Stasis::Hash', $xid, $rid;

	    # Copy the scratch space into the tied hash.
	    foreach my $k (keys %h) {
		$$val{$k} = $h{$k}
	    }

	    # Insert the populated hashtable
	    Stasis::ThashInsert($xid, $$this{rid}, $key, $rid);
	    if($$this{autoxact}) {
		tied(%$val)->setRoot($this);
	    }
	}
    } else {
	# XXX assumes the value is a scalar.
	Stasis::ThashInsert($xid, $$this{rid}, $key, $val);
    }
}
sub DELETE {
    my $this = shift;
    my $key = shift;
    my $xid = $this->getXid();
    # This will leak hashes that were automatically created.  Refcounts?
    return Stasis::ThashRemove($xid, $$this{rid}, $key);
}
sub FIRSTKEY {
    my $this = shift;
    my $xid = $this->getXid();
    $$this{it} = Stasis::ThashIterator($xid, $$this{rid});
    if(Stasis::Titerator_next($xid, $$this{it})) {
	return Stasis::Titerator_key($xid, $$this{it});
    } else {
	Stasis::Titerator_close($xid, $$this{it});
	return;
    }
}
sub NEXTKEY {
    my $this = shift;
    my $lastkey = shift;
    my $xid = $this->getXid();
    Stasis::Titerator_tupleDone($xid, $$this{it});
    if(Stasis::Titerator_next($xid, $$this{it})) {
	return Stasis::Titerator_key($xid, $$this{it});
    } else {
	Stasis::Titerator_close($xid, $$this{it});
	return;
    }
}
sub EXISTS {
    my $this = shift;
    my $key = shift;
    warn "unimplemeted method 'EXISTS' called";
}
sub CLEAR { 
    my $this = shift;
    warn "unimplemeted method 'CLEAR' called";
}
sub commit {
    my $this = shift;
    $this->{autoxact} || die 'commit() called on non-auto hash';
    $this->{root} && die 'commit() called on non root';
    Stasis::Tcommit($this->{xid});
    $this->{xid} = Stasis::Tbegin();
}
sub abort {
    my $this = shift;
    $this->{autoxact} || die 'abort() called on non-auto hash';
    $this->{root} && die 'abort() called on non root';
    Stasis::Tabort($this->{xid});
    $this->{xid} = Stasis::Tbegin();
}

package Stasis::Recordid;  # Silence warning about "can't locate package Stasis::Recordid for @Stasis::HashHeader::ISA"
package Stasis::HashHeader;
our @ISA = qw(Stasis::Recordid);

package Stasis;
1;
__DATA__
__C__
#include "stasis/transactional.h"

int TinitHelper() {
    return Tinit();
}
int TdeinitHelper() {
    return Tdeinit();
}
int Tbegin();
int Tcommit(int xid);
int TsoftCommit(int xid);
void TforceCommits();
int Tabort(int xid);
int Tprepare(int xid);
recordid Talloc(int xid, unsigned long size);

int TrecordType(int xid, recordid rid);
int TrecordSize(int xid, recordid rid);

static recordid recordid_SV(SV* sv) {
   if(!(SvROK(sv) && SvTYPE(SvRV(sv)) == SVt_PVAV)) {
     printf("recordid_SV fail 1\n");
     abort();
   }
   if (!(sv_isobject(sv) && sv_derived_from(sv, "Stasis::Recordid"))) {
     printf("SV is not a recordid\n");
     abort();
   }
   AV * av = (AV*)SvRV(sv);
   if(av_len(av)+1 != 2) {
     printf("recordid_SV fail 3\n");
     abort();
   }
   SV ** pageSV = av_fetch(av, 0, 0);
   SV ** slotSV = av_fetch(av, 1, 0);
   if(!(pageSV  &&  slotSV &&
        *pageSV && *slotSV &&
        SvIOK(*pageSV) && SvIOK(*slotSV))) {
     printf("recordid_SV fail 4\n");
     abort();
   }
   recordid rid = {
     (pageid_t) SvUV(*pageSV),
     (slotid_t) SvUV(*slotSV)
   };

   return rid;
}
static SV* SV_recordid(recordid rid) {
   SV* sv;
   if(!memcmp(&rid, &NULLRID, sizeof(NULLRID))) {
     sv = newSV(0);
   } else {
     AV* arry = newAV();
     av_push(arry, newSVuv((UV) rid.page));
     av_push(arry, newSVuv((UV) rid.slot));
     sv = newRV_noinc((SV*)arry);
   }
   return sv_bless(sv, gv_stashpv("Stasis::Recordid", GV_ADD));
}

static byte * bytes_SV(SV* sv, STRLEN * sz) {
    // XXX check for hashes, arrays?
  byte * ret = 0;
  IV valI;
  NV valN;
  byte* valP;
  byte * tmp;
  recordid valR;
  char code;
  if(SvIOK(sv)) {
    // signed int, machine length
    valI = SvIV(sv);
    *sz = (STRLEN)sizeof(IV);
    tmp = (byte*)&valI;
    code = 'I';
  } else if (SvNOK(sv)) {
    valN = SvNV(sv); // double
    *sz = sizeof(NV);
    tmp = (byte*)&valN;
    code = 'N';
  } else if (SvPOK(sv)) {
    valP = (byte*)SvPV(sv,*sz); // string
    tmp = valP;
    code = 'P';
  } else if (sv_isobject(sv) && sv_isa(sv, "Stasis::HashHeader")) {
    valR = recordid_SV(sv);
    *sz = sizeof(recordid);
    tmp = (byte*)&valR;
    code = 'H';
  } else if (sv_isobject(sv) && sv_isa(sv, "Stasis::Recordid")) {
    valR = recordid_SV(sv);
    *sz = sizeof(recordid);
    tmp = (byte*)&valR;
    code = 'R';
  } else if (sv_isobject(sv)) {
    printf("Stasis.pm: Encountered unsupported object\n");
    abort();
  } else {
    printf("Stasis.pm: Encountered unsupported SV\n");
    abort();
  }
  // make room for code byte
  *sz = *sz+1;
  if(code == 'P') {
    // append null
    (*sz)++;
    ret = malloc(*sz);
    memcpy(ret,tmp, (*sz)-2);
    ret[(*sz)-2] = '\0';
    ret[(*sz)-1] = code;
  } else {
    ret = malloc(*sz);
    memcpy(ret, tmp, (*sz)-1);
    ret[(*sz)-1] = code;
  }
  return ret;
}
static SV * SV_bytes(byte* bytes, STRLEN sz) {
  SV * ret;
  char code = bytes[sz-1];
  switch(code) {
  case 'I': {
    assert(sz-1 == sizeof(IV));
    ret = newSViv(*(IV*)bytes);
  } break;
  case 'N': {
    assert(sz-1 == sizeof(NV));
    ret = newSVnv(*(NV*)bytes);
  } break;
  case 'P': {
    ret = newSVpvn((const char*)bytes,sz-2);
  } break;
  case 'R': {
    assert(sz-1 == sizeof(recordid));
    ret = SV_recordid(*(recordid*)bytes);
  } break;
  case 'H': {
    assert(sz-1 == sizeof(recordid));
    ret = SV_recordid(*(recordid*)bytes);
    ret = sv_bless(ret, gv_stashpv("Stasis::HashHeader", GV_ADD));
  } break;
  default: {
    abort();
  }
  }
  return ret;
}

/** Records */

recordid TallocScalar(int xid, SV* sv) {
  STRLEN sz;
  byte * buf = bytes_SV(sv, &sz);
  free(buf);
  return Talloc(xid, sz);
}

int stasis_perl_Tset(int xid, recordid rid, SV * sv) {
  STRLEN sz;
  byte * buf = bytes_SV(sv, &sz);
  rid.size = sz;
  int ret = Tset(xid, rid, buf);
  free(buf);
  return ret;
}
SV* stasis_perl_Tread(int xid, recordid rid) {
  rid.size = TrecordSize(xid, rid);
  byte * buf = malloc(rid.size);
  Tread(xid, rid, buf);
  SV* ret = SV_bytes(buf, rid.size);
  free(buf);
  return ret;
}

int stasis_perl_TsetRecordid(int xid, recordid rid, recordid buf) {
  rid.size = sizeof(buf);
  return Tset(xid, rid, &buf);
}

recordid stasis_perl_TreadRecordid(int xid, recordid rid) {
  recordid buf;
  Tread(xid, rid, &buf);
  return buf;
}

/** Hash table */

recordid stasis_perl_ThashCreateHelper(int xid) {
  return ThashCreate(xid, VARIABLE_LENGTH, VARIABLE_LENGTH);
}

int stasis_perl_ThashInsert(int xid, recordid hash, SV * key, SV * val) {
  STRLEN key_len, val_len;
  byte * keyb = bytes_SV(key,&key_len);
  byte * valb = bytes_SV(val,&val_len);
  int ret = ThashInsert(xid, hash, keyb, key_len, valb, val_len);
  free(keyb);
  free(valb);
  return ret;
}

int stasis_perl_ThashRemove(int xid, recordid hash, SV * key) {
  STRLEN key_len;
  byte * keyb = bytes_SV(key,&key_len);
  int ret = ThashRemove(xid, hash, keyb, key_len);
  free(keyb);
  return ret;
}

SV* stasis_perl_ThashLookup(int xid, recordid hash, SV * key) {
  STRLEN key_len;
  byte* keyb = bytes_SV(key, &key_len);

  byte* valb;
  int val_len = ThashLookup(xid, hash, keyb, key_len, &valb);
  free(keyb);
  if(val_len != -1) {
    SV* ret = SV_bytes(valb, val_len);
    free(valb);
    return ret;
  } else {
    return &PL_sv_undef;
  }
}

void * stasis_perl_ThashIterator(int xid, recordid hash) {
    return ThashGenericIterator(xid, hash);
}

/** Arrays */

recordid stasis_perl_TarrayList_alloc(int xid, SV* exemplar) {
  byte * bytes;
  size_t sz;
  bytes = bytes_SV(exemplar, &sz);
  return TarrayListAlloc(xid, 4, 2, sz);
  free(bytes);
}

int stasis_perl_TarrayList_extend(int xid, recordid rid, int slots) {
  return TarrayListExtend(xid, rid, slots);
}
int stasis_perl_TarrayList_length(int xid, recordid rid) {
  return TarrayListLength(xid, rid);
}

/** Iterators */

int stasis_perl_Titerator_next(int xid, void *it) {
    return Titerator_next(xid, it);
}
SV* stasis_perl_Titerator_key(int xid, void *it) {
    byte * bytes;
    STRLEN sz = Titerator_key(xid, it, &bytes);
    return SV_bytes(bytes, sz);
}
SV* stasis_perl_Titerator_value(int xid, void *it) {
    byte * bytes;
    STRLEN sz = Titerator_value(xid, it, &bytes);
    return SV_bytes(bytes, sz);
}
void stasis_perl_Titerator_tupleDone(int xid, void *it) {
    Titerator_tupleDone(xid, it);
}
void stasis_perl_Titerator_close(int xid, void *it) {
    Titerator_close(xid, it);
}
SV* stasis_perl_ROOT_RID() {
  return SV_recordid(ROOT_RECORD);
}

SV* stasis_perl_NULL_RID() {
  return SV_recordid(NULLRID);
}

int stasis_perl_INVALID_SLOT() {
  return INVALID_SLOT;
}


__END__

=head1 NAME

Stasis - Flexible transactional storage

=head1 SYNOPSIS

    use Stasis;

    my %h;

    Stasis::open(\%h);

    $h{foo} = 'bar';

    # Create an anonymous hash and insert it into h 

    $h{bat}{bam} = 'boom';

    tied(%h)->commit();

    $h{bar} = 'bad update';

    tied(%h)->abort();

    defined $h{bar} && die;

=head1 DESCRIPTION

Stasis is a lightweight transactional storage library.  This perl
module provides bindings for Stasis' C functions (Tinit(),
ThashInsert(), etc), and higher-level interfaces based upon tied perl
hashes.

=head2 Programming Style

The synopsis describes Stasis.pm's high level interface.  Lower-level
interactions with Stasis are possible as well:

    use Stasis;

    Stasis::Tinit(); # Initialize Stasis
 
    Stasis::TbootstrapHash(); # Open or bootstrap a stasis database

    # Bootstrapping arranges for a hash to live in ROOT_RECORD

    my $rid = STASIS::ROOT_RECORD();

    my $xid = Stasis::Tbegin(); # Start new transaction

    # Insert a value into the hash
    Stasis::ThashInsert($xid, $rid, "foo", "bar");

    # Lookup the value
    my $bar = Stasis::ThashLookup($xid, $rid, "foo", "bar");

    Stasis::Tcommit($xid);

    $xid = Stasis::Tbegin();

    # This update will not be reflected after abort.
    Stasis::ThashRemove($xid, $rid, "foo", "bar");

    Stasis::Tabort($xid);

    #Deinitialize Stasis (Called automatically at shutdown if needed)
    Stasis::Tdeinit(); 

Stasis supports a wide range of other data structures (including
arrays, records, large objects and trees), which are somewhat
supported by Stasis.pm.  These bindings are a work in progress;
consult the source code for a list of currently implemented methods.

Note that Stasis (and this module) are thread safe.  However, Stasis
does not perform lock management.  Refer to the Stasis documentation
for more information before attempting to make use of concurrent (even
if single threaded) transactions.


=head1 CAVEATS AND BUGS

=head2 No garbage collection 

Nested hashes are not garbage collected.  Therefore, the following
code leaks storage:

    my %h;
    Stasis::open(\%h);

    $h{a}{b} = 1;  # Automatically instantiates a hash.
    delete $h{a};  # The automatically created hash is now unreachable.

    tied(%h)->commit();  # However, abort() would have reclaimed the space

=head2 Small hash performance

Stasis is currently tuned for small numbers of large hashes.
Its hashtable implementation would be more efficient if it included
special cases for small indexes, then dynamically switched to the
current layout for large data sets.

=head2 No tied arrays

Stasis provides an array type, but this module does not export them to
perl as tied arrays.  Instead, it includes a partial (and untested)
set of C-style bindings.  Support for push() and pop() should be
straightforward.  However, Stasis' array implementation does not
currently provide anything analogous to shift() and unshift().  Like
the hashtables, Stasis' arrays are tuned for large data sets.

=head2 Type safety / reflection

Stasis records include a 'type' field that allows 'special' data to be
distinguished from 'normal' slots (eg: application data).  Hashtables
do not use this feature, so it is possible to attempt to access
headers as scalar values.  This will likely fail by crashing the
process.  Similarly, the recordids returned by Stasis are blessed
arrays.  Tampering with their contents, then attempting to dereference
them will likely lead to crashes and other trouble.  These problems
should not affect "well-written" code.

=head2 This documentation is incomplete

See the source for a complete list of exported Stasis functions.

=head2 This module is a work in progress

Expect API instability.  Also, note that many Stasis functions are not
yet exported to perl.
