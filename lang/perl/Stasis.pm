package Stasis;
require Inline;

my $STASIS_DIR;
BEGIN {
 $STASIS_DIR = $ENV{STASIS_DIR};
 if(!defined($STASIS_DIR)) {
  $STASIS_DIR = $INC{"Stasis.pm"};
  $STASIS_DIR =~ s~/lang/perl/Stasis.pm~~g;
 }
 1;
}
use Inline C => Config => (LIBS =>
			   "-L$STASIS_DIR/build/src/stasis/ " .
			   "-lstasis ",
			   CCFLAGS => "-Wall -pedantic -Werror -std=c99  -DPERL_GCC_PEDANTIC"
			   ),
  ENABLE => AUTOWRAP,
  TYPEMAPS => "$STASIS_DIR/lang/perl/typemap",
  PREFIX => 'stasis_perl_';
use Inline ( C => 'DATA',
             INC  => "-I $STASIS_DIR"
    );

sub version {
   return "Stasis 0.1";
}

sub ThashCreate {
    my $xid = shift;
    my $rid = ThashCreateHelper($xid);
    return bless $rid, 'Stasis::HashHeader';
}

package Stasis::Hash;
require Scalar::Util;

require Tie::Hash;

@ISA = qw(Tie::Hash);

sub TIEHASH {
    my $class = shift;
    my $xid = shift; 
    my $rid = shift; 
    defined ($xid) || die "need xid to tie hash";
    defined ($rid) || die "need rid to tie hash";

    my $this = {
	xid => $xid,
	rid => $rid,
    };
    return bless $this, $class;
}
sub FETCH {
    my $this = shift;
    my $key = shift;
    my $sv = Stasis::ThashLookup($$this{xid}, $$this{rid}, $key);
    if(Scalar::Util::blessed($sv)) {
	if($sv->isa('Stasis::HashHeader')) {
	    my %h;
	    tie(%h, 'Stasis::Hash', $$this{xid}, $sv);
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
    if('HASH' eq ref($val)) {
	if(Scalar::Util::blessed($val) && $sv->isa('Stasis::Hash')) {
	    my $obj = tied ($val);  # tied returns the object backing the hash.
	    Stasis::ThashInsert($$this{xid}, $$this{rid}, $key, $obj{rid});
	} else {
	    # Copy the hash into scratch space
	    my %h;
	    foreach my $k (keys %$val) {
		$h{$k} = $val{$k};
	    }	   

	    # Tie the hash that was passed to us
	    my $rid = Stasis::ThashCreate($$this{xid});
	    tie %$val, 'Stasis::Hash', $$this{xid}, $rid;
	    # Copy the scratch space into the tied hash.
	    foreach my $k (keys %h) {
		$val{$k} = $h{$k}
	    }

	    # Insert the populated hashtable
	    Stasis::ThashInsert($$this{xid}, $$this{rid}, $key, $rid);
	}
    } else {
	# XXX assumes the value is a scalar.
	Stasis::ThashInsert($$this{xid}, $$this{rid}, $key, $val);
    }
}
sub DELETE {
    my $this = shift;
    my $key = shift;
    # This will leak hashes that were automatically created.  Refcounts?
    return Stasis::ThashRemove($$this{xid}, $$this{rid}, $key);
}
sub FIRSTKEY {
    my $this = shift;
    $$this{it} = Stasis::ThashIterator($$this{xid}, $$this{rid});
    if(Stasis::Titerator_next($$this{xid}, $$this{it})) {
	return Stasis::Titerator_key($$this{xid}, $$this{it});
    } else {
	Stasis::Titerator_close($$this{xid}, $$this{it});
	return;
    }
}
sub NEXTKEY {
    my $this = shift;
    my $lastkey = shift;
    Stasis::Titerator_tupleDone($$this{xid}, $$this{it});
    if(Stasis::Titerator_next($$this{xid}, $$this{it})) {
	return Stasis::Titerator_key($$this{xid}, $$this{it});
    } else {
	Stasis::Titerator_close($$this{xid}, $$this{it});
	return;
    }
}
sub EXISTS {
    my $this = shift;
    my $key = shift;
}
sub CLEAR { 
    my $this = shift;
}

package Stasis::HashHeader;
@ISA = qw(Stasis::Recordid);

package Stasis;

__DATA__
__C__
#include "stasis/transactional.h"

int Tinit();
int Tdeinit();
int Tbegin();
int Tcommit(int xid);
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
     printf("recordid_SV fail 2\n");
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
