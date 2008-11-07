package Stasis;
require Inline;

my $STASIS_DIR;
BEGIN {
 $STASIS_DIR = $ENV{STASIS_DIR}
   || die "\nNeed STASIS_DIR environment variable!!\n\n";
#'/home/sears/stasis4';
}
use Inline C => Config => LIBS =>
    "-L$STASIS_DIR/build/src/stasis/ " .
    "-lstasis ",
  ENABLE => AUTOWRAP,
  TYPEMAPS => "$STASIS_DIR/lang/perl/typemap",
  PREFIX => 'stasis_perl_';
use Inline ( C => 'DATA',
             INC  => "-I $STASIS_DIR"
    );

sub version {
   return "Stasis 0.1";
}

__DATA__
__C__
#include "stasis/transactional.h"

static int initted;

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
     abort();
   }
   if (!(sv_isobject(sv) && sv_derived_from(sv, "Stasis::Recordid"))) {
     abort();
   }
   AV * av = (AV*)SvRV(sv);
   if(av_len(av)+1 != 2) {
     abort();
   }
   SV ** pageSV = av_fetch(av, 0, 0);
   SV ** slotSV = av_fetch(av, 1, 0);
   if(!(pageSV  &&  slotSV &&
        *pageSV && *slotSV &&
        SvIOK(*pageSV) && SvIOK(*slotSV))) {
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
  byte * ret = 0;
  IV valI;
  NV valN;
  char* valP;
  byte * tmp;
  recordid valR;
  char code;
  if(SvIOK(sv)) {
    // signed int, machine length
    valI = SvIV(sv);
    *sz = sizeof(IV);
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
  } else if (sv_isobject(sv) && sv_derived_from(sv, "Stasis::Recordid")) {
    valR = recordid_SV(sv);
    *sz = sizeof(recordid);
    tmp = (byte*)&valR;
    code = 'R';
  } else {
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
    ret = newSVpvn(bytes,sz-2);
  } break;
  case 'R': {
    assert(sz-1 == sizeof(recordid));
    ret = SV_recordid(*(recordid*)bytes);
  } break;
  default: {
    abort();
  }
  }
  return ret;
}

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
  char * buf = malloc(rid.size);
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

recordid stasis_perl_ThashCreate(int xid) {
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
    return 0;
  }
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

/*SV* new() {
  void * session = 0;
  Tinit();

  SV*    obj_ref = newSViv(0);
  SV*    obj = newSVrv(obj_ref, "Stasis");

  sv_setiv(obj, (IV)session);
  SvREADONLY_on(obj);
  return obj_ref;
}

int begin_xact(SV* obj) {
  return Tbegin();
}

void commit_xact(SV* obj, int xid) {
  Tcommit(xid);
}
void abort_xact(SV* obj, int xid) {
  Tabort(xid);
}

void DESTROY(SV* obj) {
  Tdeinit();
}
*/
