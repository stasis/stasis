#ifndef _ROSE_COMPRESSION_STATICTUPLE_H__
#define _ROSE_COMPRESSION_STATICTUPLE_H__

namespace rose {
  template<int N, class TYPE0,
    class TYPE1=bool, class TYPE2=bool, class TYPE3=bool, class TYPE4=bool,
    class TYPE5=bool, class TYPE6=bool, class TYPE7=bool, class TYPE8=bool,
    class TYPE9=bool, class TYPE10=bool,
    class TYPE11=bool, class TYPE12=bool, class TYPE13=bool, class TYPE14=bool,
    class TYPE15=bool, class TYPE16=bool, class TYPE17=bool, class TYPE18=bool,
    class TYPE19=bool>
    class StaticTuple {
  public:
  static const char NORMAL = 0;
  static const char TOMBSTONE = 1;
  static const int TUPLE_ID = 1;
  static const int NN = N;
  /** Compatibility for dynamic dispatch stuff */
  inline int column_count() const { return NN; }
  inline void* get(column_number_t i) const { return ((byte*)&s) + cols_[i]; }

  typedef TYPE0 TYP0;
  typedef TYPE1 TYP1;
  typedef TYPE2 TYP2;
  typedef TYPE3 TYP3;
  typedef TYPE4 TYP4;
  typedef TYPE5 TYP5;
  typedef TYPE6 TYP6;
  typedef TYPE7 TYP7;
  typedef TYPE8 TYP8;
  typedef TYPE9 TYP9;
  typedef TYPE10 TYP10;
  typedef TYPE11 TYP11;
  typedef TYPE12 TYP12;
  typedef TYPE13 TYP13;
  typedef TYPE14 TYP14;
  typedef TYPE15 TYP15;
  typedef TYPE16 TYP16;
  typedef TYPE17 TYP17;
  typedef TYPE18 TYP18;
  typedef TYPE19 TYP19;

  inline bool tombstone() const {
    return s.flag_ == TOMBSTONE;
  }

  explicit inline StaticTuple() {
    s.flag_ = NORMAL; s.epoch_ = 0 ;
    initializePointers();
  }
  explicit inline StaticTuple(const StaticTuple& t) {
    s.flag_ = t.s.flag_;
    s.epoch_ = t.s.epoch_;
    if(0 < N) s.cols0_ = t.s.cols0_;
    if(1 < N) s.cols1_ = t.s.cols1_;
    if(2 < N) s.cols2_ = t.s.cols2_;
    if(3 < N) s.cols3_ = t.s.cols3_;
    if(4 < N) s.cols4_ = t.s.cols4_;
    if(5 < N) s.cols5_ = t.s.cols5_;
    if(6 < N) s.cols6_ = t.s.cols6_;
    if(7 < N) s.cols7_ = t.s.cols7_;
    if(8 < N) s.cols8_ = t.s.cols8_;
    if(9 < N) s.cols9_ = t.s.cols9_;
    if(10 < N) s.cols10_ = t.s.cols10_;
    if(11 < N) s.cols11_ = t.s.cols11_;
    if(12 < N) s.cols12_ = t.s.cols12_;
    if(13 < N) s.cols13_ = t.s.cols13_;
    if(14 < N) s.cols14_ = t.s.cols14_;
    if(15 < N) s.cols15_ = t.s.cols15_;
    if(16 < N) s.cols16_ = t.s.cols16_;
    if(17 < N) s.cols17_ = t.s.cols17_;
    if(18 < N) s.cols18_ = t.s.cols18_;
    if(19 < N) s.cols19_ = t.s.cols19_;
    initializePointers();
  }

  inline ~StaticTuple() { }

  static inline byte_off_t sizeofBytes() {
    // Computing by starting from zero, and adding up column costs wouldn't
    // take struct padding into account.  This might over-estimate the
    // size, but that's fine, since any in-memory copy will either be malloced
    // according to what we say here, or will be an actual st struct.
    return sizeof(st) -
    ((0 >= N) ? sizeof(TYPE0) : 0) -
    ((1 >= N) ? sizeof(TYPE1) : 0) -
    ((2 >= N) ? sizeof(TYPE2) : 0) -
    ((3 >= N) ? sizeof(TYPE3) : 0) -
    ((4 >= N) ? sizeof(TYPE4) : 0) -
    ((5 >= N) ? sizeof(TYPE5) : 0) -
    ((6 >= N) ? sizeof(TYPE6) : 0) -
    ((7 >= N) ? sizeof(TYPE7) : 0) -
    ((8 >= N) ? sizeof(TYPE8) : 0) -
    ((9 >= N) ? sizeof(TYPE9) : 0) -
    ((10 >= N) ? sizeof(TYPE10) : 0) -
    ((11 >= N) ? sizeof(TYPE11) : 0) -
    ((12 >= N) ? sizeof(TYPE12) : 0) -
    ((13 >= N) ? sizeof(TYPE13) : 0) -
    ((14 >= N) ? sizeof(TYPE14) : 0) -
    ((15 >= N) ? sizeof(TYPE15) : 0) -
    ((16 >= N) ? sizeof(TYPE16) : 0) -
    ((17 >= N) ? sizeof(TYPE17) : 0) -
    ((18 >= N) ? sizeof(TYPE18) : 0) -
    ((19 >= N) ? sizeof(TYPE19) : 0) ;
  }

  /*  inline void* set(column_number_t col, void* val) {
    memcpy(((byte*)&s)+cols_[col],val,size_[col]);
    return(((byte*)&s)+cols_[col]);
    } */

  inline void set0(TYPE0* val) { s.cols0_=*val; }
  inline void set1(TYPE1* val) { s.cols1_=*val; }
  inline void set2(TYPE2* val) { s.cols2_=*val; }
  inline void set3(TYPE3* val) { s.cols3_=*val; }
  inline void set4(TYPE4* val) { s.cols4_=*val; }
  inline void set5(TYPE5* val) { s.cols5_=*val; }
  inline void set6(TYPE6* val) { s.cols6_=*val; }
  inline void set7(TYPE7* val) { s.cols7_=*val; }
  inline void set8(TYPE8* val) { s.cols8_=*val; }
  inline void set9(TYPE9* val) { s.cols9_=*val; }
  inline void set10(TYPE10* val) { s.cols10_=*val; }
  inline void set11(TYPE11* val) { s.cols11_=*val; }
  inline void set12(TYPE12* val) { s.cols12_=*val; }
  inline void set13(TYPE13* val) { s.cols13_=*val; }
  inline void set14(TYPE14* val) { s.cols14_=*val; }
  inline void set15(TYPE15* val) { s.cols15_=*val; }
  inline void set16(TYPE16* val) { s.cols16_=*val; }
  inline void set17(TYPE17* val) { s.cols17_=*val; }
  inline void set18(TYPE18* val) { s.cols18_=*val; }
  inline void set19(TYPE19* val) { s.cols19_=*val; }

  inline const TYPE0 * get0() const { return &s.cols0_; }
  inline const TYPE1 * get1() const { return &s.cols1_; }
  inline const TYPE2 * get2() const { return &s.cols2_; }
  inline const TYPE3 * get3() const { return &s.cols3_; }
  inline const TYPE4 * get4() const { return &s.cols4_; }
  inline const TYPE5 * get5() const { return &s.cols5_; }
  inline const TYPE6 * get6() const { return &s.cols6_; }
  inline const TYPE7 * get7() const { return &s.cols7_; }
  inline const TYPE8 * get8() const { return &s.cols8_; }
  inline const TYPE9 * get9() const { return &s.cols9_; }
  inline const TYPE10 * get10() const { return &s.cols10_; }
  inline const TYPE11 * get11() const { return &s.cols11_; }
  inline const TYPE12 * get12() const { return &s.cols12_; }
  inline const TYPE13 * get13() const { return &s.cols13_; }
  inline const TYPE14 * get14() const { return &s.cols14_; }
  inline const TYPE15 * get15() const { return &s.cols15_; }
  inline const TYPE16 * get16() const { return &s.cols16_; }
  inline const TYPE17 * get17() const { return &s.cols17_; }
  inline const TYPE18 * get18() const { return &s.cols18_; }
  inline const TYPE19 * get19() const { return &s.cols19_; }

  /*  inline void* get(column_number_t col) const {
    return ((byte*)&s) + cols_[col];
    } */
  //inline column_number_t column_count() const { return N; }

  /*  inline byte_off_t column_len(column_number_t col) const {
    return size_[col];
    } */
  inline byte* toByteArray() const {
    return (byte*)&s;
  }
  inline bool operator==(const StaticTuple &t) const {
    if(0 < N) if(s.cols0_ != t.s.cols0_) return 0;
    if(1 < N) if(s.cols1_ != t.s.cols1_) return 0;
    if(2 < N) if(s.cols2_ != t.s.cols2_) return 0;
    if(3 < N) if(s.cols3_ != t.s.cols3_) return 0;
    if(4 < N) if(s.cols4_ != t.s.cols4_) return 0;
    if(5 < N) if(s.cols5_ != t.s.cols5_) return 0;
    if(6 < N) if(s.cols6_ != t.s.cols6_) return 0;
    if(7 < N) if(s.cols7_ != t.s.cols7_) return 0;
    if(8 < N) if(s.cols8_ != t.s.cols8_) return 0;
    if(9 < N) if(s.cols9_ != t.s.cols9_) return 0;
    if(10 < N) if(s.cols10_ != t.s.cols10_) return 0;
    if(11 < N) if(s.cols11_ != t.s.cols11_) return 0;
    if(12 < N) if(s.cols12_ != t.s.cols12_) return 0;
    if(13 < N) if(s.cols13_ != t.s.cols13_) return 0;
    if(14 < N) if(s.cols14_ != t.s.cols14_) return 0;
    if(15 < N) if(s.cols15_ != t.s.cols15_) return 0;
    if(16 < N) if(s.cols16_ != t.s.cols16_) return 0;
    if(17 < N) if(s.cols17_ != t.s.cols17_) return 0;
    if(18 < N) if(s.cols18_ != t.s.cols18_) return 0;
    if(19 < N) if(s.cols19_ != t.s.cols19_) return 0;
    return 1;
  }
  inline bool operator<(const StaticTuple &t) const {
    if(0 < N) {
      if(s.cols0_ < t.s.cols0_) return 1;
    }
    if(1 < N) {
      if(s.cols0_ != t.s.cols0_) return 0;
      else if(s.cols1_ < t.s.cols1_) return 1;
    }
    if(2 < N) {
      if(s.cols1_ != t.s.cols1_) return 0;
      else if(s.cols2_ < t.s.cols2_) return 1;
    }
    if(3 < N) {
      if(s.cols2_ != t.s.cols2_) return 0;
      else if(s.cols3_ < t.s.cols3_) return 1;
    }
    if(4 < N) {
      if(s.cols3_ != t.s.cols3_) return 0;
      else if(s.cols4_ < t.s.cols4_) return 1;
    }
    if(5 < N) {
      if(s.cols4_ != t.s.cols4_) return 0;
      else if(s.cols5_ < t.s.cols5_) return 1;
    }
    if(6 < N) {
      if(s.cols5_ != t.s.cols5_) return 0;
      else if(s.cols6_ < t.s.cols6_) return 1;
    }
    if(7 < N) {
      if(s.cols6_ != t.s.cols6_) return 0;
      else if(s.cols7_ < t.s.cols7_) return 1;
    }
    if(8 < N) {
      if(s.cols7_ != t.s.cols7_) return 0;
      else if(s.cols8_ < t.s.cols8_) return 1;
    }
    if(9 < N) {
      if(s.cols8_ != t.s.cols8_) return 0;
      else if(s.cols9_ < t.s.cols9_) return 1;
    }
    if(10 < N) {
      if(s.cols9_ != t.s.cols9_) return 0;
      else if(s.cols10_ < t.s.cols10_) return 1;
    }
    if(11 < N) {
      if(s.cols10_ != t.s.cols10_) return 0;
      else if(s.cols11_ < t.s.cols11_) return 1;
    }
    if(12 < N) {
      if(s.cols11_ != t.s.cols11_) return 0;
      else if(s.cols12_ < t.s.cols12_) return 1;
    }
    if(13 < N) {
      if(s.cols12_ != t.s.cols12_) return 0;
      else if(s.cols13_ < t.s.cols13_) return 1;
    }
    if(14 < N) {
      if(s.cols13_ != t.s.cols13_) return 0;
      else if(s.cols14_ < t.s.cols14_) return 1;
    }
    if(15 < N) {
      if(s.cols14_ != t.s.cols14_) return 0;
      else if(s.cols15_ < t.s.cols15_) return 1;
    }
    if(16 < N) {
      if(s.cols15_ != t.s.cols15_) return 0;
      else if(s.cols16_ < t.s.cols16_) return 1;
    }
    if(17 < N) {
      if(s.cols16_ != t.s.cols16_) return 0;
      else if(s.cols17_ < t.s.cols17_) return 1;
    }
    if(18 < N) {
      if(s.cols17_ != t.s.cols17_) return 0;
      else if(s.cols18_ < t.s.cols18_) return 1;
    }
    if(19 < N) {
      if(s.cols18_ != t.s.cols18_) return 0;
      else if(s.cols19_ < t.s.cols19_) return 1;
    }
    return 0;
  }

  inline void copyFrom(const StaticTuple& t) {
    s.flag_=t.s.flag_;
    s.epoch_=t.s.epoch_;
    if(0 < N) { s.cols0_ = t.s.cols0_; }
    if(1 < N) { s.cols1_ = t.s.cols1_; }
    if(2 < N) { s.cols2_ = t.s.cols2_; }
    if(3 < N) { s.cols3_ = t.s.cols3_; }
    if(4 < N) { s.cols4_ = t.s.cols4_; }
    if(5 < N) { s.cols5_ = t.s.cols5_; }
    if(6 < N) { s.cols6_ = t.s.cols6_; }
    if(7 < N) { s.cols7_ = t.s.cols7_; }
    if(8 < N) { s.cols8_ = t.s.cols8_; }
    if(9 < N) { s.cols9_ = t.s.cols9_; }
    if(10 < N) { s.cols10_ = t.s.cols10_; }
    if(11 < N) { s.cols11_ = t.s.cols11_; }
    if(12 < N) { s.cols12_ = t.s.cols12_; }
    if(13 < N) { s.cols13_ = t.s.cols13_; }
    if(14 < N) { s.cols14_ = t.s.cols14_; }
    if(15 < N) { s.cols15_ = t.s.cols15_; }
    if(16 < N) { s.cols16_ = t.s.cols16_; }
    if(17 < N) { s.cols17_ = t.s.cols17_; }
    if(18 < N) { s.cols18_ = t.s.cols18_; }
    if(19 < N) { s.cols19_ = t.s.cols19_; }
  }

  static void printSt(void const * const sp) {
    st const * const s = (st const * const)sp;
    printf("(");
    if(0<N) printf("%lld",   (long long)s->cols0_);
    if(1<N) printf(", %lld", (long long)s->cols1_);
    if(2<N) printf(", %lld", (long long)s->cols2_);
    if(3<N) printf(", %lld", (long long)s->cols3_);
    if(4<N) printf(", %lld", (long long)s->cols4_);
    if(5<N) printf(", %lld", (long long)s->cols5_);
    if(6<N) printf(", %lld", (long long)s->cols6_);
    if(7<N) printf(", %lld", (long long)s->cols7_);
    if(8<N) printf(", %lld", (long long)s->cols8_);
    if(9<N) printf(", %lld", (long long)s->cols9_);
    if(10<N) printf(", %lld",(long long)s->cols10_);
    if(11<N) printf(", %lld",(long long)s->cols11_);
    if(12<N) printf(", %lld",(long long)s->cols12_);
    if(13<N) printf(", %lld",(long long)s->cols13_);
    if(14<N) printf(", %lld",(long long)s->cols14_);
    if(15<N) printf(", %lld",(long long)s->cols15_);
    if(16<N) printf(", %lld",(long long)s->cols16_);
    if(17<N) printf(", %lld",(long long)s->cols17_);
    if(18<N) printf(", %lld",(long long)s->cols18_);
    if(19<N) printf(", %lld",(long long)s->cols19_);
    printf(")");
  }

  static void printErrSt(void const * const sp) {
    st const * const s = (st const * const)sp;
    fprintf(stderr, "(");
    if(0<N) fprintf(stderr, "%lld",   (long long)s->cols0_);
    if(1<N) fprintf(stderr, ", %lld", (long long)s->cols1_);
    if(2<N) fprintf(stderr, ", %lld", (long long)s->cols2_);
    if(3<N) fprintf(stderr, ", %lld", (long long)s->cols3_);
    if(4<N) fprintf(stderr, ", %lld", (long long)s->cols4_);
    if(5<N) fprintf(stderr, ", %lld", (long long)s->cols5_);
    if(6<N) fprintf(stderr, ", %lld", (long long)s->cols6_);
    if(7<N) fprintf(stderr, ", %lld", (long long)s->cols7_);
    if(8<N) fprintf(stderr, ", %lld", (long long)s->cols8_);
    if(9<N) fprintf(stderr, ", %lld", (long long)s->cols9_);
    if(10<N) fprintf(stderr, ", %lld",(long long)s->cols10_);
    if(11<N) fprintf(stderr, ", %lld",(long long)s->cols11_);
    if(12<N) fprintf(stderr, ", %lld",(long long)s->cols12_);
    if(13<N) fprintf(stderr, ", %lld",(long long)s->cols13_);
    if(14<N) fprintf(stderr, ", %lld",(long long)s->cols14_);
    if(15<N) fprintf(stderr, ", %lld",(long long)s->cols15_);
    if(16<N) fprintf(stderr, ", %lld",(long long)s->cols16_);
    if(17<N) fprintf(stderr, ", %lld",(long long)s->cols17_);
    if(18<N) fprintf(stderr, ", %lld",(long long)s->cols18_);
    if(19<N) fprintf(stderr, ", %lld",(long long)s->cols19_);
    fprintf(stderr, ")\n");
  }
  static inline int noisycmp(const void *ap, const void *bp) {
    st const * const a = (st const * const)ap;
    st const * const b = (st const * const)bp;

    int ret = cmp(ap,bp);
    printSt(a); printf(" cmp "); printSt(b); printf(" = %d", ret); printf("\n");
    return ret;
  }
  static inline int cmp(const void *ap, const void *bp) {
    st const * const a = (st const * const)ap;
    st const * const b = (st const * const)bp;

    if(0<N) {
      if(a->cols0_ < b->cols0_)  return -1;
      if(a->cols0_ != b->cols0_) return 1;
      DEBUG("0 matched\n");;
    }
    if(1<N) {
      if(a->cols1_  < b->cols1_)  return -1;
      if(a->cols1_ != b->cols1_) return 1;
      DEBUG("1 matched\n");
    }
    if(2<N) {
      if(a->cols2_  < b->cols2_)  return -1;
      if(a->cols2_ != b->cols2_) return 1;
      DEBUG("2 matched\n");
    }
    if(3<N) {
      if(a->cols3_  < b->cols3_)  return -1;
      if(a->cols3_ != b->cols3_) return 1;
      DEBUG("3 matched\n");
    }
    if(4<N) {
      if(a->cols4_ < b->cols4_)  return -1;
      if(a->cols4_ != b->cols4_) return 1;
      DEBUG("4 matched\n");
    }
    if(5<N) {
      if(a->cols5_ < b->cols5_)  return -1;
      if(a->cols5_ != b->cols5_) return 1;
      DEBUG("5 matched\n");
    }
    if(6<N) {
      if(a->cols6_ < b->cols6_)  return -1;
      if(a->cols6_ != b->cols6_) return 1;
      DEBUG("6 matched\n");
    }
    if(7<N) {
      if(a->cols7_ < b->cols7_)  return -1;
      if(a->cols7_ != b->cols7_) return 1;
      DEBUG("7 matched\n");
    }
    if(8<N) {
      if(a->cols8_ < b->cols8_)  return -1;
      if(a->cols8_ != b->cols8_) return 1;
      DEBUG("8 matched\n");
    }
    if(9<N) {
      if(a->cols9_ < b->cols9_)  return -1;
      if(a->cols9_ != b->cols9_) return 1;
      DEBUG("9 matched\n");
    }
    if(10<N) {
      if(a->cols10_  < b->cols10_)  return -1;
      if(a->cols10_ != b->cols10_) return 1;
      DEBUG("10 matched\n");;
    }
    if(11<N) {
      if(a->cols11_  < b->cols11_)  return -1;
      if(a->cols11_ != b->cols11_) return 1;
      DEBUG("11 matched\n");
    }
    if(12<N) {
      if(a->cols12_  < b->cols12_)  return -1;
      if(a->cols12_ != b->cols12_) return 1;
      DEBUG("12 matched\n");
    }
    if(13<N) {
      if(a->cols13_  < b->cols13_)  return -1;
      if(a->cols13_ != b->cols13_) return 1;
      DEBUG("13 matched\n");
    }
    if(14<N) {
      if(a->cols14_  < b->cols14_)  return -1;
      if(a->cols14_ != b->cols14_) return 1;
      DEBUG("14 matched\n");
    }
    if(15<N) {
      if(a->cols15_ < b->cols15_)  return -1;
      if(a->cols15_ != b->cols15_) return 1;
      DEBUG("15 matched\n");
    }
    if(16<N) {
      if(a->cols16_ < b->cols16_)  return -1;
      if(a->cols16_ != b->cols16_) return 1;
      DEBUG("16 matched\n");
    }
    if(17<N) {
      if(a->cols17_ < b->cols17_)  return -1;
      if(a->cols17_ != b->cols17_) return 1;
      DEBUG("17 matched\n");
    }
    if(18<N) {
      if(a->cols18_ < b->cols18_)  return -1;
      if(a->cols18_ != b->cols18_) return 1;
      DEBUG("18 matched\n");
    }
    if(19<N) {
      if(a->cols19_ < b->cols19_)  return -1;
      if(a->cols19_ != b->cols19_) return 1;
      DEBUG("19 matched\n");
    }
    DEBUG("N matched\n");
    return 0;
  }

  /*  static inline int cmp(const void *ap, const void *bp) {
    const StaticTuple * a = (const StaticTuple*)ap;
    const StaticTuple * b = (const StaticTuple*)bp;
    if(*a < *b) {
      return -1;
    } else if(*a == *b) {
      // Sort *backwards* on epoch values.
      if(a->s.epoch_ > b->s.epoch_) { return 1; }
      else if(a->s.epoch_ < b->s.epoch_) { return -1; }
      else return 0;
    } else {
      return 1;
    }
    } */

  struct stl_cmp
  {
    bool operator()(const StaticTuple& s1, const StaticTuple& s2) const
    {
      return s1 < s2;
    }
  };

  class iterator {
  public:
    inline iterator(void const *const *const dataset, int offset)
      : dat_(dataset),
      off_(offset),
      scratch_() {}
    inline explicit iterator(const iterator &i) : c_(i.c_), dat_(i.dat_),
      off_(i.off_), scratch_() {}
    inline StaticTuple& operator*() {
      if(0 < N) scratch_.set0((TYPE0*)dat_[0][off_]);
      if(1 < N) scratch_.set1((TYPE1*)dat_[1][off_]);
      if(2 < N) scratch_.set2((TYPE2*)dat_[2][off_]);
      if(3 < N) scratch_.set3((TYPE3*)dat_[3][off_]);
      if(4 < N) scratch_.set4((TYPE4*)dat_[4][off_]);
      if(5 < N) scratch_.set5((TYPE5*)dat_[5][off_]);
      if(6 < N) scratch_.set6((TYPE6*)dat_[6][off_]);
      if(7 < N) scratch_.set7((TYPE7*)dat_[7][off_]);
      if(8 < N) scratch_.set8((TYPE8*)dat_[8][off_]);
      if(9 < N) scratch_.set9((TYPE9*)dat_[9][off_]);
      if(10 < N) scratch_.set10((TYPE0*)dat_[10][off_]);
      if(11 < N) scratch_.set11((TYPE1*)dat_[11][off_]);
      if(12 < N) scratch_.set12((TYPE2*)dat_[12][off_]);
      if(13 < N) scratch_.set13((TYPE3*)dat_[13][off_]);
      if(14 < N) scratch_.set14((TYPE4*)dat_[14][off_]);
      if(15 < N) scratch_.set15((TYPE5*)dat_[15][off_]);
      if(16 < N) scratch_.set16((TYPE6*)dat_[16][off_]);
      if(17 < N) scratch_.set17((TYPE7*)dat_[17][off_]);
      if(18 < N) scratch_.set18((TYPE8*)dat_[18][off_]);
      if(19 < N) scratch_.set19((TYPE9*)dat_[19][off_]);
      return scratch_;
    }

    inline bool operator==(const iterator &a) const {
      return (off_==a.off_);
    }
    inline bool operator!=(const iterator &a) const {
      return(off_!=a.off);
    }
    inline void operator++() { off_++; }
    inline void operator--() { off_--; }
    inline void operator+=(int i) { abort(); }
    inline int operator-(iterator &i) {
      return off_ - i.off_;
    }
    inline void operator=(iterator &i) {
      off_=i.off_;
    }
    inline void offset(int off) {
      off_=off;
    }
  private:
    column_number_t c_;
    void const * const dat_[N];
    int off_;
    StaticTuple scratch_;
  };
  private:
  static bool first_;
  static short cols_[N];
  static byte_off_t size_[N];
  typedef char flag_t;
  typedef unsigned int epoch_t;
  typedef struct {
    flag_t flag_;
    epoch_t epoch_;
    TYPE0 cols0_;
    TYPE1 cols1_;
    TYPE2 cols2_;
    TYPE3 cols3_;
    TYPE4 cols4_;
    TYPE5 cols5_;
    TYPE6 cols6_;
    TYPE7 cols7_;
    TYPE8 cols8_;
    TYPE9 cols9_;
    TYPE10 cols10_;
    TYPE11 cols11_;
    TYPE12 cols12_;
    TYPE13 cols13_;
    TYPE14 cols14_;
    TYPE15 cols15_;
    TYPE16 cols16_;
    TYPE17 cols17_;
    TYPE18 cols18_;
    TYPE19 cols19_;
  } st;

  st s;

  inline void initializePointers() {
    if(first_) {
      st str;
      if(0 < N) cols_[0] = (byte*)&str.cols0_ - (byte*)&str;
      if(1 < N) cols_[1] = (byte*)&str.cols1_ - (byte*)&str;
      if(2 < N) cols_[2] = (byte*)&str.cols2_ - (byte*)&str;
      if(3 < N) cols_[3] = (byte*)&str.cols3_ - (byte*)&str;
      if(4 < N) cols_[4] = (byte*)&str.cols4_ - (byte*)&str;
      if(5 < N) cols_[5] = (byte*)&str.cols5_ - (byte*)&str;
      if(6 < N) cols_[6] = (byte*)&str.cols6_ - (byte*)&str;
      if(7 < N) cols_[7] = (byte*)&str.cols7_ - (byte*)&str;
      if(8 < N) cols_[8] = (byte*)&str.cols8_ - (byte*)&str;
      if(9 < N) cols_[9] = (byte*)&str.cols9_ - (byte*)&str;
      if(10 < N) cols_[10] = (byte*)&str.cols10_ - (byte*)&str;
      if(11 < N) cols_[11] = (byte*)&str.cols11_ - (byte*)&str;
      if(12 < N) cols_[12] = (byte*)&str.cols12_ - (byte*)&str;
      if(13 < N) cols_[13] = (byte*)&str.cols13_ - (byte*)&str;
      if(14 < N) cols_[14] = (byte*)&str.cols14_ - (byte*)&str;
      if(15 < N) cols_[15] = (byte*)&str.cols15_ - (byte*)&str;
      if(16 < N) cols_[16] = (byte*)&str.cols16_ - (byte*)&str;
      if(17 < N) cols_[17] = (byte*)&str.cols17_ - (byte*)&str;
      if(18 < N) cols_[18] = (byte*)&str.cols18_ - (byte*)&str;
      if(19 < N) cols_[19] = (byte*)&str.cols19_ - (byte*)&str;

      if(0 < N) size_[0] = sizeof(str.cols0_);
      if(1 < N) size_[1] = sizeof(str.cols1_);
      if(2 < N) size_[2] = sizeof(str.cols2_);
      if(3 < N) size_[3] = sizeof(str.cols3_);
      if(4 < N) size_[4] = sizeof(str.cols4_);
      if(5 < N) size_[5] = sizeof(str.cols5_);
      if(6 < N) size_[6] = sizeof(str.cols6_);
      if(7 < N) size_[7] = sizeof(str.cols7_);
      if(8 < N) size_[8] = sizeof(str.cols8_);
      if(9 < N) size_[9] = sizeof(str.cols9_);
      if(10 < N) size_[10] = sizeof(str.cols10_);
      if(11 < N) size_[11] = sizeof(str.cols11_);
      if(12 < N) size_[12] = sizeof(str.cols12_);
      if(13 < N) size_[13] = sizeof(str.cols13_);
      if(14 < N) size_[14] = sizeof(str.cols14_);
      if(15 < N) size_[15] = sizeof(str.cols15_);
      if(16 < N) size_[16] = sizeof(str.cols16_);
      if(17 < N) size_[17] = sizeof(str.cols17_);
      if(18 < N) size_[18] = sizeof(str.cols18_);
      if(19 < N) size_[19] = sizeof(str.cols19_);

      first_ = 0;
    }
  }

  };

  template<int N, class TYPE0,
    class TYPE1, class TYPE2, class TYPE3, class TYPE4,
    class TYPE5, class TYPE6, class TYPE7, class TYPE8,
    class TYPE9, class TYPE10,
    class TYPE11, class TYPE12, class TYPE13, class TYPE14,
    class TYPE15, class TYPE16, class TYPE17, class TYPE18,
    class TYPE19>
  short StaticTuple<N,TYPE0,TYPE1,TYPE2,TYPE3,TYPE4,
    TYPE5,TYPE6,TYPE7,TYPE8,TYPE9,TYPE10,TYPE11,TYPE12,TYPE13,TYPE14,
    TYPE15,TYPE16,TYPE17,TYPE18,TYPE19>::cols_[N];
  template<int N, class TYPE0,
    class TYPE1, class TYPE2, class TYPE3, class TYPE4,
    class TYPE5, class TYPE6, class TYPE7, class TYPE8,
    class TYPE9, class TYPE10,
    class TYPE11, class TYPE12, class TYPE13, class TYPE14,
    class TYPE15, class TYPE16, class TYPE17, class TYPE18,
    class TYPE19>
  byte_off_t StaticTuple<N,TYPE0,TYPE1,TYPE2,TYPE3,TYPE4,
    TYPE5,TYPE6,TYPE7,TYPE8,TYPE9,TYPE10,TYPE11,TYPE12,TYPE13,TYPE14,
    TYPE15,TYPE16,TYPE17,TYPE18,TYPE19>::size_[N];
  template<int N, class TYPE0,
    class TYPE1, class TYPE2, class TYPE3, class TYPE4,
    class TYPE5, class TYPE6, class TYPE7, class TYPE8,
    class TYPE9, class TYPE10,
    class TYPE11, class TYPE12, class TYPE13, class TYPE14,
    class TYPE15, class TYPE16, class TYPE17, class TYPE18,
    class TYPE19>
  bool StaticTuple<N,TYPE0,TYPE1,TYPE2,TYPE3,TYPE4,
    TYPE5,TYPE6,TYPE7,TYPE8,TYPE9,TYPE10,TYPE11,TYPE12,TYPE13,TYPE14,
    TYPE15,TYPE16,TYPE17,TYPE18,TYPE19>::first_ = true;

}
#endif  // _ROSE_COMPRESSION_STATICTUPLE_H__
