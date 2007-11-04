#ifndef _ROSE_COMPRESSION_STATICTUPLE_H__
#define _ROSE_COMPRESSION_STATICTUPLE_H__

namespace rose {
  template<int N, class TYPE0,
    class TYPE1=bool, class TYPE2=bool, class TYPE3=bool, class TYPE4=bool,
    class TYPE5=bool, class TYPE6=bool, class TYPE7=bool, class TYPE8=bool,
    class TYPE9=bool>
    class StaticTuple {
  public:
  static const char NORMAL = 0;
  static const char TOMBSTONE = 1;
  static const int TUPLE_ID = 1;

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


  explicit inline StaticTuple() {
    s.flag_ = NORMAL;
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
    initializePointers();
  }

  inline ~StaticTuple() { }

  static inline byte_off_t sizeofBytes() {
    return sizeof(flag_t) + sizeof(epoch_t) +
    ((0 < N) ? sizeof(TYPE0) : 0) +
    ((1 < N) ? sizeof(TYPE1) : 0) +
    ((2 < N) ? sizeof(TYPE2) : 0) +
    ((3 < N) ? sizeof(TYPE3) : 0) +
    ((4 < N) ? sizeof(TYPE4) : 0) +
    ((5 < N) ? sizeof(TYPE5) : 0) +
    ((6 < N) ? sizeof(TYPE6) : 0) +
    ((7 < N) ? sizeof(TYPE7) : 0) +
    ((8 < N) ? sizeof(TYPE8) : 0) +
    ((9 < N) ? sizeof(TYPE9) : 0) ;
  }

  inline void* set(column_number_t col, void* val) {
    memcpy(((byte*)&s)+cols_[col],val,size_[col]);
    return(((byte*)&s)+cols_[col]);
  }

  inline TYPE0 * set0(TYPE0* val) { s.cols0_=*val; }
  inline TYPE1 * set1(TYPE1* val) { s.cols1_=*val; }
  inline TYPE2 * set2(TYPE2* val) { s.cols2_=*val; }
  inline TYPE3 * set3(TYPE3* val) { s.cols3_=*val; }
  inline TYPE4 * set4(TYPE4* val) { s.cols4_=*val; }
  inline TYPE5 * set5(TYPE5* val) { s.cols5_=*val; }
  inline TYPE6 * set6(TYPE6* val) { s.cols6_=*val; }
  inline TYPE7 * set7(TYPE7* val) { s.cols7_=*val; }
  inline TYPE8 * set8(TYPE8* val) { s.cols8_=*val; }
  inline TYPE9 * set9(TYPE9* val) { s.cols9_=*val; }

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

  /*  inline void* get(column_number_t col) const {
    return ((byte*)&s) + cols_[col];
    } */
  inline column_number_t column_count() const { return N; }

  inline byte_off_t column_len(column_number_t col) const {
    return size_[col];
  }
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
    return 0;
  }

  static inline int cmp(const void *ap, const void *bp) {
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

  }

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
    flag_t flag_ : 1;
    epoch_t epoch_ : 31;
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

      first_ = 0;
    }
  }

  };

  template<int N, class TYPE0,
    class TYPE1, class TYPE2, class TYPE3, class TYPE4,
    class TYPE5, class TYPE6, class TYPE7, class TYPE8,
    class TYPE9>
  short StaticTuple<N,TYPE0,TYPE1,TYPE2,TYPE3,TYPE4,
    TYPE5,TYPE6,TYPE7,TYPE8,TYPE9>::cols_[N];
  template<int N, class TYPE0,
    class TYPE1, class TYPE2, class TYPE3, class TYPE4,
    class TYPE5, class TYPE6, class TYPE7, class TYPE8,
    class TYPE9>
  byte_off_t StaticTuple<N,TYPE0,TYPE1,TYPE2,TYPE3,TYPE4,
    TYPE5,TYPE6,TYPE7,TYPE8,TYPE9>::size_[N];
  template<int N, class TYPE0,
    class TYPE1, class TYPE2, class TYPE3, class TYPE4,
    class TYPE5, class TYPE6, class TYPE7, class TYPE8,
    class TYPE9>
  bool StaticTuple<N,TYPE0,TYPE1,TYPE2,TYPE3,TYPE4,
    TYPE5,TYPE6,TYPE7,TYPE8,TYPE9>::first_ = true;

}
#endif  // _ROSE_COMPRESSION_STATICTUPLE_H__
