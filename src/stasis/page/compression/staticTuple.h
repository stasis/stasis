#ifndef _ROSE_COMPRESSION_STATICTUPLE_H__
#define _ROSE_COMPRESSION_STATICTUPLE_H__

namespace rose {

template<int N,
  typename TYPE0, class TYPE1, class TYPE2, class TYPE3, class TYPE4,
  class TYPE5, class TYPE6, class TYPE7, class TYPE8, class TYPE9>
class StaticTuple {
 public:
  explicit inline StaticTuple() {
    initializePointers();
  }

  explicit inline StaticTuple(StaticTuple& t) {
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

  inline void* set(column_number_t col, void* val) {
    memcpy(cols_[col],val,size_[col]);
    return(cols_[col]);
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

  inline const void* get(column_number_t col) const {
    return cols_[col];
  }
  inline column_number_t column_count() const { return N; }

  inline byte_off_t column_len(column_number_t col) const {
    return size_[col];
  }
  inline byte* toByteArray() {
    return &s;
  }
  inline bool operator==(StaticTuple &t) {
    return s == t.s;
  }
  inline bool operator<(StaticTuple &t) {
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

  explicit StaticTuple(const StaticTuple& t) { abort(); }

  void * cols_[N];
  size_t size_[N];
  struct {
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
  } s;

  inline void initializePointers() {
    if(0 < N) cols_[0] = &s.cols0_;
    if(1 < N) cols_[1] = &s.cols1_;
    if(2 < N) cols_[2] = &s.cols2_;
    if(3 < N) cols_[3] = &s.cols3_;
    if(4 < N) cols_[4] = &s.cols4_;
    if(5 < N) cols_[5] = &s.cols5_;
    if(6 < N) cols_[6] = &s.cols6_;
    if(7 < N) cols_[7] = &s.cols7_;
    if(8 < N) cols_[8] = &s.cols8_;
    if(9 < N) cols_[9] = &s.cols9_;

    if(0 < N) size_[0] = sizeof(s.cols0_);
    if(1 < N) size_[1] = sizeof(s.cols1_);
    if(2 < N) size_[2] = sizeof(s.cols2_);
    if(3 < N) size_[3] = sizeof(s.cols3_);
    if(4 < N) size_[4] = sizeof(s.cols4_);
    if(5 < N) size_[5] = sizeof(s.cols5_);
    if(6 < N) size_[6] = sizeof(s.cols6_);
    if(7 < N) size_[7] = sizeof(s.cols7_);
    if(8 < N) size_[8] = sizeof(s.cols8_);
    if(9 < N) size_[9] = sizeof(s.cols9_);
  }
};

}

#endif  // _ROSE_COMPRESSION_STATICTUPLE_H__
