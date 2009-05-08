#ifndef _LSMITERATORS_H__
#define _LSMITERATORS_H__

#include "stasis/page.h"
#include "stasis/bufferManager.h"
#include "stasis/page/compression/compression.h"
#include "stasis/page/compression/tuple.h"
#include "stasis/operations.h"
/**
   @file

   This file contains a number of C++ STL-style iterators that are
   used during the LSM tree merges.

*/

/**
   @todo get rid of these undefs once compensation.h has been removed...
 */
#undef end
#undef begin

namespace rose {

template <class ITERA, class ITERB, class ROW> class mergeIterator;

template <class ITERA, class ITERB, class ROW>
inline const byte * toByteArray(mergeIterator<ITERA,ITERB,ROW> * const t);


template <class ITER, class ROW> class versioningIterator;

template <class ITER, class ROW>
inline const byte * toByteArray(versioningIterator<ITER,ROW> * const t);


template <class STLITER, class ROW> class stlSetIterator;
template <class STLITER, class ROW>
inline const byte * toByteArray(stlSetIterator<STLITER,ROW> * const t);



/**
   Scans over another iterator, checking for tombstones, and garbage collecting old tuples.
 */
template <class ROW, class ITER>
class gcIterator {
 public:
  explicit gcIterator(ITER * i, ITER * iend, epoch_t beginning_of_time, column_number_t ts_col)
    : i_(i),
    newest_(),
    current_(),
    have_newest_(0),
    have_current_(0),
    went_back_(0),
    at_end_(0),
    //newTS_(-1),
    iend_(iend),
    freeIt(0),
    beginning_of_time_(beginning_of_time),
    ts_col_(ts_col) {
      if(*i_ != *iend_) {
        get_next();
        if(have_newest_) {
          have_current_ = true; // needed by ++.
          ++(*this);
          // 	assert(have_current_); // Should pass; commented out for perf.
        }
      } else {
        at_end_=true;
      }
    }
  explicit gcIterator()
    : i_(0),
    newest_(),
    current_(),
    have_newest_(false),
    have_current_(false),
    at_end_(true),
    freeIt(0),
    beginning_of_time_(0),
    ts_col_(0) {}

  explicit gcIterator(gcIterator& t)
    : i_(new ITER(*(t.i_))),
    newest_(t.newest_),
    current_(t.current_),
    have_newest_(t.have_newest_),
    have_current_(t.have_current_),
    went_back_(t.went_back_),
    at_end_(t.at_end_),
    iend_(t.iend_),
    freeIt(1),
    beginning_of_time_(t.beginning_of_time_),
    ts_col_(t.ts_col_) { }

  ~gcIterator() {
    if (freeIt) {
      delete i_;
    }
  }
  ROW & operator*() {
    // Both should pass, comment out for perf
    //assert(!went_back_);
    //assert(have_current_);
    return current_;
  }
  bool get_next() {
    //    assert(!went_back_);
    //    assert(!at_end_);
    while(!have_newest_) {
      have_newest_ = true;
      newest_ = **i_;
      if(ts_col_ != INVALID_COL) {
	epoch_t newest_time = *(epoch_t*)newest_.get(ts_col_);
	while(1) {
	  ++(*i_);
	  if(*i_ == *iend_) { at_end_=true; return true; }
	  if(!myTupCmp(newest_,**i_)) { break; }
	  if(newest_time >= beginning_of_time_) { break; }

	  epoch_t this_time = *(epoch_t*)(**i_).get(ts_col_);
	  if(this_time > newest_time) {
	    newest_= **i_;
	    newest_time = this_time;
	  }
	}
	// is it a tombstone we can forget?
	if (newest_time & 0x1 && newest_time < beginning_of_time_) {
	  have_newest_ = 0;
	}
      } else {
	++(*i_);
	if(*i_ == *iend_) { at_end_=true; return false; }
      }
    }
    return true; // newest_;
  }
  inline bool operator==(const gcIterator &a) const {
    //    return (*i_) == (*a.i_);
    if((!have_current_) && at_end_) { return a.at_end_; }
    return false;
  }
  inline bool operator!=(const gcIterator &a) const {
    //    return (*i_) != (*a.i_);
    return !(*this == a);
  }
  inline void operator++() {
    if(went_back_) {
      went_back_ = false;
    } else {
      //      assert(have_current_);
      if(have_newest_) {
	current_ = newest_;
	have_current_ = have_newest_;
	have_newest_ = false;
	if(!at_end_) {
	  get_next();
	}
      } else {
	// assert(at_end_);
	have_current_ = false;
      }
    }
  }
  inline void operator--() {
    //    assert(!went_back_);
    went_back_ = true;
  }
  /*  inline gcIterator* end() {
    return new gcIterator(i_->end());
    } */
 private:
  bool myTupCmp(const ROW &a, const ROW &b) {
    /*    for(int i = 0; i < cnt; i++) {
      if(a.get(i) != b.get(i)) {
	return 0;
      }
      }*/
    if(ROW::NN > 0) if(*a.get0() != *b.get0()) { if(0 != ts_col_) return 0; }
    if(ROW::NN > 1) if(*a.get1() != *b.get1()) { if(1 != ts_col_) return 0; }
    if(ROW::NN > 2) if(*a.get2() != *b.get2()) { if(2 != ts_col_) return 0; }
    if(ROW::NN > 3) if(*a.get3() != *b.get3()) { if(3 != ts_col_) return 0; }
    if(ROW::NN > 4) if(*a.get4() != *b.get4()) { if(4 != ts_col_) return 0; }
    if(ROW::NN > 5) if(*a.get5() != *b.get5()) { if(5 != ts_col_) return 0; }
    if(ROW::NN > 6) if(*a.get6() != *b.get6()) { if(6 != ts_col_) return 0; }
    if(ROW::NN > 7) if(*a.get7() != *b.get7()) { if(7 != ts_col_) return 0; }
    if(ROW::NN > 8) if(*a.get8() != *b.get8()) { if(8 != ts_col_) return 0; }
    if(ROW::NN > 9) if(*a.get9() != *b.get9()) { if(9 != ts_col_) return 0; }
    if(ROW::NN > 10) if(*a.get10() != *b.get10()) { if(10 != ts_col_) return 0; }
    if(ROW::NN > 11) if(*a.get11() != *b.get11()) { if(11 != ts_col_) return 0; }
    if(ROW::NN > 12) if(*a.get12() != *b.get12()) { if(12 != ts_col_) return 0; }
    if(ROW::NN > 13) if(*a.get13() != *b.get13()) { if(13 != ts_col_) return 0; }
    if(ROW::NN > 14) if(*a.get14() != *b.get14()) { if(14 != ts_col_) return 0; }
    if(ROW::NN > 15) if(*a.get15() != *b.get15()) { if(15 != ts_col_) return 0; }
    if(ROW::NN > 16) if(*a.get16() != *b.get16()) { if(16 != ts_col_) return 0; }
    if(ROW::NN > 17) if(*a.get17() != *b.get17()) { if(17 != ts_col_) return 0; }
    if(ROW::NN > 18) if(*a.get18() != *b.get18()) { if(18 != ts_col_) return 0; }
    if(ROW::NN > 19) if(*a.get19() != *b.get19()) { if(19 != ts_col_) return 0; }
    return 1;
  }

  //explicit gcIterator() { abort(); }
  void operator=(gcIterator & t) { abort(); }
  int operator-(gcIterator & t) { abort(); }
  ITER * i_;
  ROW newest_;
  ROW current_;
  bool have_newest_;
  bool have_current_;
  bool went_back_;
  bool at_end_;
  //  epoch_t newTS_;
  ITER * iend_;
  bool freeIt;
  epoch_t beginning_of_time_;
  column_number_t ts_col_;
};

//---------------------------------------------------------------------------


/**
   Scans through an LSM tree's leaf pages, each tuple in the tree, in
   order.  This iterator is designed for maximum forward scan
   performance, and does not support all STL operations.
 */
template <class ROW, class PAGELAYOUT>
class treeIterator {
 private:
  inline void init_helper() {
    if(!lsmIterator_) {
      currentPage_ = 0;
      pageid_ = -1;
      p_ = 0;
    } else if(!lsmTreeIterator_next(-1, lsmIterator_)) {
      currentPage_ = 0;
      pageid_ = -1;
      p_ = 0;
    } else {
      pageid_t * pid_tmp;
      pageid_t ** hack = &pid_tmp;
      lsmTreeIterator_value(-1,lsmIterator_,(byte**)hack);
      pageid_ = *pid_tmp;
      p_ = loadPage(-1,pageid_);
      readlock(p_->rwlatch,0);
      currentPage_ = (PAGELAYOUT*)p_->impl;
      assert(currentPage_);
    }
  }
 public:
    //  typedef recordid handle;
    class treeIteratorHandle {
    public:
      treeIteratorHandle() : r_(NULLRID) {}
      treeIteratorHandle(const recordid r) : r_(r) {}
      /*      const treeIteratorHandle & operator=(const recordid *r) {
	r_ = *r;
	return thisopenat;
	} */
      treeIteratorHandle * operator=(const recordid &r) {
	r_ = r;
	return this;
      }

     recordid r_;
    };
    typedef treeIteratorHandle* handle;
  explicit treeIterator(treeIteratorHandle* tree, ROW& key) :
    tree_(tree?tree->r_:NULLRID),
    scratch_(),
    keylen_(ROW::sizeofBytes()),
    lsmIterator_(lsmTreeIterator_openAt(-1,tree?tree->r_:NULLRID,key.toByteArray())),
    slot_(0)
  {
    init_helper();
    if(lsmIterator_) {
      treeIterator * end = this->end();
      for(;*this != *end && **this < key; ++(*this)) { }
      delete end;
    } else {
      this->slot_ = 0;
      this->pageid_ = 0;
    }
  }
  explicit treeIterator(recordid tree, ROW &scratch, int keylen) :
    tree_(tree),
    scratch_(scratch),
    keylen_(keylen),
    lsmIterator_(lsmTreeIterator_open(-1,tree)),
    slot_(0)
  {
    init_helper();
  }
  explicit treeIterator(recordid tree) :
    tree_(tree),
      scratch_(),
      keylen_(ROW::sizeofBytes()),
      lsmIterator_(lsmTreeIterator_open(-1,tree)),
      slot_(0)
    {
      init_helper();
    }
  explicit treeIterator(treeIteratorHandle* tree) :
    tree_(tree?tree->r_:NULLRID),
      scratch_(),
      keylen_(ROW::sizeofBytes()),
      lsmIterator_(lsmTreeIterator_open(-1,tree?tree->r_:NULLRID)),
      slot_(0)
    {
      init_helper();
    }
  explicit treeIterator(treeIterator& t) :
    tree_(t.tree_),
    scratch_(t.scratch_),
    keylen_(t.keylen_),
    lsmIterator_(t.lsmIterator_?lsmTreeIterator_copy(-1,t.lsmIterator_):0),
    slot_(t.slot_),
    pageid_(t.pageid_),
    p_((Page*)((t.p_)?loadPage(-1,t.p_->id):0)),
    currentPage_((PAGELAYOUT*)((p_)?p_->impl:0)) {
      if(p_) { readlock(p_->rwlatch,0); }
  }
  ~treeIterator() {
    if(lsmIterator_) {
      lsmTreeIterator_close(-1, lsmIterator_);
    }
    if(p_) {
      unlock(p_->rwlatch);
      releasePage(p_);
      p_ = 0;
    }
  }
  ROW & operator*() {
    assert(this->lsmIterator_);
    ROW* readTuple = currentPage_->recordRead(-1,slot_, &scratch_);

    if(!readTuple) {
      unlock(p_->rwlatch);
      releasePage(p_);
      p_=0;
      if(lsmTreeIterator_next(-1,lsmIterator_)) {
        pageid_t *pid_tmp;

        slot_ = 0;
        pageid_t **hack = &pid_tmp;
        lsmTreeIterator_value(-1,lsmIterator_,(byte**)hack);
        pageid_ = *pid_tmp;
        p_ = loadPage(-1,pageid_);
        readlock(p_->rwlatch,0);
        currentPage_ = (PAGELAYOUT*)p_->impl;

        readTuple = currentPage_->recordRead(-1,slot_, &scratch_);
	//        assert(readTuple);
      } else {
        // past end of iterator!  "end" should contain the pageid of the
        // last leaf, and 1+ numslots on that page.
        abort();
      }
    }
    /*    for(int c = 0; c < (scratch_).column_count(); c++) {
      assert(*(byte*)(scratch_).get(c) || !*(byte*)(scratch_).get(c));
      } */
    return scratch_;
  }
  inline bool operator==(const treeIterator &a) const {
    return (slot_ == a.slot_ && pageid_ == a.pageid_)/* || !(lsmIterator_ && a.lsmIterator_)*/ ;
  }
  inline bool operator!=(const treeIterator &a) const {
    return !(*this==a);
  }
  inline void operator++() {
    slot_++;
  }
  inline void operator--() {
    // This iterator consumes its input, and only partially supports
    // "==". "--" is just for book keeping, so we don't need to worry
    // about setting the other state.
    slot_--;
  }
  inline treeIterator* end() {
    treeIterator* t = new treeIterator(tree_,scratch_,keylen_);
    if(!lsmIterator_) {
      t->slot_ = 0;
      t->pageid_ = 0;
      return t;
    }
    if(t->p_) {
      unlock(t->p_->rwlatch);
      releasePage(t->p_);
      t->p_=0;
    }
    t->currentPage_ = 0;

    pageid_t pid = TlsmLastPage(-1,tree_);
    if(pid != -1) {
      t->pageid_= pid;
      Page * p = loadPage(-1, t->pageid_);
      readlock(p->rwlatch,0);
      PAGELAYOUT * lastPage = (PAGELAYOUT*)p->impl;
      t->slot_ = 0;
      while(lastPage->recordRead(-1,t->slot_,&scratch_)) { t->slot_++; }
      unlock(p->rwlatch);
      releasePage(p);
    } else {
      // begin == end already; we're done.
    }
    return t;
  }
 private:
  explicit treeIterator() { abort(); }
  void operator=(treeIterator & t) { abort(); }
  int operator-(treeIterator & t) { abort(); }
  recordid tree_;
  ROW scratch_;
  int keylen_;
  lladdIterator_t * lsmIterator_;
  slot_index_t slot_;
  pageid_t pageid_;
  Page * p_;
  PAGELAYOUT * currentPage_;
};

/**
   This iterator takes two otehr iterators as arguments, and merges
   their output, dropping duplicate entries.

   It does not understand versioning or tombstones.
 */
template<class ITERA, class ITERB, class ROW>
class mergeIterator {
 private:
  static const int A = 0;
  static const int B = 1;
  static const int NONE = -1;
  static const int BOTH = -2;

  inline  int calcCurr(int oldcur) {
    int cur;
    if(oldcur == NONE) { return NONE; }
    if(a_ == aend_) {
      if(b_ == bend_) {
        cur = NONE;
      } else {
        cur = B;
      }
    } else {
      if(b_ == bend_) {
        cur = A;
      } else {
        if((*a_) < (*b_)) {
          cur = A;
        } else if((*a_) == (*b_)) {
          cur = BOTH;
        } else {
          cur = B;
        }
      }
    }
    return cur;
  }
 public:
  mergeIterator(ITERA & a, ITERB & b, ITERA & aend, ITERB & bend) :
    off_(0),
    a_(a),
    b_(b),
    aend_(aend),
    bend_(bend),
    curr_(calcCurr(A)),
    before_eof_(0)
  {}
  explicit mergeIterator(mergeIterator &i) :
    off_(i.off_),
    a_(i.a_),
    b_(i.b_),
    aend_(i.aend_),
    bend_(i.bend_),
    curr_(i.curr_),
    before_eof_(i.before_eof_)
  { }

  const ROW& operator* () {
    if(curr_ == A) { return *a_; }
    if(curr_ == B || curr_ == BOTH) { return *b_; }
    abort();
    curr_ = calcCurr(A);
    if(curr_ == A) { return *a_; }
    if(curr_ == B || curr_ == BOTH) { return *b_; }
    abort();
  }
  void seekEnd() {
    curr_ = NONE;
  }
  // XXX Only works if exactly one of the comparators is derived from end.
  inline bool operator==(const mergeIterator &o) const {
    if(curr_ == NONE && o.curr_ == NONE) {
      return 1;
    } else if(curr_ != NONE && o.curr_ != NONE) {
      return (a_ == o.a_) && (b_ == o.b_);
    }
    return 0;
  }
  inline bool operator!=(const mergeIterator &o) const {
    return !(*this == o);
  }
  inline void operator++() {
    off_++;
    if(curr_ == BOTH) {
      ++a_;
      ++b_;
    } else {
      if(curr_ == A) { ++a_; }
      if(curr_ == B) { ++b_; }
    }
    curr_ = calcCurr(curr_);
  }
  inline void operator--() {
    off_--;
    if(curr_ == BOTH) {
      --a_;
      --b_;
    } else {
      if(curr_ == A) { --a_; }
      if(curr_ == B) { --b_; }
    }
    if(curr_ == NONE) {
      before_eof_ = 1;
    } else {
      before_eof_ = 0;
    }
  }
  inline int  operator-(mergeIterator&i) {
    return off_ - i.off_;
  }
  inline void operator=(mergeIterator const &i) {
    off_ = i.off_;
    a_ = i.a_;
    b_ = i.b_;
    aend_ = i.aend_;
    bend_ = i.bend_;
    curr_ = i.curr_;
    before_eof_ = i.before_eof;
  }
  inline unsigned int offset() { return off_; }
 private:
  unsigned int off_;
  ITERA a_;
  ITERB b_;
  ITERA aend_;
  ITERB bend_;
  int curr_;
  int before_eof_;
  friend const byte*
    toByteArray<ITERA,ITERB,ROW>(mergeIterator<ITERA,ITERB,ROW> * const t);
};

/**
   This iterator takes an iterator that produces rows with versioning
   information.  The rows should be sorted based on value, then sorted by
   version, with the newest value first.
 */
template<class ITER, class ROW>
class versioningIterator {
 public:
   versioningIterator(ITER & a, ITER & aend,
		      int beginning_of_time) :
    a_(a),
    aend_(aend),
    check_tombstone_(0),
    tombstone_(),
    off_(0)
  {}
  explicit versioningIterator(versioningIterator &i) :
    a_(i.a_),
    aend_(i.aend_),
    check_tombstone_(i.check_tombstone_),
    tombstone_(i.tombstone_),
    off_(i.off_)
  {}

  const ROW& operator* () {
    return *a_;
  }
  void seekEnd() {
    a_.seekEnd();// = aend_; // XXX good idea?
  }
  inline bool operator==(const versioningIterator &o) const {
    return a_ == o.a_;
  }
  inline bool operator!=(const versioningIterator &o) const {
    return !(*this == o);
  }
  inline void operator++() {
    if(check_tombstone_) {
      do {
	++a_;
      } while(a_ != aend_ && *a_ == tombstone_);
    } else {
      ++a_;
    }
    if(a_ != aend_ && (*a_).tombstone()) {
      tombstone_.copyFrom(*a_);
      check_tombstone_ = 1;
    } else {
      check_tombstone_ = 0;
    }
    off_++;
  }
  inline void operator--() {
    --a_;
    // need to remember that we backed up so that ++ can work...
    // the cursor is always positioned on a live value, and -- can
    // only be followed by ++, so this should do the right thing.
    check_tombstone_ = 0;
    off_--;
  }
  inline int  operator-(versioningIterator&i) {
    return off_ - i.off_;
  }
  inline void operator=(versioningIterator const &i) {
    a_ = i.a_;
    aend_ = i.aend_;
    check_tombstone_ = i.check_tombstone_;
    tombstone_ = i.tombstone_;
    //    scratch_ = *a_;
    off_ = i.off_;
  }
  inline unsigned int offset() { return off_; }
 private:
  ITER a_;
  ITER aend_;
  int check_tombstone_;
  ROW tombstone_;
  off_t off_;
  friend const byte*
    toByteArray<ITER,ROW>(versioningIterator<ITER,ROW> * const t);
};

/**
   This iterator takes an iterator that produces rows with versioning
   information.  The rows should be sorted based on value, then sorted by
   version, with the newest value first.
 */
 template<class SET,class ROW> class stlSetIterator {
 private:
   typedef typename SET::const_iterator STLITER;
 public:
   typedef SET * handle;

   stlSetIterator( SET * s ) : it_(s->begin()), itend_(s->end()) {}
   stlSetIterator( STLITER& it, STLITER& itend ) : it_(it), itend_(itend) {}
   explicit stlSetIterator(stlSetIterator &i) : it_(i.it_), itend_(i.itend_){}
   const ROW& operator* () { return *it_; }

  void seekEnd() {
    it_ = itend_; // XXX good idea?
  }
  stlSetIterator * end() { return new stlSetIterator(itend_,itend_); }
  inline bool operator==(const stlSetIterator &o) const {
    return it_ == o.it_;
  }
  inline bool operator!=(const stlSetIterator &o) const {
    return !(*this == o);
  }
  inline void operator++() {
    ++it_;
  }
  inline void operator--() {
    --it_;
  }
  inline int  operator-(stlSetIterator&i) {
    return it_ - i.it_;
  }
  inline void operator=(stlSetIterator const &i) {
    it_ = i.it_;
    itend_ = i.itend_;
  }
 private:
  STLITER it_;
  STLITER itend_;
  friend const byte*
    toByteArray<SET,ROW>(stlSetIterator<SET,ROW> * const t);
};

template <class SET,class ROW>
inline const byte * toByteArray(stlSetIterator<SET,ROW> * const t) {
  return (*(t->it_)).toByteArray();
}
/** Produce a byte array from the value stored at t's current
    position */
template <class ITERA, class ITERB, class ROW>
  inline const byte * toByteArray(mergeIterator<ITERA,ITERB,ROW> * const t) {
  if(t->curr_ == t->A) {
    return toByteArray(&t->a_);
  } else if(t->curr_ == t->B || t->curr_ == t->BOTH) {
    return toByteArray(&t->b_);
  }
  abort();
}

/** Produce a byte array from the value stored at t's current
    position */
 template <class ITER, class ROW>
   inline const byte * toByteArray(versioningIterator<ITER,ROW> * const t) {
   return toByteArray(&t->a_);
 }

#ifdef DEFINED_VAL_T
template <class PAGELAYOUT>
inline const byte* toByteArray(treeIterator<val_t,PAGELAYOUT> *const t) {
  return (const byte*)&(**t);
}
#endif
template <class PAGELAYOUT,class ROW>
inline const byte* toByteArray(treeIterator<ROW,PAGELAYOUT> *const t) {
  return (**t).toByteArray();
}

}
#endif // _LSMITERATORS_H__
