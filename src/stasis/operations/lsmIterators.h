#ifndef _LSMITERATORS_H__
#define _LSMITERATORS_H__

#include "stasis/page.h"
#include "stasis/bufferManager.h"
#include "stasis/page/compression/compression.h"
#include "stasis/page/compression/tuple.h"

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
   Scans through an LSM tree's leaf pages, each tuple in the tree, in
   order.  This iterator is designed for maximum forward scan
   performance, and does not support all STL operations.
 */
template <class ROW, class PAGELAYOUT>
class treeIterator {
 private:
  inline void init_helper() {
    if(!lsmTreeIterator_next(-1, lsmIterator_)) {
      currentPage_ = 0;
      pageid_ = -1;
      p_ = 0;
    } else {
      pageid_t * pid_tmp;
      lsmTreeIterator_value(-1,lsmIterator_,(byte**)&pid_tmp);
      pageid_ = *pid_tmp;
      p_ = loadPage(-1,pageid_);
      currentPage_ = (PAGELAYOUT*)p_->impl;
    }
  }
 public:
  explicit treeIterator(recordid tree, ROW &scratch, int keylen) :
    tree_(tree),
    scratch_(scratch),
    keylen_(keylen),
    lsmIterator_(lsmTreeIterator_open(-1,tree)),
    slot_(0)
  {
    init_helper();
  }
    //  typedef recordid handle;
    class treeIteratorHandle {
    public:
      treeIteratorHandle() : r_(NULLRID) {}
      treeIteratorHandle(const recordid r) : r_(r) {}
      /*      const treeIteratorHandle & operator=(const recordid *r) {
	r_ = *r;
	return this;
	} */
      treeIteratorHandle * operator=(const recordid &r) {
	r_ = r;
	return this;
      }

     recordid r_;
    };
    typedef treeIteratorHandle* handle;
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
    tree_(tree->r_),
      scratch_(),
      keylen_(ROW::sizeofBytes()),
      lsmIterator_(lsmTreeIterator_open(-1,tree->r_)),
      slot_(0)
    {
      init_helper();
    }
  explicit treeIterator(treeIterator& t) :
    tree_(t.tree_),
    scratch_(t.scratch_),
    keylen_(t.keylen_),
    lsmIterator_(lsmTreeIterator_copy(-1,t.lsmIterator_)),
    slot_(t.slot_),
    pageid_(t.pageid_),
    p_((Page*)((t.p_)?loadPage(-1,t.p_->id):0)),
    currentPage_((PAGELAYOUT*)((p_)?p_->impl:0)) {
  }
  ~treeIterator() {

    lsmTreeIterator_close(-1, lsmIterator_);
    if(p_) {
      releasePage(p_);
      p_ = 0;
    }
  }
  ROW & operator*() {
    ROW* readTuple = currentPage_->recordRead(-1,slot_, &scratch_);

    if(!readTuple) {
      releasePage(p_);
      p_=0;
      if(lsmTreeIterator_next(-1,lsmIterator_)) {
        pageid_t *pid_tmp;

        slot_ = 0;

        lsmTreeIterator_value(-1,lsmIterator_,(byte**)&pid_tmp);
        pageid_ = *pid_tmp;
        p_ = loadPage(-1,pageid_);

        currentPage_ = (PAGELAYOUT*)p_->impl;

        readTuple = currentPage_->recordRead(-1,slot_, &scratch_);
        assert(readTuple);
      } else {
        // past end of iterator!  "end" should contain the pageid of the
        // last leaf, and 1+ numslots on that page.
        abort();
      }
    }
    return scratch_;
  }
  inline bool operator==(const treeIterator &a) const {
    return (slot_ == a.slot_ && pageid_ == a.pageid_);
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
    if(t->p_) {
      releasePage(t->p_);
      t->p_=0;
    }
    t->currentPage_ = 0;

    pageid_t pid = TlsmLastPage(-1,tree_);
    if(pid != -1) {
      t->pageid_= pid;
      Page * p = loadPage(-1, t->pageid_);
      PAGELAYOUT * lastPage = (PAGELAYOUT*)p->impl;
      t->slot_ = 0;
      while(lastPage->recordRead(-1,t->slot_,&scratch_)) { t->slot_++; }
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
    if(curr_ == A || curr_ == BOTH) { return *a_; }
    if(curr_ == B) { return *b_; }
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
    tombstone_(0),
    off_(0)
  {}
  explicit versioningIterator(versioningIterator &i) :
    a_(i.a_),
    aend_(i.aend_),
    check_tombstone_(i.check_tombstone_),
    tombstone_(i.tombstone_),
    off_(i.off_)
  {}

  ROW& operator* () {
    return *a_;
  }
  void seekEnd() {
    a_ = aend_; // XXX good idea?
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
    if((*a_).tombstone()) {
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
  //  unsigned int off_;
  ITER a_;
  ITER aend_;
  int check_tombstone_;
  ROW tombstone_;
  //  ROW &scratch_;
  off_t off_;
  //  int before_eof_;
  //  typeof(ROW::TIMESTAMP) beginning_of_time_;
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
   typedef typename SET::iterator STLITER;
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
  //  inline unsigned int offset() { return off_; }
 private:
  //  unsigned int off_;
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
  if(t->curr_ == t->A || t->curr_ == t->BOTH) {
    return toByteArray(&t->a_);
  } else if(t->curr_ == t->B) {
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

template <class PAGELAYOUT>
inline const byte* toByteArray(treeIterator<int,PAGELAYOUT> *const t) {
  return (const byte*)&(**t);
}
template <class PAGELAYOUT,class ROW>
inline const byte* toByteArray(treeIterator<ROW,PAGELAYOUT> *const t) {
  return (**t).toByteArray();
}

}
#endif // _LSMITERATORS_H__
