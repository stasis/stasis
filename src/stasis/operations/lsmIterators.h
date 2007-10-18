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

template <class ITER, class ROW> class mergeIterator;

template <class ITER, class ROW>
inline const byte * toByteArray(mergeIterator<ITER,ROW> * const t);


/**
   Scans through an LSM tree's leaf pages, each tuple in the tree, in
   order.  This iterator is designed for maximum forward scan
   performance, and does not support all STL operations.
 */
template <class ROW, class PAGELAYOUT>
class treeIterator {
 public:
  explicit treeIterator(recordid tree, ROW &scratch, int keylen) :
    tree_(tree),
    scratch_(scratch),
    keylen_(keylen),
    lsmIterator_(lsmTreeIterator_open(-1,tree)),
    slot_(0)
  {
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
  ROW & scratch_;
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

   @todo LSM tree is not be very useful without support for deletion
         (and, therefore, versioning).  Such support will require
         modifications to mergeIterator (or perhaps, a new iterator
         class).
 */
template<class ITER, class ROW>
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
  mergeIterator(ITER & a, ITER & b, ITER & aend, ITER & bend) :
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

  ROW& operator* () {
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
  ITER a_;
  ITER b_;
  ITER aend_;
  ITER bend_;
  int curr_;
  int before_eof_;
  friend const byte* toByteArray<ITER,ROW>(mergeIterator<ITER,ROW> * const t);
};

/** Produce a byte array from the value stored at t's current
    position */
template <class ITER, class ROW>
inline const byte * toByteArray(mergeIterator<ITER,ROW> * const t) {
  if(t->curr_ == t->A || t->curr_ == t->BOTH) {
    return toByteArray(&t->a_);
  } else if(t->curr_ == t->B) {
    return toByteArray(&t->b_);
  }
  abort();
}

template <class PAGELAYOUT>
inline const byte* toByteArray(treeIterator<int,PAGELAYOUT> *const t) {
  return (const byte*)&(**t);
}
template <class PAGELAYOUT,class TYPE>
inline const byte* toByteArray(treeIterator<Tuple<TYPE>,PAGELAYOUT> *const t) {
  return (**t).toByteArray();
}

}
#endif // _LSMITERATORS_H__
