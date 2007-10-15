// Copyright 2007 Google Inc. All Rights Reserved.
// Author: sears@google.com (Rusty Sears)

#include <stdio.h>
#include <unistd.h>
#include <errno.h>

#include <stasis/transactional.h>

#include "stasis/page/compression/for-impl.h"
#include "stasis/page/compression/pstar-impl.h"
#include "stasis/page/compression/rle-impl.h"
#include "stasis/page/compression/multicolumn-impl.h"
#include "stasis/page/compression/tuple.h"

#undef end
#undef begin

/*
  If this is defined, then check to see that the data produced by
  decompressing the data actually matches the original dataset.
*/

#define CHECK_OUTPUT

using rose::Pstar;
using rose::Multicolumn;
using rose::Tuple;
using rose::For;
using rose::Rle;
using rose::plugin_id_t;
using rose::column_number_t;
using rose::slot_index_t;

typedef int32_t val_t;
static const int32_t GB = 1024 * 1024 * 1024;

static int lsm_sim;  // XXX this global variable shouldn't be global!


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
   The following initPage() functions initialize the page that is
   passed into them, and allocate a new PAGELAYOUT object of the
   appropriate type.
*/
template <class COMPRESSOR, class TYPE>
static Pstar<COMPRESSOR, TYPE> * initPage(Pstar<COMPRESSOR,TYPE> **pstar,
					  Page *p, TYPE current) {
  *pstar = new Pstar<COMPRESSOR, TYPE>(-1, p);
  (*pstar)->compressor()->offset(current);
  return *pstar;
}
template <class COMPRESSOR, class TYPE>
static Pstar<COMPRESSOR, TYPE> * initPage(Pstar<COMPRESSOR,TYPE> **pstar,
					  Page *p, Tuple<TYPE> & current) {
  *pstar = new Pstar<COMPRESSOR, TYPE>(-1, p);
  (*pstar)->compressor()->offset(current);
  return *pstar;
}

template <class COMPRESSOR, class TYPE >
static Multicolumn<Tuple<TYPE> > * initPage(Multicolumn<Tuple<TYPE> > ** mc,
					    Page *p, Tuple<TYPE> & t) {
  column_number_t column_count = t.column_count();
  plugin_id_t plugin_id =
    rose::plugin_id<Multicolumn<Tuple<TYPE> >,COMPRESSOR,TYPE>();

  plugin_id_t * plugins = new plugin_id_t[column_count];
  for(column_number_t c = 0; c < column_count; c++) {
    plugins[c] = plugin_id;
  }

  *mc = new Multicolumn<Tuple<TYPE> >(-1,p,column_count,plugins);
  for(column_number_t c = 0; c < column_count; c++) {
    ((COMPRESSOR*)(*mc)->compressor(c))->offset(*t.get(c));
  }

  delete [] plugins;
  return *mc;
}
template <class COMPRESSOR, class TYPE >
static Multicolumn<Tuple<TYPE> > * initPage(Multicolumn<Tuple<TYPE> > ** mc,
					    Page *p, TYPE t) {
  plugin_id_t plugin_id =
    rose::plugin_id<Multicolumn<Tuple<TYPE> >,COMPRESSOR,TYPE>();

  plugin_id_t * plugins = new plugin_id_t[1];
  plugins[0] = plugin_id;

  *mc = new Multicolumn<Tuple<TYPE> >(-1,p,1,plugins);
  ((COMPRESSOR*)(*mc)->compressor(0))->offset(t);

  delete [] plugins;
  return *mc;
}

#define FIRST_PAGE 1

/**
   Bypass stasis' allocation mechanisms.  Stasis' page allocation
   costs should be minimal, but this program doesn't support
   durability yet, and using the normal allocator would complicate
   things. */
pageid_t roseFastAlloc(int xid, void * conf) {
  static pthread_mutex_t alloc_mut = PTHREAD_MUTEX_INITIALIZER;
  int *num_pages = (int*)conf;
  pthread_mutex_lock(&alloc_mut);
  pageid_t ret = FIRST_PAGE + *num_pages;
  (*num_pages)++;
  pthread_mutex_unlock(&alloc_mut);
  return ret;
}

/*static pthread_key_t  alloc_key;
static pthread_once_t alloc_key_once;
static void alloc_setup(void) {
  pthread_once(&alloc_key_once,alloc_key_create);
  pthread_setspecific(alloc_key,malloc(sizeof(alloc_struct)));
  } */

#define INT_CMP 1
#define TUP_CMP 2
/**
    Comparators for the lsmTrees
 */
template <class TYPE>
int intCmp(const void *ap, const void *bp) {
  TYPE a = *(TYPE*)ap;
  TYPE b = *(TYPE*)bp;
  if(a<b) { return -1; }
  if(a>b) { return 1; }
  return 0;
}

template <class TYPE>
int tupCmp(const void *ap, const void *bp) {
  column_number_t count = *(column_number_t*)ap;
  TYPE * a = (TYPE*)(1+(column_number_t*)ap);
  TYPE * b = (TYPE*)(1+(column_number_t*)bp);

  for(column_number_t i = 0; i < count; i++) {
    if(a[i] < b[i]) { return -1; }
    if(a[i] > b[i]) { return  1; }
  }
  return 0;
}

/**
   cast from a TYPE to a byte *.  These were written to make it easier
   to write templates that work with different types of iterators.
*/
inline const byte * toByteArray(int const *const *const i) {
  return (const byte*)*i;
}
inline const byte * toByteArray(Tuple<val_t>::iterator * const t) {
  return (**t).toByteArray();
}

#define RAND_TUP_CHECK
#define RAND_TUP_YES 0
#define RAND_TUP_NO  1
#define RAND_TUP_NEVER -1
#define RAND_TUP_BROKE -2
/**
   Produce a stream of random tuples.  The stream is repeatable, and
   supports increment (each value in the stream must be read using
   operator*()), and decrement (at least one increment must occur
   between consecutive decrements).
 */

template <class TYPE> class randomIterator;

template <class TYPE>
inline const byte * toByteArray(randomIterator<TYPE> * const t);

template<class TYPE>
class randomIterator {
 public:
  randomIterator(unsigned int seed, unsigned int count,
		 column_number_t col_count, double bump_prob, int ret_tuple) :
    ret_tuple_(ret_tuple),
    off_(0),
    count_(count),
    col_count_(col_count),
    bump_thresh_(static_cast<long>(bump_prob*static_cast<double>(RAND_MAX))),
    random_state_(seed),
    can_deref_(RAND_TUP_YES),
    scratch_(col_count_)
  {
    TYPE val =0;
    for(column_number_t c = 0; c < col_count_; c++) {
      scratch_.set(c,&val);
    }
  }
  explicit randomIterator(const randomIterator<TYPE> &i) :
    ret_tuple_(i.ret_tuple_),
    off_(i.off_),
    count_(i.count_),
    col_count_(i.col_count_),
    bump_thresh_(i.bump_thresh_),
    random_state_(i.random_state_),
    can_deref_(i.can_deref_),
    scratch_(col_count_) {
    if(lsm_sim) {  // XXX hack!
      struct timeval s;
      gettimeofday(&s, 0);
      random_state_ = s.tv_usec;
    }
    for(column_number_t c = 0; c < col_count_; c++) {
      scratch_.set(c,i.scratch_.get(c));
    }
  }

  Tuple<TYPE>& operator*() {
    if(can_deref_ == RAND_TUP_NO) { return scratch_; }
#ifdef RAND_TUP_CHECK
    assert(can_deref_ == RAND_TUP_YES);
#endif
    can_deref_ = RAND_TUP_NO;
    for(column_number_t i = 0; i < col_count_; i++) {
      unsigned int bump =
          rand_r(&random_state_) < bump_thresh_;
      TYPE val = bump+*scratch_.get(i);
      scratch_.set(i,&val);
    }
    return scratch_;
  }
  inline bool operator==(const randomIterator &a) const {
    return(off_ == a.off_);
  }
  inline bool operator!=(const randomIterator &a) const {
    return(off_ != a.off_);
  }
  inline void operator++() {
    off_++;
    if(can_deref_ == RAND_TUP_NO) {
      can_deref_ = RAND_TUP_YES;
    } else if(can_deref_ == RAND_TUP_NEVER) {
      can_deref_ = RAND_TUP_NO;
    } else {
      assert(can_deref_ == RAND_TUP_BROKE);
    }
  }
  inline void operator+=(int i) {
    can_deref_ = RAND_TUP_BROKE;
    off_+=i;
  }
  inline void operator--() {
    off_--;
#ifdef RAND_TUP_CHECK
    assert(can_deref_ != RAND_TUP_NEVER);
#endif
    if(can_deref_ == RAND_TUP_YES) {
      can_deref_ = RAND_TUP_NO;
    } else if(can_deref_ == RAND_TUP_NO) {
      can_deref_ = RAND_TUP_NEVER;
    } else {
      assert(can_deref_ == RAND_TUP_BROKE);
    }
  }
  inline int  operator-(randomIterator&i) {
    return off_ - i.off_;
  }
  inline void operator=(randomIterator const &i) {
    off_ = i.off_;
    count_ = i.count_;
    col_count_ = i.col_count_;
    bump_thresh_ = i.bump_thresh_;
    random_state_=i.random_state_;
    for(column_number_t c = 0; c < col_count_; c++) {
      scratch_.set(c,i.scratch_.get(c));
    }
    can_deref_ = i.can_deref_;
  }
  inline void offset(unsigned int off) {
    off_ = off;
  }
 private:
  int ret_tuple_;
  unsigned int off_;
  unsigned int count_;
  column_number_t col_count_;
  long bump_thresh_;
  unsigned int random_state_;
  int can_deref_;
  Tuple<TYPE> scratch_;
  friend const byte* toByteArray(randomIterator<val_t> * const t);
};
#undef RAND_TUP_CHECK
/** Produce a byte array from the value stored at t's current
    position.  If ret_tuple_ is false, it converts the first value
    of the tuple into a byte array.  Otherwise, it converts the iterator's
    current tuple into a byte array.
*/
inline const byte * toByteArray(randomIterator<val_t> * const t) {
  if(t->ret_tuple_ == 0) {
    return (const byte*)(**t).get(0);
  } else {
    return (**t).toByteArray();
  }
}

template <class ITER, class ROW> class mergeIterator;

template <class ITER, class ROW>
inline const byte * toByteArray(mergeIterator<ITER,ROW> * const t);

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
#undef RAND_TUP_CHECK

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

/**
   Create pages that are managed by Pstar<COMPRESSOR, TYPE>, and
   use them to store a compressed representation of the data set.

   @param dataset A pointer to the data that should be compressed.
   @param inserts The number of elements in dataset.

   @return the number of pages that were needed to store the
   compressed data.
*/
template <class PAGELAYOUT, class COMPRESSOR, class TYPE, class ITER, class ROW>
pageid_t compressData(ITER * const begin, ITER * const end,
		 int buildTree, recordid tree, pageid_t (*pageAlloc)(int,void*),
                      void *pageAllocState, uint64_t * inserted) {

  *inserted = 0;

  if(*begin == *end) {
    return 0;
  }

  pageid_t next_page = pageAlloc(-1,pageAllocState);
  Page *p = loadPage(-1, next_page);
  pageid_t pageCount = 0;


  if(*begin != *end && buildTree) {
    TlsmAppendPage(-1,tree,toByteArray(begin),p->id);
  }
  pageCount++;

  PAGELAYOUT * mc;

  initPage<COMPRESSOR,TYPE>(&mc, p, **begin);

  int lastEmpty = 0;

  for(ITER i(*begin); i != *end; ++i) {
    rose::slot_index_t ret = mc->append(-1, *i);

    (*inserted)++;

    if(ret == rose::NOSPACE) {
      p->dirty = 1;
      mc->pack();
      releasePage(p);

      --(*end);
      if(i != *end) {
        next_page = pageAlloc(-1,pageAllocState);
        p = loadPage(-1, next_page);

	mc = initPage<COMPRESSOR, TYPE>(&mc, p, *i);
	if(buildTree) {
	  TlsmAppendPage(-1,tree,toByteArray(&i),p->id);
	}
        pageCount++;
        lastEmpty = 0;
      } else {
        lastEmpty = 1;
      }
      ++(*end);
      --i;
    }
  }

  p->dirty = 1;
  mc->pack();
  releasePage(p);
  return pageCount;
}

template <class PAGELAYOUT>
inline const byte* toByteArray(treeIterator<int,PAGELAYOUT> *const t) {
  return (const byte*)&(**t);
}
template <class PAGELAYOUT,class TYPE>
inline const byte* toByteArray(treeIterator<Tuple<TYPE>,PAGELAYOUT> *const t) {
  return (**t).toByteArray();
}

/**
   Read compressed data from pages starting at FIRST_PAGE.

   @param num_pages the number of compressed pages to be read.
   @param dataset a pointer to the uncompresed representation of the data
          that was inserted.  This function uses this as a sanity check to
          make sure that the value read from the pages matches the original
          data.
   @return The nubmer of values read from the compressed pages.
*/
template <class PAGELAYOUT, class ITER, class ROW>
int readData(pageid_t firstPage, unsigned int num_pages,
               ITER &iter1, ROW* scratch) {

  // Copy the iterator (use the original to compute the number
  // of rows read below).
  ITER iter(iter1);

  for(unsigned int j = 0; j < num_pages; j++) {
    Page * p = loadPage(-1, firstPage + j);
    PAGELAYOUT * mc = (PAGELAYOUT*)(p->impl);

    int slot = 0;

    for(ROW* i = mc->recordRead(-1, slot, scratch);
        i; i = mc->recordRead(-1, slot, scratch)) {
#ifdef CHECK_OUTPUT
      assert(*i == *iter);
#endif
      ++(iter);
      slot++;
    }
    releasePage(p);
  }
  unsigned int count = iter-iter1;
  iter1 = iter;
  return count;
}
/**
   Like readData, but uses an lsm tree to locate the pages.
*/
template <class PAGELAYOUT,class ITER, class ROW>
int readDataFromTree(recordid tree, ITER &iter, ROW *scratch, int keylen) {

  unsigned int count = 0;
  lladdIterator_t * it = lsmTreeIterator_open(-1,tree);

  while(lsmTreeIterator_next(-1, it)) {
    byte * firstPage;
    int valsize = lsmTreeIterator_value(-1,it,&firstPage);
#ifdef CHECK_OUTPUT
    assert(valsize == sizeof(pageid_t));
    byte * scratchBuf;
    int keysize = lsmTreeIterator_key(-1,it,&scratchBuf);
    assert(keysize == keylen);

    const byte * iterBuf = toByteArray(&iter);
    assert(!memcmp(iterBuf,scratchBuf,keylen));
#else
    (void)valsize;
#endif
    count +=
      readData<PAGELAYOUT,ITER,ROW>(*(pageid_t*)firstPage,1,iter,scratch);
  }
  lsmTreeIterator_close(-1,it);
  return count;
}

double tv_to_double(struct timeval tv) {
  return static_cast<double>(tv.tv_sec) +
      (static_cast<double>(tv.tv_usec) / 1000000.0);
}

template<class PAGELAYOUT,class ENGINE,class ITER,class ROW,class TYPE>
struct insert_args {
  int comparator_idx;
  int rowsize;
  ITER *begin;
  ITER *end;
  pageid_t(*pageAlloc)(int,void*);
  void *pageAllocState;
  pthread_mutex_t * block_ready_mut;
  pthread_cond_t * block_needed_cond;
  pthread_cond_t * block_ready_cond;
  int max_waiters;
  int wait_count;
  recordid * wait_queue;
  ROW *scratchA;
  ROW *scratchB;
  pageid_t mergedPages;
};

template<class PAGELAYOUT,class ENGINE,class ITER,class ROW,class TYPE>
void* insertThread(void* arg) {
  insert_args<PAGELAYOUT,ENGINE,ITER,ROW,TYPE>* a =
    (insert_args<PAGELAYOUT,ENGINE,ITER,ROW,TYPE>*)arg;

  struct timeval start_tv, start_wait_tv, stop_tv;

  int insert_count = 0;

  pageid_t lastTreeBlocks = 0;
  uint64_t lastTreeInserts = 0;
  pageid_t desiredInserts = 0;

  // this is a hand-tuned value; it should be set dynamically, not staticly
  double K = 0.18;

  // loop around here to produce multiple batches for merge.
  while(1) {
    gettimeofday(&start_tv,0);
    ITER i(*(a->begin));
    ITER j(desiredInserts ? *(a->begin) : *(a->end));
    if(desiredInserts) {
      j += desiredInserts;
    }
    recordid tree = TlsmCreate(-1, a->comparator_idx,a->rowsize);
    lastTreeBlocks = compressData<PAGELAYOUT,ENGINE,TYPE,ITER,ROW>
          (&i, &j,1,tree,a->pageAlloc,a->pageAllocState, &lastTreeInserts);

    gettimeofday(&start_wait_tv,0);
    pthread_mutex_lock(a->block_ready_mut);
    while(a->wait_count >= a->max_waiters) {
      pthread_cond_wait(a->block_needed_cond,a->block_ready_mut);
    }

    memcpy(&a->wait_queue[a->wait_count],&tree,sizeof(recordid));
    a->wait_count++;

    pthread_cond_signal(a->block_ready_cond);
    gettimeofday(&stop_tv,0);
    double work_elapsed = tv_to_double(start_wait_tv) - tv_to_double(start_tv);
    double wait_elapsed = tv_to_double(stop_tv) - tv_to_double(start_wait_tv);
    double elapsed = tv_to_double(stop_tv) - tv_to_double(start_tv);
    printf("insert# %-6d                         waited %6.1f sec   "
           "worked %6.1f sec inserts %-12ld (%9.3f mb/s)\n",
           ++insert_count,
           wait_elapsed,
           work_elapsed,
           (long int)lastTreeInserts,
           (lastTreeInserts*(uint64_t)a->rowsize / (1024.0*1024.0)) / elapsed);

    if(a->mergedPages != -1) {
      desiredInserts = (pageid_t)(((double)a->mergedPages / K)
                                  * ((double)lastTreeInserts
                                     / (double)lastTreeBlocks));
    }
    pthread_mutex_unlock(a->block_ready_mut);

  }
  return 0;
}

template<class PAGELAYOUT,class ENGINE,class ITER,class ROW,class TYPE>
void* mergeThread(void* arg) {
  insert_args<PAGELAYOUT,ENGINE,ITER,ROW,TYPE>* a =
    (insert_args<PAGELAYOUT,ENGINE,ITER,ROW,TYPE>*)arg;

  struct timeval start_tv, wait_tv, stop_tv;

  int merge_count = 0;
  // loop around here to produce multiple batches for merge.
  while(1) {

    gettimeofday(&start_tv,0);

    pthread_mutex_lock(a->block_ready_mut);
    while(a->wait_count <2) {
      pthread_cond_wait(a->block_ready_cond,a->block_ready_mut);
    }

    gettimeofday(&wait_tv,0);

    recordid * oldTreeA = &a->wait_queue[0];
    recordid * oldTreeB = &a->wait_queue[1];

    pthread_mutex_unlock(a->block_ready_mut);

    recordid tree = TlsmCreate(-1, a->comparator_idx,a->rowsize);

    treeIterator<ROW,PAGELAYOUT> taBegin(*oldTreeA,*(a->scratchA),a->rowsize);
    treeIterator<ROW,PAGELAYOUT> tbBegin(*oldTreeB,*(a->scratchB),a->rowsize);

    treeIterator<ROW,PAGELAYOUT> *taEnd = taBegin.end();
    treeIterator<ROW,PAGELAYOUT> *tbEnd = tbBegin.end();

    mergeIterator<treeIterator<ROW,PAGELAYOUT>,ROW>
        mBegin(taBegin, tbBegin, *taEnd, *tbEnd);

    mergeIterator<treeIterator<ROW,PAGELAYOUT>,ROW>
        mEnd(taBegin, tbBegin, *taEnd, *tbEnd);


    mEnd.seekEnd();
    uint64_t insertedTuples;
    pageid_t mergedPages = compressData<PAGELAYOUT,ENGINE,TYPE,
        mergeIterator<treeIterator<ROW,PAGELAYOUT>,ROW>,ROW>
          (&mBegin, &mEnd,1,tree,a->pageAlloc,a->pageAllocState,&insertedTuples);

    delete taEnd;
    delete tbEnd;

    gettimeofday(&stop_tv,0);

    pthread_mutex_lock(a->block_ready_mut);

    a->mergedPages = mergedPages;

    // TlsmFree(wait_queue[0])  /// XXX Need to implement (de)allocation!
    // TlsmFree(wait_queue[1])

    memcpy(&a->wait_queue[0],&tree,sizeof(tree));
    for(int i = 1; i + 1 < a->wait_count; i++) {
      memcpy(&a->wait_queue[i],&a->wait_queue[i+1],sizeof(tree));
    }
    a->wait_count--;
    pthread_mutex_unlock(a->block_ready_mut);

    merge_count++;

    double wait_elapsed  = tv_to_double(wait_tv) - tv_to_double(start_tv);
    double work_elapsed  = tv_to_double(stop_tv) - tv_to_double(wait_tv);
    double total_elapsed = wait_elapsed + work_elapsed;
    double ratio = ((double)(insertedTuples * (uint64_t)a->rowsize))
                      / (double)(PAGE_SIZE * mergedPages);
    double throughput = ((double)(insertedTuples * (uint64_t)a->rowsize))
                      / (1024.0 * 1024.0 * total_elapsed);

    printf("merge # %-6d: comp ratio: %-9.3f  waited %6.1f sec   "
           "worked %6.1f sec inserts %-12ld (%9.3f mb/s)\n", merge_count, ratio,
           wait_elapsed, work_elapsed, (unsigned long)insertedTuples, throughput);

    pthread_cond_signal(a->block_needed_cond);
  }
  return 0;
}



/**
   Test driver for lsm tree.  This function dispatches to the correct
   invocations of compressData() and readDataFromTree().
*/
template<class PAGELAYOUT,class ENGINE,class ITER,class ROW,class TYPE>
void run_test(unsigned int inserts, column_number_t column_count,
              int buildTree, ITER& begin, ITER& end, int comparator_idx,
              int rowsize, ROW &scratch) {

  // Init storage --------------------------------------------

  struct timeval start_tv, stop_tv;
  double elapsed, start, stop, decompressed_size;

  unlink("storefile.txt");
  unlink("logfile.txt");

  // sync to minimize the measured performance impact of the file
  // deletions.
  sync();

  stasis_page_impl_register(Pstar<For<val_t>, val_t>::impl());
  stasis_page_impl_register(Pstar<Rle<val_t>, val_t>::impl());
  stasis_page_impl_register(Multicolumn<Tuple<val_t> >::impl());

  bufferManagerNonBlockingSlowHandleType = IO_HANDLE_PFILE;

  lsmTreeRegisterComparator(INT_CMP, intCmp<val_t>);
  lsmTreeRegisterComparator(TUP_CMP, tupCmp<val_t>);

  int num_pages = 0;

  TlsmSetPageAllocator(roseFastAlloc, &num_pages);

  Tinit();

  recordid tree = NULLRID;

  pthread_mutex_t block_ready_mut = PTHREAD_MUTEX_INITIALIZER;
  pthread_cond_t  block_needed_cond = PTHREAD_COND_INITIALIZER;
  pthread_cond_t  block_ready_cond = PTHREAD_COND_INITIALIZER;
  int max_waiters = 3; // merged, old, new.
  if(lsm_sim) {
    struct insert_args<PAGELAYOUT,ENGINE,ITER,ROW,TYPE> args = {
      comparator_idx,
      rowsize,
      &begin,
      &end,
      roseFastAlloc,
      &num_pages,
      &block_ready_mut,
      &block_needed_cond,
      &block_ready_cond,
      max_waiters, // max waiters
      0, // wait count
      new recordid[max_waiters], // wait queue
      new ROW(scratch),
      new ROW(scratch),
      -1  // merged pages
    };

    pthread_t inserter;
    pthread_create(&inserter, 0,
                   (void*(*)(void*))insertThread<PAGELAYOUT,ENGINE,ITER,ROW,TYPE>,
                   &args);
    pthread_t merger;
    pthread_create(&merger, 0,
                   (void*(*)(void*))mergeThread<PAGELAYOUT,ENGINE,ITER,ROW,TYPE>,
                   &args);

    pthread_join(inserter,0);
    pthread_join(merger,0);
    return;
  }

  // Compress data -------------------------------------------

  gettimeofday(&start_tv, 0);

  if(buildTree) {
    tree = TlsmCreate(-1, comparator_idx,rowsize);
  }

  uint64_t insertedByCompress;
  compressData<PAGELAYOUT,ENGINE,TYPE,ITER,ROW>
    (&begin, &end,buildTree,tree,roseFastAlloc,(void*)&num_pages,
     &insertedByCompress);

  gettimeofday(&stop_tv, 0);

  start = tv_to_double(start_tv);
  stop  = tv_to_double(stop_tv);
  elapsed = stop - start;
  decompressed_size =
      static_cast<double>(inserts * sizeof(val_t) * column_count);

  printf("%8d %9.2fx", num_pages,
         decompressed_size/static_cast<double>(num_pages*PAGE_SIZE));
  printf(" %10.3f", decompressed_size / (GB*elapsed));
  fflush(stdout);

  // the two commented out bodies of this if test merge iterators
  // and tree iterators

  //  if(buildTree) {
  //    gettimeofday(&start_tv, 0);

    //    int oldNumPages = num_pages;

    //    ROW scratch_row(column_count); // XXX use scratch_row?

    /*
    treeIterator<ROW,PAGELAYOUT> treeBegin(tree,scratch,rowsize);
    treeIterator<ROW,PAGELAYOUT> * treeEnd = treeBegin.end();
    tree = TlsmCreate(-1, comparator_idx,rowsize);
    compressData<PAGELAYOUT,ENGINE,TYPE,treeIterator<ROW,PAGELAYOUT>,ROW>
        (&treeBegin, treeEnd,buildTree,tree,&num_pages);
    delete treeEnd;
    */


    /*    treeIterator<ROW,PAGELAYOUT> treeBegin(tree,scratch,rowsize);
    treeIterator<ROW,PAGELAYOUT> treeBegin2(tree,scratch,rowsize);
    treeIterator<ROW,PAGELAYOUT> * treeEnd = treeBegin.end();
    treeIterator<ROW,PAGELAYOUT> * treeEnd2 = treeBegin2.end();
    treeIterator<ROW,PAGELAYOUT> * treeEnd3 = treeBegin2.end();

    mergeIterator<treeIterator<ROW,PAGELAYOUT>,ROW> mbegin(treeBegin,*treeEnd3,*treeEnd,*treeEnd2);
        //mergeIterator<treeIterator<ROW,PAGELAYOUT>,ROW> mbegin(treeBegin,treeBegin,*treeEnd,*treeEnd2);
    mergeIterator<treeIterator<ROW,PAGELAYOUT>,ROW> mend(treeBegin,*treeEnd3,*treeEnd,*treeEnd2);
    mend.seekEnd();

    assert(inserts == 0 || mbegin != mend);

    tree = TlsmCreate(-1, comparator_idx,rowsize);


    compressData<
        PAGELAYOUT,
        ENGINE,
        TYPE,
        mergeIterator<treeIterator<ROW,PAGELAYOUT>, ROW>,
        ROW>
        (&mbegin,&mend,buildTree,tree,&num_pages);


    delete treeEnd;
    delete treeEnd2;
    delete treeEnd3;


    assert(num_pages - oldNumPages == oldNumPages);

    gettimeofday(&stop_tv, 0);

    start = tv_to_double(start_tv);
    stop  = tv_to_double(stop_tv);
    elapsed = stop - start;
    decompressed_size =
        static_cast<double>(inserts * sizeof(val_t) * column_count);


    //    printf("%8d %9.2fx", num_pages - oldNumPages,
    //           decompressed_size/static_cast<double>((num_pages-oldNumPages)*PAGE_SIZE));
    printf(" %10.3f", decompressed_size / (GB*elapsed));
    fflush(stdout);
    */
  //  }
  // Read data -------------------------------------------

  gettimeofday(&start_tv, 0);

  unsigned int count;

  if(!buildTree) {
    ITER i(begin);
    count = readData<PAGELAYOUT,ITER,ROW>
	(FIRST_PAGE, num_pages, i, &scratch);
  } else {
    ITER i(begin);
    count = readDataFromTree<PAGELAYOUT,ITER,ROW>
      (tree, i, &scratch, rowsize);
  }

  gettimeofday(&stop_tv, 0);

  assert(count == inserts);

  start = tv_to_double(start_tv);
  stop  = tv_to_double(stop_tv);
  elapsed = stop - start;
  decompressed_size =
      static_cast<double>(inserts * sizeof(val_t) * column_count);
  printf(" %11.3f", decompressed_size / (GB*elapsed));
  fflush(stdout);
  Tdeinit();

}

/**
   An extra dispatch function.  This function and run_test perform
   nested template instantiations.  Breaking it into two functions
   keeps the code size from exploding.
*/
template <class ITER>
void run_test2(int engine, int multicolumn, unsigned int inserts,
	       ITER &begin, ITER &end,
	       column_number_t column_count, int buildTree) {

  if(multicolumn) {
    int rowsize = Tuple<val_t>::sizeofBytes(column_count);
    Tuple<val_t> scratch(column_count);

    switch(engine) {
    case Rle<val_t>::PLUGIN_ID: {
      run_test<Multicolumn<Tuple<val_t> >,Rle<val_t>,ITER,
	  Tuple<val_t>,val_t>
	(inserts, column_count, buildTree,begin,end,TUP_CMP,rowsize,scratch);
    } break;
    case For<val_t>::PLUGIN_ID: {
      run_test<Multicolumn<Tuple<val_t> >,For<val_t>,ITER,
	  Tuple<val_t>,val_t>
	(inserts, column_count, buildTree,begin,end,TUP_CMP,rowsize,scratch);
    } break;
    default: abort();
    }
  } else {
    int rowsize = sizeof(val_t);
    val_t scratch;
    column_count = 1;
    switch(engine) {
    case Rle<val_t>::PLUGIN_ID: {
      run_test<Pstar<Rle<val_t>,val_t>,Rle<val_t>,typeof(begin),
	  val_t,val_t>
	(inserts, column_count,buildTree,begin,end,INT_CMP,rowsize,scratch);
    } break;
    case For<val_t>::PLUGIN_ID: {
      run_test<Pstar<For<val_t>,val_t>,For<val_t>,typeof(begin),
	  val_t,val_t>
	(inserts, column_count,buildTree,begin,end,INT_CMP,rowsize,scratch);
    } break;
    default: abort();
    }
  }
}

const char * usage =
"Usage:\n"
"Mode 1: Generate synthetic data\n"
"\n\t%s [-e engine] [-i insert_count] [-s random_seed] [-p p(bump)]\n"
"\t\t[-n col_count] [-m] [-t] [-l]\n\n"
"Mode 2: Read data from csv file\n"
"\n\t%s -e engine -f filename -c col_num1 -c col_num2 ... [-m] [-t] [-l]\n\n"
"Mode 3: Simulate replicatio- by running a continuous insert / merge job\n"
"\n\t%s -r [synthetic data options]\n\n"
"- engine is 1 for run length encoding, 2 for frame of reference.\n"
"  If engine is not specified runs both engines with and without multicolumn\n"
"  support.\n"
"- column_number starts at zero\n"
"- p(bump) is the probability of incrementing each generated value.\n"
"- -f reads from a CSV file, repeated -c arguments pick columns\n"
"- -n col_count provides a data set with the given number of columns\n"
"- -m enables multicolumn page format\n"
"- -t builds an lsmTree from the compressed pages\n"
"- -l pipelines the data instead of buffering the whole dataset in RAM\n"
"\n";
int main(int argc, char **argv) {
  // Parse arguments -----------------------------------------

  unsigned int inserts = 1000000;
  double bump_prob = 0.001;

  unsigned int seed = 0;
  int engine = -1;

  int file_mode = 0;
  int column_count = 0;
  int requested_column_count = 0;
  int * column = new int[column_count+1];
  int buildTree = 0;
  int pipelined = 0;
  char * file = 0;

  int multicolumn = 0;

  for (int i = 1; i < argc; i++) {
    if (!strcmp(argv[i], "-e")) {
      i++;
      assert(i < argc);
      engine = atoi(argv[i])-1;
    } else if (!strcmp(argv[i], "-i")) {
      i++;
      assert(i < argc);
      inserts = atoi(argv[i]);
    } else if (!strcmp(argv[i], "-s")) {
      i++;
      assert(i < argc);
      seed = atoi(argv[i]);
    } else if (!strcmp(argv[i], "-p")) {
      i++;
      assert(i < argc);
      bump_prob = atof(argv[i]);
    } else if (!strcmp(argv[i], "-f")) {
      i++;
      assert(i < argc);
      file_mode = 1;
      file = argv[i];
    } else if (!strcmp(argv[i], "-n")) {
      i++;
      assert(i < argc);
      requested_column_count = atoi(argv[i]);
      assert(requested_column_count ==
             (column_number_t)requested_column_count);
    } else if (!strcmp(argv[i], "-c")) {
      i++;
      assert(i < argc);
      column[column_count] = atoi(argv[i]);
      column_count++;
      column = reinterpret_cast<int*>(realloc(column,
                                              (column_count+1) * sizeof(int)));
    } else if (!strcmp(argv[i], "-m")) {
      multicolumn = 1;
    } else if (!strcmp(argv[i], "-t")) {
      buildTree = 1;
    } else if (!strcmp(argv[i], "-l")) {
      pipelined = 1;
    } else if (!strcmp(argv[i], "-r")) {
      lsm_sim = 1;
      pipelined = 1;  // XXX otherwise, we'd core dump later...
    } else {
      printf("Unrecognized argument: %s\n", argv[i]);
      printf(usage, argv[0], argv[0], argv[0]);
      return 1;
    }
  }

  if(lsm_sim && file_mode) {
    printf("Sorry, lsm simulation doesn't work with file input.\n");
    printf(usage, argv[0], argv[0], argv[0]);
    return 1;
  }

  char * engine_name;

  switch (engine) {
    case Rle<val_t>::PLUGIN_ID: {
      engine_name = "RLE";
    } break;
    case For<val_t>::PLUGIN_ID: {
      engine_name = "PFOR";
    } break;
    case -1: {
      engine_name = "Time trial (multiple engines)";
    } break;
    default: {
      printf("Specify a valid compression scheme\n");
      printf(usage, argv[0], argv[0], argv[0]);
      return 1;
    }
  }
  printf("Compression scheme: %s\n", engine_name);
  printf("Page size:          %d\n", PAGE_SIZE);

  srandom(seed);

  // These are used throughout the rest of this file.

  struct timeval start_tv, stop_tv;
  double elapsed, start, stop, decompressed_size;

  // Generate data -------------------------------------------

  val_t current = 0;

  // dataset is managed by malloc so that it can be realloc()'ed
  val_t **dataset;

  if(requested_column_count && file_mode) {
      printf("-n and -f are incompatible\n");
      printf(usage,argv[0],argv[0],argv[0]);
      return 1;
  }
  if(!file_mode) {
    if(!requested_column_count) {
      requested_column_count = 1;
    }
    column_count = requested_column_count;
  }

  printf("P(bump):            %f\n", bump_prob);
  printf("Random seed:        %d\n", seed);
  printf("Column count:       %d\n", column_count);


  gettimeofday(&start_tv, 0);

  if ((!file_mode)) {
    if(!pipelined) {

      dataset = new val_t*[column_count];

      for(int col = 0; col < column_count; col++) {
        current = 0;
        dataset[col]
            = reinterpret_cast<val_t*>(malloc(sizeof(val_t) * inserts));
	for (unsigned int i = 0; i < inserts; i++) {
	  if (bump_prob == 1) {
	    current++;
	  } else {
	    while (static_cast<double>(random())
		   / static_cast<double>(RAND_MAX) < bump_prob) {
	      current++;
	    }
	  }
	  dataset[col][i] = current;
	}
      }
    } else {
      dataset = 0;
    }
  } else {

    dataset = new val_t*[column_count];
    int max_col_number = 0;
    for(int col = 0; col < column_count; col++) {
      max_col_number = max_col_number < column[col]
	? column[col] : max_col_number;

      dataset[col] = reinterpret_cast<val_t*>(malloc(sizeof(val_t)));
    }
    max_col_number++;
    char **toks = reinterpret_cast<char**>
      (malloc(sizeof(char *) * max_col_number));

    printf("Reading from file %s ", file);

    inserts = 0;

    size_t line_len = 100;
    // getline wants malloced memory (it probably calls realloc...)
    char * line = reinterpret_cast<char*>(malloc(sizeof(char) * line_len));

    FILE * input = fopen(file, "r");
    if (!input) {
      perror("Couldn't open input");
      return 1;
    }
    ssize_t read_len;

    while (-1 != (read_len = getline(&line, &line_len, input))) {
      int line_tok_count;
      {
        char *saveptr;
        int i;
        toks[0] = strtok_r(line, ",\n", &saveptr);
        for (i = 1; i < max_col_number; i++) {
          toks[i] = strtok_r(0, ",\n", &saveptr);
          if (!toks[i]) {
            break;
          }
          //          printf("found token: %s\n",toks[i]);
        }
        line_tok_count = i;
      }
      if (line_tok_count < max_col_number) {
        if (-1 == getline(&line, &line_len, input)) {
          // done parsing file
        } else {
          printf("Not enough tokens on line %d (found: %d expected: %d)\n",
                 inserts+1, line_tok_count, max_col_number);
          return 1;
        }
      } else {
        inserts++;
        for(int col = 0; col < column_count; col++) {
          dataset[col] = reinterpret_cast<val_t*>(
              realloc(dataset[col], sizeof(val_t) * (inserts + 1)));
          errno = 0;
          char * endptr;
          dataset[col][inserts]
              = (val_t)strtoll(toks[column[col]], &endptr, 0);
          if (strlen(toks[column[col]]) -
	      (size_t)(endptr-toks[column[col]]) > 1) {
            printf("Couldn't parse token #%d: %s\n", col, toks[column[col]]);
            return 1;
          }
          if (errno) {
            printf("Couldn't parse token #%d: %s", col,toks[column[col]]);
            perror("strtoll error is");
            return 1;
          }
          // printf("token: %d\n", dataset[inserts]);
        }
      }
    }
    fclose(input);

    gettimeofday(&stop_tv, 0);
    printf("%10d tuples ", inserts);

    start = tv_to_double(start_tv);
    stop  = tv_to_double(stop_tv);

    elapsed = stop - start;
    decompressed_size = static_cast<double>(inserts * sizeof(val_t));

    printf ("at %6.3f gb/s\n",
            column_count*decompressed_size / (GB * elapsed));
  }

  if(column_count > 1 && (!multicolumn || engine == -1)) {
    printf("\nWARNING: Pstar will only use the first column.\n");
  }

  if(!pipelined) {

    Tuple<val_t>::iterator begin(column_count, dataset,0);
    Tuple<val_t>::iterator end(column_count, dataset,inserts);
    val_t * ptr_begin = dataset[0];
    val_t * ptr_end   = dataset[0] + inserts;

    if(engine != -1) {
      printf("\n  #pages      ratio  comp gb/s  decom gb/s\n");
      if(multicolumn) {
	run_test2<Tuple<val_t>::iterator>
	  (engine, multicolumn, inserts, begin, end,
	   column_count, buildTree);
      } else {
	run_test2<val_t*>(engine,multicolumn,inserts,ptr_begin,ptr_end,
			  column_count, buildTree);
      }
    } else {
      //  if(buildTree) {
      //    printf("\nCompression scheme   #pages      ratio  comp gb/s "
      //    "recom gb/s  decom gb/s");
      //  } else {
      printf("\nCompression scheme   #pages      ratio  comp gb/s  decom gb/s");
      // }
      printf("\nPstar        (For) ");
      run_test2<val_t*>
	(For<val_t>::PLUGIN_ID,0,inserts,ptr_begin,ptr_end,
	 column_count,buildTree);
      printf("\nMulticolumn  (For) ");
      run_test2<Tuple<val_t>::iterator>
	(For<val_t>::PLUGIN_ID,1,inserts,begin,end,
	 column_count,buildTree);
      printf("\nPstar        (Rle) ");
      run_test2<val_t*>
	(Rle<val_t>::PLUGIN_ID,0,inserts,ptr_begin,ptr_end,
	 column_count,buildTree);
      printf("\nMulticolumn  (Rle) ");
      run_test2<Tuple<val_t>::iterator>
	(Rle<val_t>::PLUGIN_ID,1,inserts,begin,end,
	 column_count,buildTree);
    }
    printf("\n");

    for(int col = 0; col < column_count; col++) {
      free(dataset[col]);
    }
    delete [] dataset;

  } else {

    assert(!file_mode);

    randomIterator<val_t> begin(seed, inserts, column_count, bump_prob, 1);
    randomIterator<val_t> end(seed,inserts,column_count,bump_prob, 1);
    end.offset(inserts);
    // These three iterators are for pstar.  They hide the fact that they're
    // backed by tuples.
    randomIterator<val_t> pstrbegin(seed, inserts, column_count, bump_prob, 0);
    randomIterator<val_t> pstrend(seed,inserts, column_count, bump_prob, 0);
    pstrend.offset(inserts);

    if(engine != -1) {
      printf("\n  #pages      ratio  comp gb/s  decom gb/s\n");
      if(multicolumn) {
	run_test2<randomIterator<val_t> >
	  (engine, multicolumn, inserts, begin, end,
	   column_count, buildTree);
      } else {
	run_test2<randomIterator<val_t> >(engine,multicolumn,inserts,pstrbegin,pstrend,
			  column_count, buildTree);
      }
    } else {
      //      if(buildTree) {
      //        printf("\nCompression scheme   #pages      ratio  comp gb/s "
      //               "recom gb/s  decom gb/s");
      //      } else {
      printf("\nCompression scheme   #pages      ratio  comp gb/s  decom gb/s");
      //      }
      printf("\nPstar        (For) ");
      run_test2<randomIterator<val_t> >
	(For<val_t>::PLUGIN_ID,0,inserts,pstrbegin,pstrend,
	 column_count,buildTree);
      printf("\nMulticolumn  (For) ");
      run_test2<randomIterator<val_t> >
	(For<val_t>::PLUGIN_ID,1,inserts,begin,end,
	 column_count,buildTree);
      printf("\nPstar        (Rle) ");
      run_test2<randomIterator<val_t> >
	(Rle<val_t>::PLUGIN_ID,0,inserts,pstrbegin,pstrend,
	 column_count,buildTree);
      printf("\nMulticolumn  (Rle) ");
      run_test2<randomIterator<val_t> >
	(Rle<val_t>::PLUGIN_ID,1,inserts,begin,end,
	 column_count,buildTree);
    }
    printf("\n");
  }

  delete [] column;

  return 0;
}
