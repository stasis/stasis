#ifndef _ROSE_COMPRESSION_LSMTABLE_H__
#define _ROSE_COMPRESSION_LSMTABLE_H__

#undef end
#undef begin

#include <set>
#include "lsmIterators.h"

namespace rose {
  /**
     @file

     This file contains worker threads and the end user interface for Rose's
     LSM-tree based table implementation.  The page format is set at compile
     time with a template instantiation.

     @see lsmWorkers.h provides a more general (and dynamically
     dispatched), interface to the underlying primititves
  */


  template<class PAGELAYOUT>
    struct new_insert_args {
      int comparator_idx;
      int rowsize; //typedef int32_t val_t;
      //  ITER *begin;
      //  ITER *end;
      pageid_t(*pageAlloc)(int,void*);
      void *pageAllocState;
      pthread_mutex_t * block_ready_mut;
      pthread_cond_t * block_needed_cond;
      pthread_cond_t * block_ready_cond;
      int max_waiters;
      int wait_count;
      recordid * wait_queue;
      typename PAGELAYOUT::FMT::TUP *scratchA;
      typename PAGELAYOUT::FMT::TUP *scratchB;
      pageid_t mergedPages;
    };

  template <class PAGELAYOUT, class ITER>
    pageid_t compressData(ITER * begin, ITER * end, recordid tree,
			  pageid_t (*pageAlloc)(int,void*),
			  void *pageAllocState, uint64_t *inserted) {
    *inserted = 0;

    if(*begin == *end) {
      return 0;
    }
    pageid_t next_page = pageAlloc(-1,pageAllocState);
    Page *p = loadPage(-1, next_page);
    pageid_t pageCount = 0;

    if(*begin != *end) {
      TlsmAppendPage(-1,tree,toByteArray(begin),pageAlloc,pageAllocState,p->id);
    }
    pageCount++;

    typename PAGELAYOUT::FMT * mc = PAGELAYOUT::initPage(p, &**begin);

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

	  mc = PAGELAYOUT::initPage(p, &*i);

	  TlsmAppendPage(-1,tree,toByteArray(&i),pageAlloc,pageAllocState,p->id);

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


  /**
     ITERA is an iterator over the data structure that mergeThread creates (a lsm tree iterator).
     ITERB is an iterator over the data structures that mergeThread takes as input (lsm tree, or rb tree..)
  */
  template<class PAGELAYOUT, class ITERA, class ITERB> //class PAGELAYOUTX, class ENGINE, class ITERA, class ITERB,
    //  class ROW, class TYPE>
    void* mergeThread(void* arg) {
    // The ITER argument of a is unused (we don't look at it's begin or end fields...)
    //insert_args<PAGELAYOUT,ENGINE,ITERA,ROW>* a =
    //    (insert_args<PAGELAYOUT,ENGINE,ITERA,ROW>*)arg;
    new_insert_args<PAGELAYOUT> * a = (new_insert_args<PAGELAYOUT>*)arg;

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

      recordid tree = TlsmCreate(-1, a->comparator_idx,a->pageAlloc,a->pageAllocState,a->rowsize);

      ITERA taBegin(*oldTreeA,*(a->scratchA),a->rowsize);
      ITERB tbBegin(*oldTreeB,*(a->scratchB),a->rowsize);

      ITERA *taEnd = taBegin.end();
      ITERB *tbEnd = tbBegin.end();

      mergeIterator<ITERA, ITERB, typename PAGELAYOUT::FMT::TUP>
	mBegin(taBegin, tbBegin, *taEnd, *tbEnd);

      mergeIterator<ITERA, ITERB, typename PAGELAYOUT::FMT::TUP>
	mEnd(taBegin, tbBegin, *taEnd, *tbEnd);


      mEnd.seekEnd();
      uint64_t insertedTuples;
      pageid_t mergedPages = compressData<PAGELAYOUT,mergeIterator<ITERA,ITERB,typename PAGELAYOUT::FMT::TUP> >
	(&mBegin, &mEnd,tree,a->pageAlloc,a->pageAllocState,&insertedTuples);
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
  /*

    template<class PAGELAYOUT, class ITER>
    void* insertThread(void* arg) {

    new_insert_args<PAGELAYOUT> * a = (new_insert_args<PAGELAYOUT>*)arg;
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
    // XXX this needs to be an iterator over an in-memory tree.
    ITER i(*(a->begin));
    ITER j(desiredInserts ? *(a->begin) : *(a->end));
    if(desiredInserts) {
    j += desiredInserts;
    }
    recordid tree = TlsmCreate(-1, a->comparator_idx,a->rowsize);
    lastTreeBlocks =
    compressData<PAGELAYOUT,PAGELAYOUT::init_page,ITER>
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
  */
  typedef struct {
    recordid bigTree;
    recordid bigTreeAllocState; // this is probably the head of an arraylist of regions used by the tree...
    recordid mediumTree;
    recordid mediumTreeAllocState;
    epoch_t beginning;
    epoch_t end;
  } lsmTableHeader_t;

  template<class PAGELAYOUT>
    inline recordid TlsmTableAlloc(int xid) {

    // XXX use a (slow) page allocator in alloc, then create a new
    // (fast) region allocator in start.

    recordid ret = Talloc(xid, sizeof(lsmTableHeader_t));
    lsmTableHeader_t h;
    h.bigTreeAllocState = Talloc(xid,sizeof(TlsmRegionAllocConf_t));
    Tset(xid,h.bigTreeAllocState,&LSM_REGION_ALLOC_STATIC_INITIALIZER);
    h.bigTree = TlsmCreate(xid, PAGELAYOUT::cmp_id(),
			   TlsmRegionAllocRid,&h.bigTreeAllocState,
			   PAGELAYOUT::FMT::TUP::sizeofBytes());
    h.mediumTreeAllocState = Talloc(xid,sizeof(TlsmRegionAllocConf_t));
    Tset(xid,h.mediumTreeAllocState,&LSM_REGION_ALLOC_STATIC_INITIALIZER);
    h.mediumTree = TlsmCreate(xid, PAGELAYOUT::cmp_id(),
			      TlsmRegionAllocRid,&h.mediumTreeAllocState,
			      PAGELAYOUT::FMT::TUP::sizeofBytes());
    epoch_t beginning = 0;
    epoch_t end = 0;
    Tset(xid, ret, &h);
    return ret;
  }
  template<class PAGELAYOUT>
    void TlsmTableStart(recordid tree) {
    /// XXX xid for daemon processes?

    void * (*merger)(void*) = mergeThread
      <PAGELAYOUT,
      treeIterator<typename PAGELAYOUT::FMT::TUP, typename PAGELAYOUT::FMT>,
      treeIterator<typename PAGELAYOUT::FMT::TUP, typename PAGELAYOUT::FMT> >;

    /*mergeThread
      <PAGELAYOUT,
      treeIterator<typename PAGELAYOUT::FMT::TUP, typename PAGELAYOUT::FMT>,
      stlSetIterator<typename std::set<typename PAGELAYOUT::FMT::TUP,
                     typename PAGELAYOUT::FMT::TUP::stl_cmp>::iterator,
                     typename PAGELAYOUT::FMT::TUP> > 
		     (0); */

    lsmTableHeader_t h;
    Tread(-1, tree, &h);

  }
  template<class PAGELAYOUT>
    void TlsmTableStop(recordid tree) {

  }
}

#endif  // _ROSE_COMPRESSION_LSMTABLE_H__
