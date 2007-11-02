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


  template<class PAGELAYOUT, class ITERA, class ITERB>
    struct merge_args {
      pageid_t(*pageAlloc)(int,void*);
      void *pageAllocState;
      pthread_mutex_t * block_ready_mut;
      pthread_cond_t * in_block_needed_cond;
      pthread_cond_t * out_block_needed_cond;
      pthread_cond_t * in_block_ready_cond;
      pthread_cond_t * out_block_ready_cond;
      bool * still_open;
      typename ITERA::handle ** out_tree;
      typename ITERB::handle ** in_tree;
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
  template<class PAGELAYOUT, class ITERA, class ITERB>
    void* mergeThread(void* arg) {
    // The ITER argument of a is unused (we don't look at it's begin or end fields...)
    merge_args<PAGELAYOUT, ITERA, ITERB> * a = (merge_args<PAGELAYOUT, ITERA, ITERB>*)arg;

    struct timeval start_tv, wait_tv, stop_tv;

    int merge_count = 0;

    int xid = Tbegin();

    // Initialize tree with an empty tree.
    // XXX hardcodes ITERA's type:
    recordid oldtree = TlsmCreate(xid, PAGELAYOUT::cmp_id(),a->pageAlloc,
				  a->pageAllocState,PAGELAYOUT::FMT::TUP::sizeofBytes());

    Tcommit(xid);
    // loop around here to produce multiple batches for merge.
    while(1) {


      gettimeofday(&start_tv,0);

      pthread_mutex_lock(a->block_ready_mut);

      if(!*(a->still_open)) {
	pthread_mutex_unlock(a->block_ready_mut);
	break;
      }

      while(!*(a->in_tree)) {
	pthread_cond_signal(a->in_block_needed_cond);
	pthread_cond_wait(a->in_block_ready_cond,a->block_ready_mut);
      }

      gettimeofday(&wait_tv,0);

      xid = Tbegin();

      recordid tree = TlsmCreate(xid, PAGELAYOUT::cmp_id(),a->pageAlloc,
			a->pageAllocState,PAGELAYOUT::FMT::TUP::sizeofBytes());

      ITERA taBegin(oldtree);
      ITERB tbBegin(**a->in_tree);

      // XXX keep in_tree handle around so that it can be freed below.

      free(*a->in_tree); // free's copy of handle; not tree
      *a->in_tree = 0; // free slot for producer
      pthread_cond_signal(a->in_block_needed_cond);

      pthread_mutex_unlock(a->block_ready_mut);

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

      // TlsmFree(wait_queue[0])  /// XXX Need to implement (de)allocation!
      // TlsmFree(wait_queue[1])

      pthread_mutex_lock(a->block_ready_mut);

      static int threshold_calc = 1000; // XXX REALLY NEED TO FIX THIS!
      if(a->out_tree &&  // is there a upstream merger (note the lack of the * on a->out_tree)?
	 mergedPages > threshold_calc // do we have enough data to bother it?
	 ) {
	while(*a->out_tree) { // we probably don't need the "while..."
	  pthread_cond_wait(a->out_block_needed_cond, a->block_ready_mut);
	}

	// XXX C++?  Objects?  Constructors? Who needs them?
	*a->out_tree = (recordid*)malloc(sizeof(tree));
	**a->out_tree = tree;
	pthread_cond_signal(a->out_block_ready_cond);

	// This is a bit wasteful; allocate a new empty tree to merge against.
	// We don't want to ever look at the one we just handed upstream...
	// We could wait for an in tree to be ready, and then pass it directly
	// to compress data (to avoid all those merging comparisons...)
	tree = TlsmCreate(xid, PAGELAYOUT::cmp_id(),a->pageAlloc,
			a->pageAllocState,PAGELAYOUT::FMT::TUP::sizeofBytes());

      }
      pthread_mutex_unlock(a->block_ready_mut);

      merge_count++;

      double wait_elapsed  = tv_to_double(wait_tv) - tv_to_double(start_tv);
      double work_elapsed  = tv_to_double(stop_tv) - tv_to_double(wait_tv);
      double total_elapsed = wait_elapsed + work_elapsed;
      double ratio = ((double)(insertedTuples * (uint64_t)PAGELAYOUT::FMT::TUP::sizeofBytes()))
	/ (double)(PAGE_SIZE * mergedPages);
      double throughput = ((double)(insertedTuples * (uint64_t)PAGELAYOUT::FMT::TUP::sizeofBytes()))
	/ (1024.0 * 1024.0 * total_elapsed);

      printf("merge # %-6d: comp ratio: %-9.3f  waited %6.1f sec   "
	     "worked %6.1f sec inserts %-12ld (%9.3f mb/s)\n", merge_count, ratio,
	     wait_elapsed, work_elapsed, (unsigned long)insertedTuples, throughput);

      Tcommit(xid);

    }
    return 0;
  }
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

  /// XXX start should return a struct that contains these!
  pthread_t merge1_thread;
  pthread_t merge2_thread;
  bool * still_open;

  template<class PAGELAYOUT>
    void TlsmTableStart(recordid tree) {
    /// XXX xid for daemon processes?
    lsmTableHeader_t h;
    Tread(-1, tree, &h);

    typedef treeIterator<typename PAGELAYOUT::FMT::TUP,
      typename PAGELAYOUT::FMT> LSM_ITER;

    typedef stlSetIterator<typename std::set<typename PAGELAYOUT::FMT::TUP,
      typename PAGELAYOUT::FMT::TUP::stl_cmp>,
      typename PAGELAYOUT::FMT::TUP> RB_ITER;

    pthread_mutex_t * block_ready_mut =
      (pthread_mutex_t*)malloc(sizeof(pthread_mutex_t));
    pthread_cond_t  * block0_needed_cond =
      (pthread_cond_t*)malloc(sizeof(pthread_cond_t));
    pthread_cond_t  * block1_needed_cond =
      (pthread_cond_t*)malloc(sizeof(pthread_cond_t));
    pthread_cond_t  * block2_needed_cond =
      (pthread_cond_t*)malloc(sizeof(pthread_cond_t));
    pthread_cond_t  * block0_ready_cond =
      (pthread_cond_t*)malloc(sizeof(pthread_cond_t));
    pthread_cond_t  * block1_ready_cond =
      (pthread_cond_t*)malloc(sizeof(pthread_cond_t));
    pthread_cond_t  * block2_ready_cond =
      (pthread_cond_t*)malloc(sizeof(pthread_cond_t));

    pthread_mutex_init(block_ready_mut,0);
    pthread_cond_init(block0_needed_cond,0);
    pthread_cond_init(block1_needed_cond,0);
    pthread_cond_init(block2_needed_cond,0);
    pthread_cond_init(block0_ready_cond,0);
    pthread_cond_init(block1_ready_cond,0);
    pthread_cond_init(block2_ready_cond,0);

    typename LSM_ITER::handle * block1_scratch =
      (typename LSM_ITER::handle*) malloc(sizeof(typename LSM_ITER::handle));
    still_open = (bool*)malloc(sizeof(bool));
    *still_open = 1;
    recordid * ridp = (recordid*)malloc(sizeof(recordid));
    *ridp = h.bigTreeAllocState;

    recordid ** block1_scratch_p = (recordid**)malloc(sizeof(block1_scratch));
    *block1_scratch_p = block1_scratch;

    merge_args<PAGELAYOUT, LSM_ITER, LSM_ITER> * args1 = (merge_args<PAGELAYOUT,LSM_ITER,LSM_ITER>*)malloc(sizeof(merge_args<PAGELAYOUT,LSM_ITER,LSM_ITER>));
    merge_args<PAGELAYOUT, LSM_ITER, LSM_ITER> tmpargs1 =
      {
	TlsmRegionAllocRid,
	ridp,
	block_ready_mut,
	block1_needed_cond,
	block2_needed_cond,
	block1_ready_cond,
	block2_ready_cond,
	still_open,
	0,
	block1_scratch_p
      };
    *args1 = tmpargs1;
    void * (*merger1)(void*) = mergeThread
      <PAGELAYOUT, LSM_ITER, LSM_ITER>;

    ridp = (recordid*)malloc(sizeof(recordid));
    *ridp = h.mediumTreeAllocState;

    merge_args<PAGELAYOUT, LSM_ITER, RB_ITER> * args2 = (merge_args<PAGELAYOUT,LSM_ITER,RB_ITER>*)malloc(sizeof(merge_args<PAGELAYOUT,LSM_ITER,RB_ITER>));
    merge_args<PAGELAYOUT, LSM_ITER, RB_ITER> tmpargs2 =
      {
	TlsmRegionAllocRid,
	ridp,
	block_ready_mut,
	block0_needed_cond,
	block1_needed_cond,
	block0_ready_cond,
	block1_ready_cond,
	still_open,
	block1_scratch_p,
	0 // XXX how does this thing get fed new trees of tuples?
      };
    *args2 = tmpargs2;
    void * (*merger2)(void*) = mergeThread
      <PAGELAYOUT, LSM_ITER, RB_ITER>;


    pthread_create(&merge1_thread, 0, merger1, args1);
    pthread_create(&merge2_thread, 0, merger2, args2);

  }
  template<class PAGELAYOUT>
    void TlsmTableStop(recordid tree) {
    *still_open = 0;
    pthread_join(merge1_thread,0);
    pthread_join(merge2_thread,0);
  }
}

#endif  // _ROSE_COMPRESSION_LSMTABLE_H__
