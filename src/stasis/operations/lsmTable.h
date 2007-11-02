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
      typename ITERA::handle in_process_tree;
      typename ITERB::handle ** in_tree;
      typename ITERA::handle ** out_tree;
    };

  template <class PAGELAYOUT, class ITER>
    pageid_t compressData(int xid, ITER * begin, ITER * end, recordid tree,
			  pageid_t (*pageAlloc)(int,void*),
			  void *pageAllocState, uint64_t *inserted) {
    *inserted = 0;

    if(*begin == *end) {
      return 0;
    }
    pageid_t next_page = pageAlloc(xid,pageAllocState);
    Page *p = loadPage(xid, next_page);
    pageid_t pageCount = 0;

    if(*begin != *end) {
      TlsmAppendPage(xid,tree,toByteArray(begin),pageAlloc,pageAllocState,p->id);
    }
    pageCount++;

    typename PAGELAYOUT::FMT * mc = PAGELAYOUT::initPage(p, &**begin);

    int lastEmpty = 0;

    for(ITER i(*begin); i != *end; ++i) {
      rose::slot_index_t ret = mc->append(xid, *i);

      (*inserted)++;

      if(ret == rose::NOSPACE) {
	p->dirty = 1;
	mc->pack();
	releasePage(p);

	--(*end);
	if(i != *end) {
	  next_page = pageAlloc(xid,pageAllocState);
	  p = loadPage(xid, next_page);

	  mc = PAGELAYOUT::initPage(p, &*i);

	  TlsmAppendPage(xid,tree,toByteArray(&i),pageAlloc,pageAllocState,p->id);

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
    typename ITERA::handle oldtree = TlsmCreate(xid, PAGELAYOUT::cmp_id(),a->pageAlloc,
				  a->pageAllocState,PAGELAYOUT::FMT::TUP::sizeofBytes());

    Tcommit(xid);
    // loop around here to produce multiple batches for merge.

    gettimeofday(&start_tv,0);

    while(1) {


      pthread_mutex_lock(a->block_ready_mut);

      int done = 0;

      while(!*(a->in_tree)) {
	pthread_cond_signal(a->in_block_needed_cond);
	if(!*(a->still_open)) {
	  done = 1;
	  break;
	}
	pthread_cond_wait(a->in_block_ready_cond,a->block_ready_mut);
      }
      if(done) {
	a->in_process_tree = oldtree;
	pthread_cond_signal(a->out_block_ready_cond);
	pthread_mutex_unlock(a->block_ready_mut);
	break;
      }

      gettimeofday(&wait_tv,0);

      // XXX keep in_tree handle around so that it can be freed below.

      typename ITERB::handle old_in_tree = **a->in_tree;

      ITERA taBegin(oldtree);
      ITERB tbBegin(**a->in_tree);

      ITERA *taEnd = taBegin.end();
      ITERB *tbEnd = tbBegin.end();

      free(*a->in_tree); // free's copy of handle; not tree
      *a->in_tree = 0; // free slot for producer

      pthread_cond_signal(a->in_block_needed_cond);

      pthread_mutex_unlock(a->block_ready_mut);

      xid = Tbegin();

      recordid tree = TlsmCreate(xid, PAGELAYOUT::cmp_id(),a->pageAlloc,
			a->pageAllocState,PAGELAYOUT::FMT::TUP::sizeofBytes());

      mergeIterator<ITERA, ITERB, typename PAGELAYOUT::FMT::TUP>
	mBegin(taBegin, tbBegin, *taEnd, *tbEnd);

      mergeIterator<ITERA, ITERB, typename PAGELAYOUT::FMT::TUP>
	mEnd(taBegin, tbBegin, *taEnd, *tbEnd);

      mEnd.seekEnd();
      uint64_t insertedTuples;

      pageid_t mergedPages = compressData
	<PAGELAYOUT,mergeIterator<ITERA,ITERB,typename PAGELAYOUT::FMT::TUP> >
	(xid, &mBegin, &mEnd,tree,a->pageAlloc,a->pageAllocState,&insertedTuples);

      delete taEnd;
      delete tbEnd;

      gettimeofday(&stop_tv,0);

      // TlsmFree(wait_queue[0])  /// XXX Need to implement (de)allocation!
      // TlsmFree(wait_queue[1])

      merge_count++;

      double wait_elapsed  = tv_to_double(wait_tv) - tv_to_double(start_tv);
      double work_elapsed  = tv_to_double(stop_tv) - tv_to_double(wait_tv);
      double total_elapsed = wait_elapsed + work_elapsed;
      double ratio = ((double)(insertedTuples * (uint64_t)PAGELAYOUT::FMT::TUP::sizeofBytes()))
	/ (double)(PAGE_SIZE * mergedPages);
      double throughput = ((double)(insertedTuples * (uint64_t)PAGELAYOUT::FMT::TUP::sizeofBytes()))
	/ (1024.0 * 1024.0 * total_elapsed);

      printf("merge # %-6d: comp ratio: %-9.3f  waited %6.1f sec   "
	     "worked %6.1f sec inserts %-12ld (%9.3f mb/s) %6d pages\n", merge_count, ratio,
	     wait_elapsed, work_elapsed, (unsigned long)insertedTuples, throughput, mergedPages);


      gettimeofday(&start_tv,0);

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

      // XXX   TlsmFree(xid,oldtree);

      oldtree = tree;

      pthread_mutex_unlock(a->block_ready_mut);

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

  template <class PAGELAYOUT>
  struct lsmTableHandle {
    pthread_t merge1_thread;
    pthread_t merge2_thread;
    bool * still_open;
    typename stlSetIterator
      <typename std::set
        <typename PAGELAYOUT::FMT::TUP,
         typename PAGELAYOUT::FMT::TUP::stl_cmp>,
       typename PAGELAYOUT::FMT::TUP>::handle ** input_handle;
    typename std::set
      <typename PAGELAYOUT::FMT::TUP,
      typename PAGELAYOUT::FMT::TUP::stl_cmp> * scratch_handle;
    pthread_mutex_t * mut;
    pthread_cond_t * input_ready_cond;
    pthread_cond_t * input_needed_cond;
    merge_args<PAGELAYOUT, treeIterator<typename PAGELAYOUT::FMT::TUP,
      typename PAGELAYOUT::FMT>, treeIterator<typename PAGELAYOUT::FMT::TUP,
      typename PAGELAYOUT::FMT> > * args1;
    merge_args<PAGELAYOUT, treeIterator<typename PAGELAYOUT::FMT::TUP,
      typename PAGELAYOUT::FMT>, stlSetIterator<typename std::set<typename PAGELAYOUT::FMT::TUP,
      typename PAGELAYOUT::FMT::TUP::stl_cmp>,
      typename PAGELAYOUT::FMT::TUP> > * args2;
  };

  template<class PAGELAYOUT>
    lsmTableHandle <PAGELAYOUT> * TlsmTableStart(recordid& tree) {
    /// XXX xid for daemon processes?
    lsmTableHeader_t h;
    Tread(-1, tree, &h);

    typedef treeIterator<typename PAGELAYOUT::FMT::TUP,
      typename PAGELAYOUT::FMT> LSM_ITER;

    typedef stlSetIterator<typename std::set<typename PAGELAYOUT::FMT::TUP,
      typename PAGELAYOUT::FMT::TUP::stl_cmp>,
      typename PAGELAYOUT::FMT::TUP> RB_ITER;

    typedef typename LSM_ITER::handle LSM_HANDLE;
    typedef typename RB_ITER::handle RB_HANDLE;

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

    LSM_HANDLE ** block1_scratch = (LSM_HANDLE**) malloc(sizeof(LSM_HANDLE*));
    *block1_scratch = 0;

    RB_HANDLE ** block0_scratch = (RB_HANDLE**) malloc(sizeof(RB_HANDLE*));
    *block0_scratch = 0;

    lsmTableHandle<PAGELAYOUT> * ret = (lsmTableHandle<PAGELAYOUT>*)
      malloc(sizeof(lsmTableHandle<PAGELAYOUT>));

    // merge1_thread initialized during pthread_create, below.
    // merge2_thread initialized during pthread_create, below.

    ret->still_open = (bool*)malloc(sizeof(bool));
    *ret->still_open = 1;

    ret->input_handle = block0_scratch;
    ret->scratch_handle = new typeof(*ret->scratch_handle);

    ret->mut = block_ready_mut;

    ret->input_ready_cond = block0_ready_cond;
    ret->input_needed_cond = block0_needed_cond;


    recordid * ridp = (recordid*)malloc(sizeof(recordid));
    *ridp = h.bigTreeAllocState;

    ret->args1 = (merge_args<PAGELAYOUT,LSM_ITER,LSM_ITER>*)malloc(sizeof(merge_args<PAGELAYOUT,LSM_ITER,LSM_ITER>));
    merge_args<PAGELAYOUT, LSM_ITER, LSM_ITER> tmpargs1 =
      {
	TlsmRegionAllocRid,
	ridp,
	block_ready_mut,
	block1_needed_cond,
	block2_needed_cond,
	block1_ready_cond,
	block2_ready_cond,
	ret->still_open,
	NULLRID,
	block1_scratch,
	0
      };
    *ret->args1 = tmpargs1;
    void * (*merger1)(void*) = mergeThread<PAGELAYOUT, LSM_ITER, LSM_ITER>;

    ridp = (recordid*)malloc(sizeof(recordid));
    *ridp = h.mediumTreeAllocState;

    ret->args2 = (merge_args<PAGELAYOUT,LSM_ITER,RB_ITER>*)malloc(sizeof(merge_args<PAGELAYOUT,LSM_ITER,RB_ITER>));
    merge_args<PAGELAYOUT, LSM_ITER, RB_ITER> tmpargs2 =
      {
	TlsmRegionAllocRid,
	ridp,
	block_ready_mut,
	block0_needed_cond,
	block1_needed_cond,
	block0_ready_cond,
	block1_ready_cond,
	ret->still_open,
	NULLRID,
	block0_scratch,
	block1_scratch
      };
    *ret->args2 = tmpargs2;
    void * (*merger2)(void*) = mergeThread<PAGELAYOUT, LSM_ITER, RB_ITER>;

    pthread_create(&ret->merge1_thread, 0, merger1, ret->args1);
    pthread_create(&ret->merge2_thread, 0, merger2, ret->args2);

    return ret;
  }
  template<class PAGELAYOUT>
    void TlsmTableStop( lsmTableHandle<PAGELAYOUT> * h) {
    *(h->still_open) = 0;
    pthread_join(h->merge1_thread,0);
    pthread_join(h->merge2_thread,0);
  }
  template<class PAGELAYOUT>
    void TlsmTableInsert( lsmTableHandle<PAGELAYOUT> *h,
			  typename PAGELAYOUT::FMT::TUP &t) {
    h->scratch_handle->insert(t);
    if(h->scratch_handle->size() > 100000) { // XXX set threshold sanely!!!
      pthread_mutex_lock(h->mut);

      while(*h->input_handle) {
	pthread_cond_wait(h->input_needed_cond, h->mut);
      }

      typeof(h->scratch_handle)* tmp_ptr
	= (typeof(h->scratch_handle)*) malloc(sizeof(void*));
      *tmp_ptr = h->scratch_handle;
      *(h->input_handle) = tmp_ptr;

      pthread_cond_signal(h->input_ready_cond);

      h->scratch_handle = new typeof(*h->scratch_handle);

      pthread_mutex_unlock(h->mut);
    }

  }
}

#endif  // _ROSE_COMPRESSION_LSMTABLE_H__
