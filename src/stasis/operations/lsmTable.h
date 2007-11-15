#ifndef _ROSE_COMPRESSION_LSMTABLE_H__
#define _ROSE_COMPRESSION_LSMTABLE_H__

#undef end
#undef begin

#define INFINITE_RESOURCES
#define THROTTLED
#include <math.h>
#include <set>
#include "lsmIterators.h"
#include <stasis/truncation.h>

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
      int worker_id;
      pageid_t(*pageAlloc)(int,void*);
      void *pageAllocState;
      void *oldAllocState;
      pthread_mutex_t * block_ready_mut;
      pthread_cond_t * in_block_needed_cond;
      bool * in_block_needed;
      pthread_cond_t * out_block_needed_cond;
      bool * out_block_needed;
      pthread_cond_t * in_block_ready_cond;
      pthread_cond_t * out_block_ready_cond;
      bool * still_open;
      pageid_t * my_tree_size;
      pageid_t * out_tree_size;
      pageid_t max_size;
      pageid_t r_i;
      typename ITERB::handle ** in_tree;
      void * in_tree_allocer;
      typename ITERA::handle ** out_tree;
      void * out_tree_allocer;
      typename ITERA::handle my_tree;
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

    pageid_t pageCount = 0;

    if(*begin != *end) {
      TlsmAppendPage(xid,tree,(**begin).toByteArray(),pageAlloc,
		     pageAllocState,next_page);
    }
    pageCount++;

    Page *p = loadPage(xid, next_page);
    stasis_page_cleanup(p);
    typename PAGELAYOUT::FMT * mc = PAGELAYOUT::initPage(p, &**begin);

    for(ITER i(*begin); i != *end; ++i) {
      rose::slot_index_t ret = mc->append(xid, *i);

      if(ret == rose::NOSPACE) {
	dirtyPages_add(p);
	//	p->dirty = 1;
	mc->pack();
	releasePage(p);
	next_page = pageAlloc(xid,pageAllocState);
	TlsmAppendPage(xid,tree,(*i).toByteArray(),pageAlloc,pageAllocState,next_page);
	p = loadPage(xid, next_page);
	mc = PAGELAYOUT::initPage(p, &*i);
	pageCount++;
	ret = mc->append(xid, *i);
	assert(ret != rose::NOSPACE);
      }
      (*inserted)++;
    }
    dirtyPages_add(p);
    //    p->dirty = 1;
    mc->pack();
    releasePage(p);
    return pageCount;
  }


  // How many bytes of tuples can we afford to keep in RAM?
  // this is just a guessed value... it seems about right based on
  // experiments, but 450 bytes overhead per tuple is insane!
  static const int RB_TREE_OVERHEAD = 400; // = 450;
  static const pageid_t MEM_SIZE = 1000 * 1000 * 1000;
  //  static const pageid_t MEM_SIZE = 100 * 1000;
  // How many pages should we try to fill with the first C1 merge?
  static int R = 10; // XXX set this as low as possible (for dynamic setting.  = sqrt(C2 size / C0 size))
#ifdef THROTTLED
  static const pageid_t START_SIZE = 100; //10 * 1000; /*10 **/ //1000; // XXX 4 is fudge related to RB overhead.
#else
  static const pageid_t START_SIZE = MEM_SIZE * R /( PAGE_SIZE * 4); //10 * 1000; /*10 **/ //1000; // XXX 4 is fudge related to RB overhead.
#endif
  // Lower total work by perfomrming one merge at higher level
  // for every FUDGE^2 merges at the immediately lower level.
  // (Constrast to R, which controls the ratio of sizes of the trees.)
  static const int FUDGE = 1;



  /**
     ITERA is an iterator over the data structure that mergeThread creates (a lsm tree iterator).
     ITERB is an iterator over the data structures that mergeThread takes as input (lsm tree, or rb tree..)
  */
  template<class PAGELAYOUT, class ITERA, class ITERB>
    void* mergeThread(void* arg) {
    // The ITER argument of a is unused (we don't look at it's begin or end fields...)
    merge_args<PAGELAYOUT, ITERA, ITERB> * a = (merge_args<PAGELAYOUT, ITERA, ITERB>*)arg;
    struct timeval start_tv, start_push_tv, wait_tv, stop_tv;
    int merge_count = 0;

    int xid = Tbegin();
    // Initialize tree with an empty tree.
    // XXX hardcodes ITERA's type:
    // We assume that the caller set pageAllocState for us; oldPageAllocState
    // shouldn't be set (it should be NULLRID)
    typename ITERA::handle tree
      = new typename ITERA::treeIteratorHandle(
		TlsmCreate(xid, PAGELAYOUT::cmp_id(),a->pageAlloc,
		a->pageAllocState,PAGELAYOUT::FMT::TUP::sizeofBytes()) );

    // loop around here to produce multiple batches for merge.
    gettimeofday(&start_push_tv,0);
    gettimeofday(&start_tv,0);
    while(1) {
      pthread_mutex_lock(a->block_ready_mut);

      int done = 0;

      while(!*(a->in_tree)) {
	*a->in_block_needed = true;
	pthread_cond_signal(a->in_block_needed_cond);
	if(!*(a->still_open)) {
	  done = 1;
	  break;
	}
	pthread_cond_wait(a->in_block_ready_cond,a->block_ready_mut);
      }
      *a->in_block_needed = false;
      if(done) {
	pthread_cond_signal(a->out_block_ready_cond);
	pthread_mutex_unlock(a->block_ready_mut);
	break;
      }

      gettimeofday(&wait_tv,0);

      uint64_t insertedTuples;
      pageid_t mergedPages;
      ITERA *taBegin = new ITERA(tree);
      ITERB *tbBegin = new ITERB(**a->in_tree);

      ITERA *taEnd = taBegin->end();
      ITERB *tbEnd = tbBegin->end();
      { // this { protects us from recalcitrant iterators below (tree iterators hold stasis page latches...)

      pthread_mutex_unlock(a->block_ready_mut);

      Tcommit(xid);
      xid = Tbegin();
      // XXX hardcodes allocator type.
      if(((recordid*)a->oldAllocState)->size != -1) {
	// free the tree that we merged against during the last round.
	TlsmFree(xid,tree->r_,TlsmRegionDeallocRid,a->oldAllocState);
      }
      // we're merging against old alloc state this round.
      *(recordid*)(a->oldAllocState) = *(recordid*)(a->pageAllocState);
      // we're merging into pagealloc state.
      *(recordid*)(a->pageAllocState) =	Talloc(xid, sizeof(TlsmRegionAllocConf_t));
      Tset(xid, *(recordid*)(a->pageAllocState),
	   &LSM_REGION_ALLOC_STATIC_INITIALIZER);
      tree->r_ = TlsmCreate(xid, PAGELAYOUT::cmp_id(),a->pageAlloc,
			a->pageAllocState,PAGELAYOUT::FMT::TUP::sizeofBytes());

      mergeIterator<ITERA, ITERB, typename PAGELAYOUT::FMT::TUP>
	mBegin(*taBegin, *tbBegin, *taEnd, *tbEnd);

      mergeIterator<ITERA, ITERB, typename PAGELAYOUT::FMT::TUP>
	mEnd(*taBegin, *tbBegin, *taEnd, *tbEnd);

      mEnd.seekEnd();

      /*      versioningIterator<mergeIterator
	<ITERA,ITERB,typename PAGELAYOUT::FMT::TUP>,
	typename PAGELAYOUT::FMT::TUP> vBegin(mBegin,mEnd,0);

      versioningIterator<mergeIterator
	<ITERA,ITERB,typename PAGELAYOUT::FMT::TUP>,
	typename PAGELAYOUT::FMT::TUP> vEnd(mBegin,mEnd,0); 

	vEnd.seekEnd(); 


      mergedPages = compressData
	<PAGELAYOUT,versioningIterator<mergeIterator<ITERA,ITERB,typename PAGELAYOUT::FMT::TUP>, typename PAGELAYOUT::FMT::TUP> >
	(xid, &vBegin, &vEnd,tree->r_,a->pageAlloc,a->pageAllocState,&insertedTuples); */
      mergedPages = compressData
	<PAGELAYOUT,mergeIterator<ITERA,ITERB,typename PAGELAYOUT::FMT::TUP> >
	(xid, &mBegin, &mEnd,tree->r_,a->pageAlloc,a->pageAllocState,&insertedTuples);  

      // these tree iterators keep pages pinned!  Don't call force until they've been deleted, or we'll deadlock.

      } // free all the stack allocated iterators...
      delete taBegin;
      delete tbBegin;
      delete taEnd;
      delete tbEnd;
      // XXX hardcodes tree type.
      TlsmForce(xid,tree->r_,TlsmRegionForceRid,a->pageAllocState);

      gettimeofday(&stop_tv,0);

      merge_count++;

      double wait_elapsed  = tv_to_double(wait_tv) - tv_to_double(start_tv);
      double work_elapsed  = tv_to_double(stop_tv) - tv_to_double(wait_tv);
      double push_elapsed = tv_to_double(start_tv) - tv_to_double(start_push_tv);
      double total_elapsed = wait_elapsed + work_elapsed;
      double ratio = ((double)(insertedTuples * (uint64_t)PAGELAYOUT::FMT::TUP::sizeofBytes()))
	/ (double)(PAGE_SIZE * mergedPages);
      double throughput = ((double)(insertedTuples * (uint64_t)PAGELAYOUT::FMT::TUP::sizeofBytes()))
	/ (1024.0 * 1024.0 * total_elapsed);

      printf("worker %d merge # %-6d: comp ratio: %-9.3f  stalled %6.1f sec backpressure %6.1f "
	     "worked %6.1f sec inserts %-12ld (%9.3f mb/s) %6ld pages (need %6ld)\n", a->worker_id, merge_count, ratio,
	     wait_elapsed, push_elapsed, work_elapsed,(unsigned long)insertedTuples, throughput, mergedPages, !a->out_tree_size ? -1 :  (FUDGE * *a->out_tree_size / a->r_i));


      gettimeofday(&start_push_tv,0);

      pthread_mutex_lock(a->block_ready_mut);

      // keep actual handle around so that it can be freed below.
      typename ITERB::handle old_in_tree = **a->in_tree;
      if(a->in_tree_allocer) {
	//	TlsmFree(xid, ((typename ITERB::handle)old_in_tree)->r_,TlsmRegionDeallocRid,a->in_tree_allocer);
	// XXX kludge; assumes C1 and C2 have same type of handle....
	TlsmFree(xid, ((typename ITERA::handle)old_in_tree)->r_,TlsmRegionDeallocRid,a->in_tree_allocer);
	delete old_in_tree;
      } else {
	((typename stlSetIterator<std::set<typename PAGELAYOUT::FMT::TUP, 
	  typename PAGELAYOUT::FMT::TUP::stl_cmp>,
	  typename PAGELAYOUT::FMT::TUP>::handle)old_in_tree)->clear();
	delete
	  ((typename stlSetIterator<std::set<typename PAGELAYOUT::FMT::TUP,
	    typename PAGELAYOUT::FMT::TUP::stl_cmp>,
	    typename PAGELAYOUT::FMT::TUP>::handle)old_in_tree);
      }

      free(*a->in_tree); // free pointer to handle

      // XXX should we delay this to this point?
      //   otherwise, the contents of in_tree become temporarily unavailable to observers.
      *a->in_tree = 0; // tell producer that the slot is now open

      // don't set in_block_needed = true; we're not blocked yet.
      pthread_cond_signal(a->in_block_needed_cond);


#ifdef INFINITE_RESOURCES
      *a->my_tree_size = mergedPages;
      double target_R = 0;
      if(a->out_tree) {
	double frac_wasted = ((double)RB_TREE_OVERHEAD)/(double)(RB_TREE_OVERHEAD + PAGELAYOUT::FMT::TUP::sizeofBytes());

	target_R = sqrt(((double)(*a->out_tree_size+*a->my_tree_size)) / ((MEM_SIZE*(1-frac_wasted))/(4096*ratio)));
	printf("R_C2-C1 = %6.1f R_C1-C0 = %6.1f target = %6.1f\n", 
	       ((double)(*a->out_tree_size+*a->my_tree_size)) / ((double)*a->my_tree_size), 
	       ((double)*a->my_tree_size) / ((double)(MEM_SIZE*(1-frac_wasted))/(4096*ratio)),target_R);
      }
#else
      if(a->out_tree_size) {
	*a->my_tree_size = *a->out_tree_size / (a->r_i * FUDGE);
      } else {
	if(*a->my_tree_size < mergedPages) {
	  *a->my_tree_size = mergedPages;
	}
      }
#endif
      if(a->out_tree &&  // is there a upstream merger? (note the lack of the * on a->out_tree)
	 (
	  (
#ifdef INFINITE_RESOURCES
	  (*a->out_block_needed && 0)
#ifdef THROTTLED
	  || ((double)*a->out_tree_size / ((double)*a->my_tree_size) < target_R)
#endif
#else
	  mergedPages > (FUDGE * *a->out_tree_size / a->r_i) // do we have enough data to bother it?
#endif
	   )
	  ||
	  (a->max_size && mergedPages > a->max_size )
	  )) {

	// XXX need to report backpressure here!

	while(*a->out_tree) { // we probably don't need the "while..."
	  pthread_cond_wait(a->out_block_needed_cond, a->block_ready_mut);
	}
#ifdef INFINITE_RESOURCES
	//	printf("pushing tree R_eff = %6.1f target = %6.1f\n", ((double)*a->out_tree_size) / ((double)*a->my_tree_size), target_R);
#endif
	// XXX C++?  Objects?  Constructors? Who needs them?
	*a->out_tree = (typeof(*a->out_tree))malloc(sizeof(**a->out_tree));
	**a->out_tree = new typename ITERA::treeIteratorHandle(tree->r_);
	pthread_cond_signal(a->out_block_ready_cond);

	// This is a bit wasteful; allocate a new empty tree to merge against.
	// We don't want to ever look at the one we just handed upstream...
	// We could wait for an in tree to be ready, and then pretend
	// that we've just finished merging against it (to avoid all
	// those merging comparisons, and a table scan...)

	// old alloc state contains the tree that we used as input for this merge... we can still free it

	*(recordid*)(a->out_tree_allocer) = *(recordid*)(a->pageAllocState);
	*(recordid*)(a->pageAllocState) = NULLRID;

	// create a new allocator.
	*(recordid*)(a->pageAllocState) = Talloc(xid, sizeof(TlsmRegionAllocConf_t));
	Tset(xid, *(recordid*)(a->pageAllocState),
	     &LSM_REGION_ALLOC_STATIC_INITIALIZER);

	tree->r_ = TlsmCreate(xid, PAGELAYOUT::cmp_id(),a->pageAlloc,
			a->pageAllocState,PAGELAYOUT::FMT::TUP::sizeofBytes());

      }

      // XXX   TlsmFree(xid,*a->tree);

      assert(a->my_tree->r_.page != tree->r_.page);
      *a->my_tree = *tree;

      pthread_mutex_unlock(a->block_ready_mut);

      gettimeofday(&start_tv,0);

    }
    Tcommit(xid);

    return 0;
  }
  typedef struct {
    recordid bigTree;
    recordid bigTreeAllocState; // this is probably the head of an arraylist of regions used by the tree...
    recordid oldBigTreeAllocState; // this is probably the head of an arraylist of regions used by the tree...
    recordid mediumTree;
    recordid mediumTreeAllocState;
    recordid oldMediumTreeAllocState;
    epoch_t beginning;
    epoch_t end;
  } lsmTableHeader_t;


  template<class PAGELAYOUT>
    inline recordid TlsmTableAlloc(int xid) {

    recordid ret = Talloc(xid, sizeof(lsmTableHeader_t));
    lsmTableHeader_t h;
    h.oldBigTreeAllocState = NULLRID;
    h.bigTreeAllocState = Talloc(xid,sizeof(TlsmRegionAllocConf_t));
    Tset(xid,h.bigTreeAllocState,&LSM_REGION_ALLOC_STATIC_INITIALIZER);
    h.bigTree = TlsmCreate(xid, PAGELAYOUT::cmp_id(),
			   TlsmRegionAllocRid,&h.bigTreeAllocState,
			   PAGELAYOUT::FMT::TUP::sizeofBytes());
    h.oldMediumTreeAllocState = NULLRID;
    h.mediumTreeAllocState = Talloc(xid,sizeof(TlsmRegionAllocConf_t));
    Tset(xid,h.mediumTreeAllocState,&LSM_REGION_ALLOC_STATIC_INITIALIZER);
    h.mediumTree = TlsmCreate(xid, PAGELAYOUT::cmp_id(),
			      TlsmRegionAllocRid,&h.mediumTreeAllocState,
			      PAGELAYOUT::FMT::TUP::sizeofBytes());
    //XXX epoch_t beginning = 0;
    //XXX epoch_t end = 0;
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
    bool * input_needed;
    typename std::set
      <typename PAGELAYOUT::FMT::TUP,
      typename PAGELAYOUT::FMT::TUP::stl_cmp> * scratch_handle;
    pthread_mutex_t * mut;
    pthread_cond_t * input_ready_cond;
    pthread_cond_t * input_needed_cond;
    pageid_t * input_size;
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

    pageid_t * block0_size = (pageid_t*)malloc(sizeof(pageid_t));
    // don't merge until we have enough data to be worthwhile...
    *block0_size = START_SIZE;
    pageid_t * block1_size = (pageid_t*)malloc(sizeof(pageid_t));
    // similarly, wait to merge the next block until we have merged block FUDGE times.
    *block1_size = FUDGE * R * *block0_size;

    LSM_HANDLE ** block1_scratch = (LSM_HANDLE**) malloc(sizeof(LSM_HANDLE*));
    *block1_scratch = 0;

    RB_HANDLE ** block0_scratch = (RB_HANDLE**) malloc(sizeof(RB_HANDLE*));
    *block0_scratch = 0;

    bool *block0_needed = (bool*)malloc(sizeof(bool));
    bool *block1_needed = (bool*)malloc(sizeof(bool));
    bool *block2_needed = (bool*)malloc(sizeof(bool));
    *block0_needed = false;
    *block1_needed = false;
    *block2_needed = false;

    lsmTableHandle<PAGELAYOUT> * ret = (lsmTableHandle<PAGELAYOUT>*)
      malloc(sizeof(lsmTableHandle<PAGELAYOUT>));

    // merge1_thread initialized during pthread_create, below.
    // merge2_thread initialized during pthread_create, below.

    ret->still_open = (bool*)malloc(sizeof(bool));
    *ret->still_open = 1;

    ret->input_handle = block0_scratch;
    ret->input_needed = block0_needed;
    ret->scratch_handle = new typeof(*ret->scratch_handle);

    ret->mut = block_ready_mut;

    ret->input_ready_cond = block0_ready_cond;
    ret->input_needed_cond = block0_needed_cond;
    ret->input_size = block0_size;

    recordid * ridp = (recordid*)malloc(sizeof(recordid));
    *ridp = h.bigTreeAllocState;
    recordid * oldridp = (recordid*)malloc(sizeof(recordid));
    *oldridp = NULLRID;

    ret->args1 = (merge_args<PAGELAYOUT,LSM_ITER,LSM_ITER>*)malloc(sizeof(merge_args<PAGELAYOUT,LSM_ITER,LSM_ITER>));

    recordid * allocer_scratch = (recordid*)malloc(sizeof(recordid));
    merge_args<PAGELAYOUT, LSM_ITER, LSM_ITER> tmpargs1 =
      {
	1,
	TlsmRegionAllocRid,
	ridp,
	oldridp,
	block_ready_mut,
	block1_needed_cond,
	block1_needed,
	block2_needed_cond,
	block2_needed,
	block1_ready_cond,
	block2_ready_cond,
	ret->still_open,
	block1_size,
	0, // biggest component computes its size directly.
	0, // No max size for biggest component
	R,
	block1_scratch,
	allocer_scratch,
	0,
	0,
	new typename LSM_ITER::treeIteratorHandle(NULLRID)
      };
    *ret->args1 = tmpargs1;
    void * (*merger1)(void*) = mergeThread<PAGELAYOUT, LSM_ITER, LSM_ITER>;

    ridp = (recordid*)malloc(sizeof(recordid));
    *ridp = h.mediumTreeAllocState;
    oldridp = (recordid*)malloc(sizeof(recordid));
    *oldridp = NULLRID;

    ret->args2 = (merge_args<PAGELAYOUT,LSM_ITER,RB_ITER>*)malloc(sizeof(merge_args<PAGELAYOUT,LSM_ITER,RB_ITER>));
    merge_args<PAGELAYOUT, LSM_ITER, RB_ITER> tmpargs2 =
      {
	2,
	TlsmRegionAllocRid,
	ridp,
	oldridp,
	block_ready_mut,
	block0_needed_cond,
	block0_needed,
	block1_needed_cond,
	block1_needed,
	block0_ready_cond,
	block1_ready_cond,
	ret->still_open,
	block0_size,
	block1_size,
	(R * MEM_SIZE) / (PAGE_SIZE * 4),  // XXX 4 = estimated compression ratio
	R,
	//new typename LSM_ITER::treeIteratorHandle(NULLRID),
	block0_scratch,
	0,
	block1_scratch,
	allocer_scratch,
	new typename LSM_ITER::treeIteratorHandle(NULLRID)
      };
    *ret->args2 = tmpargs2;
    void * (*merger2)(void*) = mergeThread<PAGELAYOUT, LSM_ITER, RB_ITER>;

    pthread_create(&ret->merge1_thread, 0, merger1, ret->args1);
    pthread_create(&ret->merge2_thread, 0, merger2, ret->args2);

    return ret;
  }
  // XXX this does not force the table to disk... it simply forces everything out of the in-memory tree.
  template<class PAGELAYOUT>
    void TlsmTableFlush(lsmTableHandle<PAGELAYOUT> *h) {

      struct timeval start_tv, stop_tv;
      double start, stop;

      static double last_start;
      static bool first = 1;

      gettimeofday(&start_tv,0);
      start = tv_to_double(start_tv);
      pthread_mutex_lock(h->mut);


      while(*h->input_handle) {
	pthread_cond_wait(h->input_needed_cond, h->mut);
      }

      gettimeofday(&stop_tv,0);
      stop = tv_to_double(stop_tv);

      typeof(h->scratch_handle)* tmp_ptr
	= (typeof(h->scratch_handle)*) malloc(sizeof(void*));
      *tmp_ptr = h->scratch_handle;
      *(h->input_handle) = tmp_ptr;

      pthread_cond_signal(h->input_ready_cond);
      h->scratch_handle = new typeof(*h->scratch_handle);

      pthread_mutex_unlock(h->mut);

      if(first) {
	printf("flush waited %lf sec\n", stop-start);
	first = 0;
      } else {
	printf("flush waited %lf sec (worked %lf)\n",
	       stop-start, start-last_start);
      }
      last_start = stop;

  }
  template<class PAGELAYOUT>
    void TlsmTableStop( lsmTableHandle<PAGELAYOUT> * h) {
    TlsmTableFlush(h);
    delete(h->scratch_handle);
    *(h->still_open) = 0;
    pthread_join(h->merge1_thread,0);
    pthread_join(h->merge2_thread,0);
  }
  template<class PAGELAYOUT>
    void TlsmTableInsert( lsmTableHandle<PAGELAYOUT> *h,
			  typename PAGELAYOUT::FMT::TUP &t) {
    h->scratch_handle->insert(t);

    uint64_t handleBytes = h->scratch_handle->size() * (RB_TREE_OVERHEAD + PAGELAYOUT::FMT::TUP::sizeofBytes());
    //XXX  4 = estimated compression ratio.
    uint64_t inputSizeThresh = (4 * PAGE_SIZE * *h->input_size); // / (PAGELAYOUT::FMT::TUP::sizeofBytes());
    uint64_t memSizeThresh = MEM_SIZE;

#ifdef INFINITE_RESOURCES
    static const int LATCH_INTERVAL = 10000;
    static int count = LATCH_INTERVAL; /// XXX HACK
    bool go = false;
    if(!count) {
      pthread_mutex_lock(h->mut);
      go = *h->input_needed;
      pthread_mutex_unlock(h->mut);
      count = LATCH_INTERVAL;
    }
    count --;
#endif
    if(
#ifdef INFINITE_RESOURCES
       go ||
#else 
       handleBytes > inputSizeThresh ||
#endif
       handleBytes > memSizeThresh) { // XXX ok?
      printf("Handle mbytes %ld (%ld) Input size: %ld input size thresh: %ld mbytes mem size thresh: %ld\n",
	     handleBytes / (1024*1024), h->scratch_handle->size(), *h->input_size, inputSizeThresh / (1024*1024), memSizeThresh / (1024*1024));
      TlsmTableFlush<PAGELAYOUT>(h);
    }
  }

  template<class PAGELAYOUT>
    inline typename PAGELAYOUT::FMT::TUP *
    getRecordHelper(int xid, recordid r,
		    typename PAGELAYOUT::FMT::TUP& val,
		    typename PAGELAYOUT::FMT::TUP& scratch,
		    byte *arry) {
    if(r.size == -1) {
      DEBUG("no tree\n");
      return 0;
    }
    pageid_t pid = TlsmFindPage(xid, r, arry);
    if(pid == -1) {
      DEBUG("no page\n");
      return 0;
    }
    Page * p = loadPage(xid,pid);
    typename PAGELAYOUT::FMT * f =
      (typename PAGELAYOUT::FMT*)p->impl;
    typename PAGELAYOUT::FMT::TUP * ret =
      f->recordFind(xid, val, scratch);
    if(!ret) {
      DEBUG("not in tree");
    }
    releasePage(p);
    return ret;
  }

  template<class PAGELAYOUT>
    const typename PAGELAYOUT::FMT::TUP *
    TlsmTableFind(int xid, lsmTableHandle<PAGELAYOUT> *h,
		  typename PAGELAYOUT::FMT::TUP &val,
		  typename PAGELAYOUT::FMT::TUP &scratch) {

    pthread_mutex_lock(h->mut);
    typename std::set
      <typename PAGELAYOUT::FMT::TUP,
       typename PAGELAYOUT::FMT::TUP::stl_cmp>::iterator i =
      h->scratch_handle->find(val);
    if(i != h->scratch_handle->end()) {
      scratch = *i;
      pthread_mutex_unlock(h->mut);
      return &scratch;
    }
    DEBUG("Not in scratch_handle\n");
    if(*h->args2->in_tree) {
      i = (**h->args2->in_tree)->find(val);
      if(i != (**h->args2->in_tree)->end()) {
	scratch = *i;
	pthread_mutex_unlock(h->mut);
	return &scratch;
      }
    }
    DEBUG("Not in first in_tree\n");
    // need to check LSM trees.
    byte * arry = val.toByteArray();

    typename PAGELAYOUT::FMT::TUP * r = 0;
    if(h->args2->my_tree) {
      r = getRecordHelper<PAGELAYOUT>(xid, h->args2->my_tree->r_, val, scratch, arry);
      if(r) { pthread_mutex_unlock(h->mut); return r; }
    }
    DEBUG("Not in first my_tree {%lld}\n", h->args2->my_tree->r_.size);

    if(*h->args1->in_tree) {
      r = getRecordHelper<PAGELAYOUT>(xid, (**h->args1->in_tree)->r_, val, scratch, arry);
      if(r) { pthread_mutex_unlock(h->mut); return r; }
    } else {
      DEBUG("no second in_tree");
    }

    DEBUG("Not in second in_tree\n");
    if(h->args1->my_tree) {
      r = getRecordHelper<PAGELAYOUT>(xid, h->args1->my_tree->r_, val, scratch, arry);
      if(r) { pthread_mutex_unlock(h->mut); return r; }
    } else {
      DEBUG("no tree");
    }

    DEBUG("Not in any tree\n");
    assert(r == 0);
    return r;
  }
}

#endif  // _ROSE_COMPRESSION_LSMTABLE_H__
