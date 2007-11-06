#ifndef _ROSE_COMPRESSION_LSMWORKERS_H__
#define _ROSE_COMPRESSION_LSMWORKERS_H__

#include <unistd.h>

/**
   @file

   This file contains the worker thread implementations required for
   the LSM tree and its benchmarking code.
*/
namespace rose {

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

template<class PAGELAYOUT,class ENGINE,class ITER,class ROW>
struct insert_args {
  int comparator_idx;
  int rowsize;typedef int32_t val_t;
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

/**
   The following initPage() functions initialize the page that is
   passed into them, and allocate a new PAGELAYOUT object of the
   appropriate type.
*/
template <class COMPRESSOR, class TYPE, class ROW>
inline Pstar<COMPRESSOR, TYPE> * initPage(Pstar<COMPRESSOR,TYPE> **pstar,
					  Page *p, const TYPE current) {
  *pstar = new Pstar<COMPRESSOR, TYPE>(-1, p);
  (*pstar)->compressor()->offset(current);
  return *pstar;
}
template <class COMPRESSOR, class TYPE, class ROW>
inline Pstar<COMPRESSOR, TYPE> * initPage(Pstar<COMPRESSOR,TYPE> **pstar,
					  Page *p, const ROW & current) {
  *pstar = new Pstar<COMPRESSOR, TYPE>(-1, p);
  (*pstar)->compressor()->offset(current);
  return *pstar;
}

template <class COMPRESSOR, class TYPE, class ROW >
inline Multicolumn<ROW> * initPage(Multicolumn<ROW> ** mc,
					    Page *p, const ROW & t) {
  column_number_t column_count = t.column_count();
  plugin_id_t plugin_id =
    rose::plugin_id<Multicolumn<ROW>,COMPRESSOR,TYPE>();

  plugin_id_t * plugins = new plugin_id_t[column_count];
  for(column_number_t c = 0; c < column_count; c++) {
    plugins[c] = plugin_id;
  }

  *mc = new Multicolumn<ROW>(-1,p,column_count,plugins);
  for(column_number_t c = 0; c < column_count; c++) {
    ((COMPRESSOR*)(*mc)->compressor(c))->offset(*t.get(c));
  }

  delete [] plugins;
  return *mc;
}
template <class COMPRESSOR, class TYPE, class ROW >
inline Multicolumn<ROW> * initPage(Multicolumn<ROW> ** mc,
					    Page *p, const TYPE t) {
  plugin_id_t plugin_id =
    rose::plugin_id<Multicolumn<ROW>,COMPRESSOR,TYPE>();

  plugin_id_t * plugins = new plugin_id_t[1];
  plugins[0] = plugin_id;

  *mc = new Multicolumn<ROW>(-1,p,1,plugins);
  ((COMPRESSOR*)(*mc)->compressor(0))->offset(t);

  delete [] plugins;
  return *mc;
}

/**
   Create pages that are managed by Pstar<COMPRESSOR, TYPE>, and
   use them to store a compressed representation of the data set.

   @param dataset A pointer to the data that should be compressed.
   @param inserts The number of elements in dataset.

   @return the number of pages that were needed to store the
   compressed data.
*/
template <class PAGELAYOUT, class COMPRESSOR, class TYPE, class ROW, class ITER>
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
    TlsmAppendPage(-1,tree,toByteArray(begin),pageAlloc,pageAllocState,p->id);
  }
  pageCount++;

  PAGELAYOUT * mc;
  initPage<COMPRESSOR,TYPE,ROW>(&mc, p, **begin);

  int lastEmpty = 0;

  for(ITER i(*begin); i != *end; ++i) {
    rose::slot_index_t ret = mc->append(-1, *i);

    (*inserted)++;

    if(ret == rose::NOSPACE) {
      p->dirty = 1;
      mc->pack();
      releasePage(p);
      // XXX this used to work by decrementing i, and then running
      // through again.  That failed when i was right before end.
      // figure out why it was broken, fix the iterators (?), and write
      // a test case for this situation...
      //      --(*end);
      //      if(i != *end) {
        next_page = pageAlloc(-1,pageAllocState);
        p = loadPage(-1, next_page);

	mc = initPage<COMPRESSOR, TYPE, ROW>(&mc, p, *i);

	if(buildTree) {
	  TlsmAppendPage(-1,tree,toByteArray(&i),pageAlloc,pageAllocState,p->id);
	}
        pageCount++;
	ret = mc->append(-1, *i);
	assert(ret != rose::NOSPACE);
	//        lastEmpty = 0;
	//      } else {
	//        lastEmpty = 1;
	//      }
	//      ++(*end);
	//      --i;
    }
  }

  p->dirty = 1;
  mc->pack();
  releasePage(p);
  return pageCount;
}

template<class PAGELAYOUT,class ENGINE,class ITER,class ROW,class TYPE>
void* insertThread(void* arg) {
  insert_args<PAGELAYOUT,ENGINE,ITER,ROW>* a =
    (insert_args<PAGELAYOUT,ENGINE,ITER,ROW>*)arg;

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
    recordid tree = TlsmCreate(-1, a->comparator_idx,a->pageAlloc, a->pageAllocState, a->rowsize);
    lastTreeBlocks =
      compressData<PAGELAYOUT,ENGINE,TYPE,ROW,ITER>
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

/**
   ITERA is an iterator over the data structure that mergeThread creates (a lsm tree iterator).
   ITERB is an iterator over the data structures that mergeThread takes as input (lsm tree, or rb tree..)
 */
template<class PAGELAYOUT, class ENGINE, class ITERA, class ITERB,
  class ROW, class TYPE>
void* mergeThread(void* arg) {
  // The ITER argument of a is unused (we don't look at it's begin or end fields...)
  insert_args<PAGELAYOUT,ENGINE,ITERA,ROW>* a =
    (insert_args<PAGELAYOUT,ENGINE,ITERA,ROW>*)arg;

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

    mergeIterator<ITERA, ITERB, ROW>
      mBegin(taBegin, tbBegin, *taEnd, *tbEnd);

    mergeIterator<ITERA, ITERB, ROW>
      mEnd(taBegin, tbBegin, *taEnd, *tbEnd);


    mEnd.seekEnd();
    uint64_t insertedTuples;
    pageid_t mergedPages = compressData<PAGELAYOUT,ENGINE,TYPE,ROW,
      mergeIterator<ITERA, ITERB, ROW> >
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

}

#endif //  _ROSE_COMPRESSION_LSMWORKERS_H__
