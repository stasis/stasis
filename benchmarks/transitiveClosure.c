#include <lladd/transactional.h>
#include <lladd/multiplexer.h>
#include <lladd/graph.h>
#include "../src/lladd/logger/logMemory.h"
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#define NUM_NODES 1000000
#define DIRTY_BUF_SIZE 20000000
#define OUTDEGREE 3
#define NUM_THREADS 1
#define NUM_FIFOS 5

#define NUM_ITER 2

int transClos_outdegree = OUTDEGREE;

/** This is a hack */
/*int countRecordsPerPage(int outDegree) {
  Tinit();
  int xid = Tbegin();
  // Relies upon a bug in lladd that will always start allocation on a fresh page.
  recordid rid = Talloc(xid, sizeof(int) * outDegree);  
  int oldPage = rid.page;
  int count = 1;
  while(oldPage == rid.page) {
    Talloc(xid, sizeof(int) * outDegree);
    count++;
  }
  Tdeinit();  // When LLADD is restarted, it will leak the last page we allocated a record on.  This is the "hack".
  return count;

  }*/

int numOut = 0;
int numTset = 0;
int numShortcutted = 0;
int numSkipped = 0;
int numPushed = 0;

typedef struct {
  lladdFifo_t * dirty;
  lladdFifo_t * global;
  lladdFifoPool_t * pool;
  lladdMultiplexer_t * mux;
  recordid rid;
  int i;
} worker_arg;

int closed = 0;
pthread_mutex_t closed_mut = PTHREAD_MUTEX_INITIALIZER;



void * worker(void * arg_p) {
  worker_arg * arg = (worker_arg*)arg_p;
  lladdFifo_t * dirtyFifo = arg->dirty;
  lladdFifo_t * globalFifo = arg->global;
  recordid rid = arg->rid;
  int i = arg->i;
  int xid = Tbegin();
  
  lladdMultiplexer_flush(arg->mux);

  while(Titerator_next(xid, dirtyFifo->iterator)) {

  
    lladdFifo_t ** localFifo_ptr;
    lladdFifo_t * localFifo;
    
    int size = Titerator_value(xid, dirtyFifo->iterator, (byte**) &localFifo_ptr);
    
    localFifo = *localFifo_ptr;
    
    assert(size == sizeof(lladdFifo_t*));
    
    multiTraverse(xid, rid, localFifo, globalFifo, arg->pool, i+NUM_ITER);
    
    
    Titerator_tupleDone(xid, dirtyFifo->iterator);

    if(!numOut) { break ;}

    lladdMultiplexer_flush(arg->mux);
    
  }

  pthread_mutex_lock(&closed_mut);
  if(!closed) {
    Tconsumer_close(xid, globalFifo->consumer);
    closed = 1;
  }
  pthread_mutex_unlock(&closed_mut);
  


  Tcommit(xid);
  return NULL;
}


int main(int argc, char ** argv) {
 
  srandom(0);

  Tinit();

  int xid = Tbegin();
  //  int lastPage = -1;
  //  int pageCount = 0;

  //  int max_fanout = 0;

  /*  for(i = 0; i < NUM_NODES; i++) {
      recordid rid = Talloc(xid, sizeof(nodeHeader_t) + (sizeof(recordid) * max_fanout));
      if(lastPage != rid.page && lastPage != -1) {
      rids_per_page[lastPage] = pageCount;
      lastPage = rid.page;
      pageCount = 0;
      }
      pageCount ++;
      } 
  */
  
  recordid rid = TarrayListAlloc(xid, NUM_NODES / 100, 2, sizeof(long) * (OUTDEGREE+1));
  
  TarrayListExtend(xid, rid, NUM_NODES);

  int * node = malloc(sizeof(int) * (OUTDEGREE+1));  // the last long in the node holds flags.
  int i, j;
  for(i = 0; i < NUM_NODES; i++) {
    node[OUTDEGREE] = 0;
#ifdef RING
    node[0] = (i+1) % NUM_NODES;
    abort();
    for(j = 1; j < OUTDEGREE; j++) {
#else
    for(j = 0; j < OUTDEGREE; j++) {
#endif
      node[j] = (int)(  ((double)NUM_NODES) * (double)random()/((double)RAND_MAX+1.0));
      //     printf("%d ", node[j]);
    }
    rid.slot = i;
    Tset(xid, rid, node);
  }

  printf("Nodes loaded.\n");
  fflush(stdout);

  rid.slot = 0;

  system("date");

  numTset = 0;
  for(i = 1; i <= NUM_ITER; i++) {
    
       naiveTraverse(xid, rid, i);
  
  }

  system("date");

  printf("TransClos returned Tset called %d times\n", numTset);
  fflush(stdout);

  numTset = 0;
  numPushed = 0;
  Tcommit(xid);


  for(i = 1; i <= NUM_ITER; i++) {
    lladdFifo_t * dirtyFifo = logMemoryFifo(DIRTY_BUF_SIZE, 0);
    lladdFifoPool_t * pool = lladdFifoPool_pointerPoolInit(NUM_FIFOS, DIRTY_BUF_SIZE, lladdFifoPool_getFifoCRC32, dirtyFifo);

    lladdFifo_t * globalFifo = logMemoryFifo(DIRTY_BUF_SIZE, /*NUM_FIFOS*/ 0);
    lladdMultiplexer_t * mux = lladdMultiplexer_alloc(xid, globalFifo->iterator, &multiplexByRidPage, pool);

    //    lladdMultiplexer_start(mux, NULL);
    rid.slot =0;
    Tconsumer_push(xid, globalFifo->consumer, NULL, 0, (byte*)&rid, sizeof(recordid));
    numOut = 1;

    pthread_t * workers = malloc(sizeof(pthread_t) * NUM_THREADS); 
    int j;

    worker_arg * arg = malloc(sizeof(worker_arg));
    arg->dirty = dirtyFifo;
    arg->global = globalFifo;
    arg->pool   = pool;
    arg->rid = rid;
    arg->mux = mux;
    arg->i = i;
    for(j = 0; j < NUM_THREADS; j++) {
      pthread_create(&workers[j], NULL, worker, arg);
    }
    for(j = 0; j < NUM_THREADS; j++) {
      pthread_join(workers[j], NULL);
    }
    //    lladdMultiplexer_join(mux);

    closed = 0;
  
  }

  system("date");
  printf("FastTransClos returned Tset called %d times, shortcutted %d, skipped %d pushed %d\n", numTset, numShortcutted, numSkipped, numPushed);

  Tdeinit();
}
