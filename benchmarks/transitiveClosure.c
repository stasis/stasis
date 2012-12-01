#include <stasis/transactional.h>
#include <stasis/experimental/multiplexer.h>
#include <stasis/experimental/graph.h>
#include <stasis/experimental/logMemory.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#include <sys/time.h>
#include <time.h>


#define NUM_NODES 1000000
#define DIRTY_BUF_SIZE 60000000
//#define OUTDEGREE 3
#define NUM_THREADS 1
#define NUM_FIFOS 5


#define NUM_ITER 1

int hotSet(int j, int HotSetProb) { 
  int hotset_size = (NUM_NODES / 10);
  int coldset_size = NUM_NODES - hotset_size;
  int id;
  int p = 1 + (int)(100.0 * random() / (RAND_MAX + 1.0));
  
  if (p <= HotSetProb) {
    //    printf("a ");
    id = (int)(((double)hotset_size) * random() /
	       (RAND_MAX + 1.0));
  } else {
    //    printf("b ");
    id = hotset_size + (int)(((double)coldset_size) *
			     random() / (RAND_MAX + 1.0));
  }
  id += j;
  id -= (hotset_size / 2);
  //  printf("%d ", id);
  if(id < 0) { id += NUM_NODES; }
  id %= NUM_NODES;
  return id;
}

lladdFifo_t * transClos_getFifoLocal(lladdFifoPool_t * pool, byte * multiplexKey, size_t multiplexKeySize) {
  recordid * rid;
  assert(multiplexKeySize == sizeof(rid->page));
  

  long page = ((recordid*)multiplexKey)->page;

  return pool->pool[page % pool->fifoCount];
}


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

extern int transClos_outdegree;
extern int numOut;
extern int numTset;
extern int numShortcutted;
extern int numSkipped;
extern int numPushed;
extern int useCRC;
int OUTDEGREE = 0;

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
  //  lladdFifo_t * dirtyFifo = arg->dirty;
  lladdFifo_t * globalFifo = arg->global;
  recordid rid = arg->rid;
  int i = arg->i;
  int xid = Tbegin();
  
  lladdMultiplexer_flush(arg->mux);
  int count = 0;
  //  while(Titerator_next(xid, dirtyFifo->iterator)) {
  while(1) {
  
    
    
    //    lladdFifo_t ** localFifo_ptr;
    lladdFifo_t * localFifo;
    
    //    int size = Titerator_value(xid, dirtyFifo->iterator, (byte**) &localFifo_ptr);
    
    localFifo = arg->pool->pool[count % arg->pool->fifoCount]; //*localFifo_ptr;
    count++;
    //    assert(size == sizeof(lladdFifo_t*));
    
    multiTraverse(xid, rid, localFifo, globalFifo, arg->pool, i/*+NUM_ITER*/);
    
    
    //    Titerator_tupleDone(xid, dirtyFifo->iterator);

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
 

  Tinit();

  int xid = Tbegin();

  assert(argc == 8);
  int naive  = atoi(argv[1]);
  useCRC     = atoi(argv[2]);
  int ring   = atoi(argv[3]);
  transClos_outdegree = OUTDEGREE  = atoi(argv[4]);
  int hot    = atoi(argv[5]);
  int seed   = atoi(argv[6]);
  int silent = atoi(argv[7]);
  if(!silent) {
    printf("naive=%d crc=%d ring=%d outdegree=%d hot=%d seed=%d\n", naive, useCRC, ring, OUTDEGREE, hot, seed);
    assert(!hot || !ring);
  }

  if(seed == -1) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    srandom(tv.tv_usec);
    if(!silent) {
      printf("Seed: %lld\n", (long long)tv.tv_usec);
    }
  } else {
    srandom(seed);
  }

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

  int * node = stasis_malloc(OUTDEGREE+1, int);  // the last long in the node holds flags.
  int i, j;
  for(i = 0; i < NUM_NODES; i++) {
    node[OUTDEGREE] = 0;
    if(ring) {
      node[0] = (i+1) % NUM_NODES;
      for(j = 1; j < OUTDEGREE; j++) {
	if(hot) {
	  node[j] = hotSet(j, hot);
	} else {
	  node[j] = (int)(  ((double)NUM_NODES) * (double)random()/((double)RAND_MAX+1.0));
	}
      }
      //     printf("%d ", node[j]);
    } else {
      for(j = 0; j < OUTDEGREE; j++) {
	if(hot) {
	  node[j] = hotSet(j, hot);
	} else {
	  node[j] = (int)(  ((double)NUM_NODES) * (double)random()/((double)RAND_MAX+1.0));
	}
      }
    }
    rid.slot = i;
    Tset(xid, rid, node);
  }

  Tcommit(xid);
  if(!silent) {
    printf("Nodes loaded.\n");
    fflush(stdout);
    int err = system("date");
    (void)err;
  }
  rid.slot = 0;

  struct timeval start, stop;

  gettimeofday(&start, NULL);

  numTset = 0;
  xid = Tbegin();
  if(naive) {
    for(i = 1; i <= NUM_ITER; i++) {
      
      naiveTraverse(xid, rid, i);
  
    }
    if(!silent) { 
      int err = system("date");
      (void)err;

      printf("TransClos returned Tset called %d times\n", numTset);
      fflush(stdout);
    }
    numTset = 0;
    numPushed = 0;
  } else {

    for(i = 1; i <= NUM_ITER; i++) {
      //      lladdFifo_t * dirtyFifo = logMemoryFifo(DIRTY_BUF_SIZE, 0);
      lladdFifoPool_t * pool = lladdFifoPool_pointerPoolInit(NUM_FIFOS, DIRTY_BUF_SIZE, 
							     useCRC ? lladdFifoPool_getFifoCRC32 : transClos_getFifoLocal
							     , NULL/*, dirtyFifo*/);
      
      lladdFifo_t * globalFifo = logMemoryFifo(DIRTY_BUF_SIZE, /*NUM_FIFOS*/ 0);
      lladdMultiplexer_t * mux = lladdMultiplexer_alloc(xid, globalFifo->iterator, &multiplexByRidPage, pool);
      
      //    lladdMultiplexer_start(mux, NULL);
      rid.slot =0;
      Tconsumer_push(xid, globalFifo->consumer, NULL, 0, (byte*)&rid, sizeof(recordid));
      numOut = 1;
      
      pthread_t * workers = stasis_malloc(NUM_THREADS, pthread_t);
      int j;
      
      worker_arg * arg = stasis_alloc(worker_arg);
      //      arg->dirty = dirtyFifo;
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
    if(!silent) {
      int err = system("date");
      (void)err;
      printf("FastTransClos returned Tset called %d times, shortcutted %d, skipped %d pushed %d\n", numTset, numShortcutted, numSkipped, numPushed);
    }
  }
  
  Tcommit(xid);

  gettimeofday(&stop, NULL);

  Tdeinit();

  double elapsed = ((double)(stop.tv_sec - start.tv_sec)) + 1e-6 * ((double)(stop.tv_usec - start.tv_usec));
  
  printf("%f\n", elapsed);

}
