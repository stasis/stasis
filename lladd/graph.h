
#include <lladd/transactional.h>
#include <lladd/fifo.h>

#ifndef __LLADD_GRAPH_H
#define __LLADD_GRAPH_H

typedef struct {
  int   outPage;  ///<= number of links that leave this page.
  short inPage;   ///<= number of links that stay within this page
		  ///  (represented as an array of shorts after the
		  ///   array of links leaving the page.)
  short flags;    ///<= information about this node.  (algorithm specific...)
} nodeHeader_t;


void naiveTraverse(int xid, recordid rid);
void multiTraverse(int xid, int page, lladdFifo_t * local, lladdFifo_t * global);
void pushRequest(lladdConsumer_t * cons);


#endif 
