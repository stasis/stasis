#include <stasis/transactional.h>
#include <stasis/logger/reorderingHandle.h>
#include <string.h>

long stasis_log_reordering_usleep_after_flush = 0;

static void* stasis_log_reordering_handle_worker(void * a) {
  stasis_log_reordering_handle_t * h = (typeof(h))a;
  pthread_mutex_lock(&h->mut);
  while(h->cur_len || !h->closed) {
    while(h->cur_len) {
      size_t chunk_len = 0;
      while(chunk_len < h->chunk_len && h->cur_len) {
        LogEntry * e = stasis_log_write_update(h->log,
                                 h->l,
                                 h->queue[h->cur_off].p,
                                 h->queue[h->cur_off].op,
                                 h->queue[h->cur_off].arg,
                                 h->queue[h->cur_off].arg_size);
        assert(e->xid != INVALID_XID);
        chunk_len += sizeofLogEntry(h->log, e);

        if(h->queue[h->cur_off].p) {
          Page * p = h->queue[h->cur_off].p;
          writelock(p->rwlatch,0);
          stasis_page_lsn_write(e->xid, p, e->LSN);
          unlock(p->rwlatch);
          releasePage(p);
        } /*
            else it's the caller's problem; flush(), and checking the
            xaction table for prevLSN is their friend. */

        h->cur_len--;
        h->phys_size -= sizeofLogEntry(h->log, e);
        h->cur_off = (h->cur_off+1)%h->max_len;

      }
      if(chunk_len > 0) {
        lsn_t to_force = h->l->prevLSN;
        pthread_mutex_unlock(&h->mut);
        stasis_log_force(h->log, to_force, LOG_FORCE_COMMIT);
        if(stasis_log_reordering_usleep_after_flush) {
          usleep(stasis_log_reordering_usleep_after_flush);
        }
        pthread_mutex_lock(&h->mut);
      }
    }
    pthread_cond_signal(&h->done);
    if(!h->closed) { // XXX hack!
      pthread_cond_wait(&h->ready, &h->mut);
    }
  }
  pthread_mutex_unlock(&h->mut);
  return 0;
}

void stasis_log_reordering_handle_flush(stasis_log_reordering_handle_t * h) {
  pthread_mutex_lock(&h->mut);
  while(h->cur_len > 0) {
    pthread_cond_wait(&h->done, &h->mut);
  }
  pthread_mutex_unlock(&h->mut);
}
void stasis_log_reordering_handle_close(stasis_log_reordering_handle_t * h) {
  h->closed = 1;
  pthread_cond_signal(&h->ready);
  pthread_join(h->worker,0);
  assert(h->cur_len == 0);
  pthread_mutex_destroy(&h->mut);
  pthread_cond_destroy(&h->ready);
  pthread_cond_destroy(&h->done);
  free(h->queue);
  free(h);
}
stasis_log_reordering_handle_t *
stasis_log_reordering_handle_open(TransactionLog * l,
                                  stasis_log_t* log,
                                  size_t chunk_len,
                                  size_t max_len,
                                  size_t max_size) {
  stasis_log_reordering_handle_t * ret = malloc(sizeof(*ret));

  ret->l = l;
  ret->log = log;
  pthread_mutex_init(&ret->mut,0);
  pthread_cond_init(&ret->done,0);
  pthread_cond_init(&ret->ready,0);
  ret->closed = 0;
  ret->queue = malloc(sizeof(stasis_log_reordering_op_t)*max_len);
  ret->chunk_len = chunk_len;
  ret->max_len = max_len;
  ret->cur_off = 0;
  ret->cur_len = 0;
  ret->phys_size = 0;
  ret->max_size = max_size;
  pthread_create(&ret->worker,0,stasis_log_reordering_handle_worker,ret);
  return ret;
}
static int AskedForBump = 0;
size_t stasis_log_reordering_handle_append(stasis_log_reordering_handle_t * h,
                                         Page * p,
                                         unsigned int op,
                                         const byte * arg,
                                         size_t arg_size,
                                         size_t phys_size
                                         ) {
  while(h->phys_size >= h->max_size) {
    pthread_cond_wait(&h->done, &h->mut);
  }

  while(h->cur_len == h->max_len) {
    if(!AskedForBump) {
      printf("Warning: bump max_len\n"); fflush(stdout);
      AskedForBump = 1;
    }
    pthread_cond_wait(&h->done, &h->mut);
  }


  intptr_t idx = (h->cur_off+h->cur_len)%h->max_len;
  h->queue[idx].p = p;
  h->queue[idx].op = op;
  h->queue[idx].arg = malloc(arg_size);
  memcpy(h->queue[idx].arg,arg,arg_size);
  h->queue[idx].arg_size = arg_size;
  h->cur_len++;
  h->phys_size += phys_size;
  pthread_cond_signal(&h->ready);
  return h->phys_size;
}
