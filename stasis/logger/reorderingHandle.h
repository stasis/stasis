#ifndef __STASIS_LOG_REORDERING_HANDLE_H
#define __STASIS_LOG_REORDERING_HANDLE_H
#include <stasis/common.h>
#include <stasis/logger/logger2.h>

typedef struct {
  Page * p;
  unsigned int op;
  byte * arg;
  size_t arg_size;
} stasis_log_reordering_op_t;

typedef struct stasis_log_reordering_handle_t {
  TransactionLog *l;
  stasis_log_t * log;
  pthread_mutex_t mut;
  pthread_cond_t done;
  pthread_cond_t ready;
  int closed;
  pthread_t worker;
  stasis_log_reordering_op_t * queue;
  size_t chunk_len;
  size_t max_len;
  size_t cur_off;
  size_t cur_len;
  size_t max_size;
  size_t phys_size;
} stasis_log_reordering_handle_t;

#include <stasis/page.h>

void stasis_log_reordering_handle_flush(stasis_log_reordering_handle_t * h);
void stasis_log_reordering_handle_close(stasis_log_reordering_handle_t * h);
stasis_log_reordering_handle_t *
stasis_log_reordering_handle_open(TransactionLog * l,
                                  stasis_log_t* log,
                                  size_t chunk_len,
                                  size_t max_len,
                                  size_t max_size);
size_t stasis_log_reordering_handle_append(stasis_log_reordering_handle_t * h,
                                         Page * p,
                                         unsigned int op,
                                         const byte * arg,
                                         size_t arg_size,
                                         size_t phys_size
                                         );

#endif //__STASIS_LOG_REORDERING_HANDLE_H
