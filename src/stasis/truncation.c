#include <stasis/transactional.h>
#include <stasis/truncation.h>
#include <stasis/bufferManager.h>
#include <stdio.h>
#include <assert.h>

struct stasis_truncation_t {
  char initialized;
  char automaticallyTruncating;
  pthread_t truncationThread;
  pthread_mutex_t shutdown_mutex;
  pthread_cond_t shutdown_cond;
  stasis_dirty_page_table_t * dirty_pages;
  stasis_transaction_table_t * transaction_table;
  stasis_buffer_manager_t * buffer_manager;
  stasis_log_t * log;
};

#ifdef LONG_TEST
#define TARGET_LOG_SIZE (1024 * 1024 * 5)
#define TRUNCATE_INTERVAL 1
#define MIN_INCREMENTAL_TRUNCATION (1024 * 1024 * 1)
#else
#define TARGET_LOG_SIZE (1024 * 1024 * 50)
#define TRUNCATE_INTERVAL 1
#define MIN_INCREMENTAL_TRUNCATION (1024 * 1024 * 25)
#endif
stasis_truncation_t * stasis_truncation_init(stasis_dirty_page_table_t * dpt, stasis_transaction_table_t * tbl,
                                             stasis_buffer_manager_t *buffer_manager, stasis_log_t *log) {
  stasis_truncation_t * ret = stasis_alloc(stasis_truncation_t);
  ret->initialized = 1;
  ret->automaticallyTruncating = 0;
  pthread_mutex_init(&ret->shutdown_mutex, 0);
  pthread_cond_init(&ret->shutdown_cond, 0);
  ret->dirty_pages = dpt;
  ret->transaction_table = tbl;
  ret->buffer_manager = buffer_manager;
  ret->log = log;
  return ret;
}

void stasis_truncation_deinit(stasis_truncation_t * trunc) {
  pthread_mutex_lock(&trunc->shutdown_mutex);
  trunc->initialized = 0;
  if(trunc->automaticallyTruncating) {
    void * ret = 0;
    pthread_mutex_unlock(&trunc->shutdown_mutex);
    pthread_cond_broadcast(&trunc->shutdown_cond);
    pthread_join(trunc->truncationThread, &ret);
  } else {
    pthread_mutex_unlock(&trunc->shutdown_mutex);
  }
  trunc->automaticallyTruncating = 0;
  pthread_mutex_destroy(&trunc->shutdown_mutex);
  pthread_cond_destroy(&trunc->shutdown_cond);
  free(trunc);
}

static void* stasis_truncation_thread_worker(void* truncp) {
  stasis_truncation_t * trunc = (stasis_truncation_t*)truncp;
  pthread_mutex_lock(&trunc->shutdown_mutex);
  while(trunc->initialized) {
    if(trunc->log->first_unstable_lsn(trunc->log, LOG_FORCE_WAL) - trunc->log->truncation_point(trunc->log)
       > TARGET_LOG_SIZE) {
      stasis_truncation_truncate(trunc, 0);
    }
    struct timeval now;
    struct timespec timeout;
    int timeret = gettimeofday(&now, 0);
    assert(0 == timeret);

    timeout.tv_sec = now.tv_sec;
    timeout.tv_nsec = now.tv_usec;
    timeout.tv_sec += TRUNCATE_INTERVAL;

    pthread_cond_timedwait(&trunc->shutdown_cond, &trunc->shutdown_mutex, &timeout);
  }
  pthread_mutex_unlock(&trunc->shutdown_mutex);
  return (void*)0;
}

void stasis_truncation_thread_start(stasis_truncation_t* trunc) {
  assert(!trunc->automaticallyTruncating);
  trunc->automaticallyTruncating = 1;
  pthread_create(&trunc->truncationThread, 0, &stasis_truncation_thread_worker, trunc);
}


int stasis_truncation_truncate(stasis_truncation_t* trunc, int force) {

  // *_minRecLSN() used to return the same value as flushed if
  //there were no outstanding transactions, but flushed might
  //not point to the front of a log entry...  now, both return
  //LSN_T_MAX if there are no outstanding transactions / no
  //dirty pages.

  // applied_lsn has to be before dirty_page_table_minRecLSN(), since
  // minRecLSN can miss anything above this value, and this value
  // increases.

  lsn_t applied_lsn  = trunc->log->first_pending_lsn(trunc->log);
  lsn_t page_rec_lsn = stasis_dirty_page_table_minRecLSN(trunc->dirty_pages);
  lsn_t xact_rec_lsn = stasis_transaction_table_minRecLSN(trunc->transaction_table);
  lsn_t flushed_lsn  = trunc->log->first_unstable_lsn(trunc->log, LOG_FORCE_WAL);

  //  printf("pending %lld page_rec %lld xact_rec %lld flushed %lld\n",
  //         applied_lsn, page_rec_lsn, xact_rec_lsn, flushed_lsn);

  lsn_t rec_lsn = page_rec_lsn < xact_rec_lsn ? page_rec_lsn : xact_rec_lsn;
  rec_lsn = (rec_lsn < flushed_lsn) ? rec_lsn : flushed_lsn;
  rec_lsn = (rec_lsn < applied_lsn) ? rec_lsn : applied_lsn;

  lsn_t log_trunc = trunc->log->truncation_point(trunc->log);
  if(force || (xact_rec_lsn - log_trunc) > MIN_INCREMENTAL_TRUNCATION) {
    //fprintf(stderr, "xact = %ld \t log = %ld\n", xact_rec_lsn, log_trunc);
    if((rec_lsn - log_trunc) > MIN_INCREMENTAL_TRUNCATION) {
      //      fprintf(stderr, "Truncating now. rec_lsn = %ld, log_trunc = %ld\n", rec_lsn, log_trunc);
      //      fprintf(stderr, "Truncating to rec_lsn = %ld\n", rec_lsn);
      trunc->buffer_manager->forcePages(trunc->buffer_manager, 0);
      trunc->log->truncate(trunc->log, rec_lsn);
      return 1;
    } else {
      lsn_t flushed = trunc->log->first_unstable_lsn(trunc->log, LOG_FORCE_WAL);
      if(force || flushed - log_trunc > 2 * TARGET_LOG_SIZE) {
        DEBUG("Flushing dirty buffers: rec_lsn = %lld log_trunc = %lld flushed = %lld\n", rec_lsn, log_trunc, flushed);
        applied_lsn  = trunc->log->first_pending_lsn(trunc->log);
        if(EAGAIN == stasis_dirty_page_table_flush(trunc->dirty_pages)) {
          applied_lsn  = trunc->log->first_pending_lsn(trunc->log);
          stasis_dirty_page_table_flush(trunc->dirty_pages); // can ignore ret val, since some other thread successfully initiated + completed a flush since our first call.
        }

        page_rec_lsn = stasis_dirty_page_table_minRecLSN(trunc->dirty_pages);
        rec_lsn = page_rec_lsn;
        rec_lsn = (rec_lsn < xact_rec_lsn) ? rec_lsn : xact_rec_lsn;
        rec_lsn = (rec_lsn < flushed_lsn ) ? rec_lsn : flushed_lsn;
        rec_lsn = (rec_lsn < applied_lsn ) ? rec_lsn : applied_lsn;

        //fprintf(stderr, "Flushed Dirty Buffers.  Truncating to rec_lsn = %ld\n", rec_lsn);

        trunc->buffer_manager->forcePages(trunc->buffer_manager, 0);
        trunc->log->truncate(trunc->log, rec_lsn);
        return 1;
      } else {
        return 0;
      }
    }
  } else {
    return 0;
  }
}
