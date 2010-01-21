/*
 * groupForce.c
 *
 *  Created on: May 12, 2009
 *      Author: sears
 */
#include <stasis/logger/logger2.h>
#include <stasis/transactional.h>
#include <stdio.h>
#include <assert.h>
#include <sys/time.h>

struct stasis_log_group_force_t {
    stasis_log_t * log;
    pthread_mutex_t check_commit;
    pthread_cond_t tooFewXacts;
    int pendingCommits;
    int minNumActive;
    uint64_t wait_nsec;
};

stasis_log_group_force_t * stasis_log_group_force_init(stasis_log_t * log, uint64_t wait_nsec) {
  static int warned = 0;
  if(wait_nsec > (1000 * 1000 * 1000)) {
    warned  = 1;
    fprintf(stderr, "TODO stasis_log_group_force: Efficiently support wait "
                    "times > 1 second.  (%llu second wait time requested)\n",
                    (long long unsigned int) (wait_nsec / (1000 * 1000 * 1000)));
  }
  stasis_log_group_force_t * ret = malloc(sizeof(*ret));
  ret->log = log;
  pthread_mutex_init(&ret->check_commit,0);
  pthread_cond_init(&ret->tooFewXacts,0);
  ret->pendingCommits = 0;
  ret->wait_nsec = wait_nsec;
  return ret;
}

static int stasis_log_group_force_should_wait(int xactcount, int pendingCommits) {
  if((xactcount > 1 && pendingCommits < xactcount) ||
     (xactcount > 20 && pendingCommits < (int)((double)xactcount * 0.95))) {
    return 1;
  } else {
    return 0;
  }
}

void stasis_log_group_force(stasis_log_group_force_t* lh, lsn_t lsn) {
//  static pthread_mutex_t check_commit = PTHREAD_MUTEX_INITIALIZER;
//  static pthread_cond_t tooFewXacts = PTHREAD_COND_INITIALIZER;
//  static int pendingCommits;

  pthread_mutex_lock(&lh->check_commit);
  if(lsn == INVALID_LSN) {
    lsn = lh->log->first_unstable_lsn(lh->log,LOG_FORCE_COMMIT);
  } else if(lh->log->first_unstable_lsn(lh->log,LOG_FORCE_COMMIT) > lsn) {
    pthread_mutex_unlock(&lh->check_commit);
    return;
  }

  if(lh->log->is_durable(lh->log)) {
    struct timeval now;
    struct timespec timeout;

    gettimeofday(&now, NULL);
    timeout.tv_sec = now.tv_sec;
    timeout.tv_nsec = now.tv_usec * 1000;
    //                   0123456789  <- number of zeros on the next three lines...
//    timeout.tv_nsec +=   10000000; // wait ten msec.
    timeout.tv_nsec += lh->wait_nsec;
    while(timeout.tv_nsec > (1000 * 1000 * 1000)) {
      timeout.tv_nsec -= (1000 * 1000 * 1000);
      timeout.tv_sec++;
    }

    lh->pendingCommits++;
    int xactcount = TactiveThreadCount();
    if(stasis_log_group_force_should_wait(xactcount, lh->pendingCommits)) {
      int retcode;
      while(ETIMEDOUT != (retcode = pthread_cond_timedwait(&lh->tooFewXacts, &lh->check_commit, &timeout))) {
        if(retcode != 0) {
          printf("Warning: %s:%d: pthread_cond_timedwait was interrupted by "
                 "a signal in groupCommit().  Acting as though it timed out.\n",
                 __FILE__, __LINE__);
          break;
        }
        if(lh->log->first_unstable_lsn(lh->log,LOG_FORCE_COMMIT) > lsn) {
          (lh->pendingCommits)--;
          pthread_mutex_unlock(&lh->check_commit);
          return;
        }
      }
    }
  } else {
    (lh->pendingCommits)++;
  }
  if(lh->log->first_unstable_lsn(lh->log,LOG_FORCE_COMMIT) <= lsn) {
    lh->log->force_tail(lh->log, LOG_FORCE_COMMIT);
    lh->minNumActive = 0;
    pthread_cond_broadcast(&lh->tooFewXacts);
  }
  assert(lh->log->first_unstable_lsn(lh->log,LOG_FORCE_COMMIT) > lsn);
  (lh->pendingCommits)--;
  pthread_mutex_unlock(&lh->check_commit);
  return;
}

void stasis_log_group_force_deinit(stasis_log_group_force_t * lh) {
  pthread_mutex_destroy(&lh->check_commit);
  pthread_cond_destroy(&lh->tooFewXacts);
  free(lh);
}

