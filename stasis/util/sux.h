/*
 * sux.h
 *
 *  Created on: Jul 19, 2011
 *      Author: sears
 */
/**
 * @file SUX: Shared, Update, Exclusive latch implementation.
 *
 * Latch protocol used by some concurrent data structures.
 *
 * Lock compatibility table:
 *
 *     S U X
 *   +------
 * S | Y Y N
 * U | Y N N
 * X | N N N
 *
 * This file implements SUX locks using a pthread mutex and a pthread rwlock.
 *
 * Lock mode  |  Holds rwlock?  | Holds mutex?
 *  Shared    |    Read         |  No
 *  Update    |    No           |  Yes
 * eXclusive  |    Write        |  Yes
 *
 * The mutex is always acquired before the rwlock, which allows us to safely
 * upgrade and downgrade the SUX latch between U and X.
 */

#ifndef SUX_H_
#define SUX_H_
#include <stasis/common.h>
BEGIN_C_DECLS

typedef struct {
  pthread_rwlock_t sx_rwl;
  pthread_mutex_t  ux_mut;
} sux_latch;

void sux_latch_init(sux_latch *sux) {
  pthread_rwlock_init(&sux->sx_rwl);
  pthread_mutex_init(&sux->ux_mut);
}
void sux_latch_destroy(sux_latch *sux) {
  pthread_rwlock_destroy(&sux->sx_rwl);
  pthread_mutex_destroy(&sux->ux_mut);
}
void sux_latch_slock(sux_latch *sux) {
  pthread_rwlock_rdlock(&sux->sx_rwl);
}
void sux_latch_sunlock(sux_latch *sux) {
  pthread_rwlock_unlock(&sux->sx_rwl);
}
void sux_latch_ulock(sux_latch *sux) {
  pthread_mutex_lock(&sux->ux_mut);
}
void sux_latch_uunlock(sux_latch *sux) {
  pthread_mutex_unlock(&sux->ux_mut);
}
void sux_latch_upgrade(sux_latch *sux) {
  pthread_rwlock_wrlock(&sux->sx_rwl);
}
void sux_latch_downgrade(sux_latch *sux) {
  pthread_rwlock_unlock(&sux->sx_rwl);
}
void sux_latch_xlock(sux_latch *sux) {
  sux_latch_ulock(sux);
  sux_latch_upgrade(sux);
}
void sux_latch_xunlock(sux_latch *sux) {
  sux_latch_downgrade(sux);
  sux_latch_uunlock(sux);
}

END_C_DECLS

#endif /* SUX_H_ */
