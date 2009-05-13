/*
 * groupForce.h
 *
 *  Created on: May 12, 2009
 *      Author: sears
 */

#ifndef GROUPFORCE_H_
#define GROUPFORCE_H_

#include <stasis/common.h>
#include <stasis/logger/logger2.h>

stasis_log_group_force_t * stasis_log_group_force_init(stasis_log_t * log, uint64_t wait_nsec);
void stasis_log_group_force_deinit(stasis_log_group_force_t * lh);
void stasis_log_group_force(stasis_log_group_force_t* lh, lsn_t lsn);

#endif /* GROUPFORCE_H_ */
