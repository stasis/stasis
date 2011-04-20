#ifndef STASIS_UTIL_MIN_H
#define STASIS_UTIL_MIN_H
#include <stasis/common.h>
BEGIN_C_DECLS

typedef struct stasis_aggregate_min_t stasis_aggregate_min_t;

stasis_aggregate_min_t * stasis_aggregate_min_init(int large);
void stasis_aggregate_min_deinit(stasis_aggregate_min_t * min);
void stasis_aggregate_min_add(stasis_aggregate_min_t * min, lsn_t* a);
const lsn_t * stasis_aggregate_min_remove(stasis_aggregate_min_t * min, lsn_t * b);
const lsn_t * stasis_aggregate_min_compute(stasis_aggregate_min_t * min);

END_C_DECLS
#endif
