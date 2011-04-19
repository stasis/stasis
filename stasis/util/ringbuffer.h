/*
 * ringbuffer.h
 *
 *  Created on: Apr 1, 2011
 *      Author: sears
 */

#ifndef RINGBUFFER_H_
#define RINGBUFFER_H_
#include <stasis/common.h>
BEGIN_C_DECLS

typedef struct stasis_ringbuffer_t stasis_ringbuffer_t;

stasis_ringbuffer_t * stasis_ringbuffer_init(intptr_t size, int64_t initial_offset);
int64_t stasis_ringbuffer_nb_reserve_space(stasis_ringbuffer_t * ring, int64_t sz);
int64_t stasis_ringbuffer_reserve_space(stasis_ringbuffer_t * ring, int64_t sz, int64_t * handle);
void stasis_ringbuffer_read_done(stasis_ringbuffer_t * ring, int64_t * handle);
void   stasis_ringbuffer_advance_write_tail(stasis_ringbuffer_t * ring, int64_t off);
int64_t stasis_ringbuffer_current_write_tail(stasis_ringbuffer_t * ring);
const void * stasis_ringbuffer_nb_get_rd_buf(stasis_ringbuffer_t * ring, int64_t off, int64_t sz);
// sz is a pointer to the desired size, or RING_NEXT for "as many bytes as possible"
int64_t stasis_ringbuffer_consume_bytes(stasis_ringbuffer_t * ring, int64_t* sz, int64_t * handle);
void stasis_ringbuffer_write_done(stasis_ringbuffer_t * ring, int64_t * handle);
// sz is a pointer to the desired size, or RING_NEXT for "as many bytes as possible"
const void * stasis_ringbuffer_get_rd_buf(stasis_ringbuffer_t * ring, int64_t off, int64_t sz);
void * stasis_ringbuffer_get_wr_buf(stasis_ringbuffer_t * ring, int64_t off, int64_t sz);
int64_t stasis_ringbuffer_get_read_tail(stasis_ringbuffer_t * ring);
int64_t stasis_ringbuffer_get_write_tail(stasis_ringbuffer_t * ring);
int64_t stasis_ringbuffer_get_write_frontier(stasis_ringbuffer_t * ring);
void   stasis_ringbuffer_advance_read_tail(stasis_ringbuffer_t * ring, int64_t off);
typedef enum { RING_TORN = -1, RING_VOLATILE = -2, RING_FULL = -3, RING_TRUNCATED = -4, RING_NEXT = -5, RING_CLOSED = -6, RING_MINERR = -7 } stasis_ringbuffer_error_t;
void stasis_ringbuffer_flush(stasis_ringbuffer_t * ring, int64_t off);
void stasis_ringbuffer_shutdown(stasis_ringbuffer_t * ring);

END_C_DECLS
#endif /* RINGBUFFER_H_ */
