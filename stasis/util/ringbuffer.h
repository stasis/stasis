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

stasis_ringbuffer_t * stasis_ringbuffer_init(intptr_t size, lsn_t initial_offset);
lsn_t stasis_ringbuffer_nb_reserve_space(stasis_ringbuffer_t * ring, lsn_t sz);
lsn_t stasis_ringbuffer_reserve_space(stasis_ringbuffer_t * ring, lsn_t sz, lsn_t * handle);
void stasis_ringbuffer_read_done(stasis_ringbuffer_t * ring, lsn_t * handle);
void   stasis_ringbuffer_advance_write_tail(stasis_ringbuffer_t * ring, lsn_t off);
void stasis_ringbuffer_reading_writer_done(stasis_ringbuffer_t * ring, lsn_t * handle);
const void * stasis_ringbuffer_nb_get_rd_buf(stasis_ringbuffer_t * ring, lsn_t off, lsn_t sz);
// sz is a pointer to the desired size, or RING_NEXT for "as many bytes as possible"
lsn_t stasis_ringbuffer_consume_bytes(stasis_ringbuffer_t * ring, lsn_t* sz, lsn_t * handle);
void stasis_ringbuffer_write_done(stasis_ringbuffer_t * ring, lsn_t * handle);
// sz is a pointer to the desired size, or RING_NEXT for "as many bytes as possible"
const void * stasis_ringbuffer_get_rd_buf(stasis_ringbuffer_t * ring, lsn_t off, lsn_t sz);
void * stasis_ringbuffer_get_wr_buf(stasis_ringbuffer_t * ring, lsn_t off, lsn_t sz);
lsn_t stasis_ringbuffer_get_read_tail(stasis_ringbuffer_t * ring);
lsn_t stasis_ringbuffer_get_write_tail(stasis_ringbuffer_t * ring);
lsn_t stasis_ringbuffer_get_write_frontier(stasis_ringbuffer_t * ring);
void   stasis_ringbuffer_advance_read_tail(stasis_ringbuffer_t * ring, lsn_t off);
typedef enum { RING_TORN = -1, RING_VOLATILE = -2, RING_FULL = -3, RING_TRUNCATED = -4, RING_NEXT = -5, RING_CLOSED = -6, RING_MINERR = -7 } stasis_ringbuffer_error_t;
void stasis_ringbuffer_flush(stasis_ringbuffer_t * ring, lsn_t off);
/*
 * Like flush, but if off could still be modified, immediately returns RING_VOLATILE instead of flushing the ringbuffer.
 */
int stasis_ringbuffer_tryflush(stasis_ringbuffer_t * ring, lsn_t off);

// Causes ringbuffer requests to stop blocking, and return RING_CLOSED
void stasis_ringbuffer_shutdown(stasis_ringbuffer_t * ring);
// Deallocates the ringbuffer (call after any threads using the ringbuffer have shutdown).
void stasis_ringbuffer_free(stasis_ringbuffer_t * ring);
END_C_DECLS
#endif /* RINGBUFFER_H_ */
