/*
 * raid0.c
 *
 *  Created on: Feb 24, 2012
 *      Author: sears
 */
#include <config.h>
#include <stasis/flags.h>
#include <sys/time.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <stasis/io/handle.h>
#include <stasis/util/histogram.h>

#include <assert.h>

#ifdef RAID0_LATENCY_PROF
DECLARE_HISTOGRAM(read_hist)
DECLARE_HISTOGRAM(write_hist)
DECLARE_HISTOGRAM(force_hist)
DECLARE_HISTOGRAM(force_range_hist)
#define TICK(hist) stasis_histogram_tick(&hist);
#define TOCK(hist) stasis_histogram_tock(&hist);
#else
#define TICK(hist)
#define TOCK(hist)
#endif

typedef struct raid0_impl {
  stasis_handle_t ** h;
  int handle_count;
  int stripe_size;
} raid0_impl;

static int raid0_num_copies(stasis_handle_t *h) {
  raid0_impl * i = h->impl;
  return i->h[0]->num_copies(i->h[0]);
}
static int raid0_num_copies_buffer(stasis_handle_t *h) {
  raid0_impl * i = h->impl;
  return i->h[0]->num_copies_buffer(i->h[0]);
}
static int raid0_close(stasis_handle_t *h) {
  raid0_impl * r = h->impl;
  int ret = 0;
  for(int i = 0; i < r->handle_count; i++) {
    int this_ret = r->h[i]->close(r->h[i]);
    if(this_ret && !ret) ret = this_ret;
  }
  free(r->h);
  free(r);
  free(h);
  return ret;
}
static stasis_handle_t* raid0_dup(stasis_handle_t *h) {
  raid0_impl * r = h->impl;
  stasis_handle_t ** h_dup = malloc(sizeof(h_dup[0]) * r->handle_count);
  for(int i = 0; i < r->handle_count; i++) {
    h_dup[i] = r->h[i]->dup(r->h[i]);
  }
  stasis_handle_t * ret = stasis_handle_open_raid0(r->handle_count, h_dup, r->stripe_size);
  free(h_dup);
  return ret;
}
static void raid0_enable_sequential_optimizations(stasis_handle_t *h) {
  raid0_impl * r = h->impl;
  for(int i = 0; i < r->handle_count; i++) {
    r->h[i]->enable_sequential_optimizations(r->h[i]);
  }
}
static lsn_t raid0_end_position(stasis_handle_t *h) {
  raid0_impl *r = h->impl;
  lsn_t max_end = 0;
  for(int i = 0; i < r->handle_count; i++) {
    lsn_t this_end = r->h[i]->end_position(r->h[i]) + (i * r->stripe_size);
    if(this_end > max_end) max_end = this_end;
  }
  return max_end;
}
/**
 * Figure out which stripe this operation belongs on.  We don't support
 * inter-stripe operations, so returning a single stripe suffices.
 *
 * @param off The first byte to be accessed.
 * @param len If the access will span stripes, this method will call abort().
 */
static int raid0_calc_stripe(raid0_impl * r, lsn_t off, lsn_t len) {
  assert(len < r->stripe_size);
  int start_stripe = (off % (r->handle_count * r->stripe_size)) / r->stripe_size;
  int end_stripe = ((off+len-1) % (r->handle_count * r->stripe_size)) / r->stripe_size;
  assert(start_stripe == end_stripe);
  return start_stripe;
}
static lsn_t raid0_calc_block(raid0_impl * r, lsn_t off, lsn_t len) {
  return off / (r->stripe_size * r->handle_count);
}
static lsn_t raid0_calc_off(raid0_impl * r, lsn_t off, lsn_t len) {
  lsn_t block = raid0_calc_block(r, off, len);
  return block * r->stripe_size + (off % r->stripe_size);
}
static int raid0_read(stasis_handle_t *h, lsn_t off, byte *buf, lsn_t len) {
  raid0_impl *r = h->impl;
  int stripe = raid0_calc_stripe(r, off, len);
  lsn_t stripe_off = raid0_calc_off(r, off, len);
  return r->h[stripe]->read(r->h[stripe], stripe_off, buf, len);
}
static int raid0_write(stasis_handle_t *h, lsn_t off, const byte *dat, lsn_t len) {
  raid0_impl *r = h->impl;
  int stripe = raid0_calc_stripe(r, off, len);
  lsn_t stripe_off = raid0_calc_off(r, off, len);
  return r->h[stripe]->write(r->h[stripe], stripe_off, dat, len);
}
static stasis_write_buffer_t * raid0_write_buffer(stasis_handle_t *h, lsn_t off, lsn_t len) {
  raid0_impl *r = h->impl;
  int stripe = raid0_calc_stripe(r, off, len);
  lsn_t stripe_off = raid0_calc_off(r, off, len);
  return r->h[stripe]->write_buffer(r->h[stripe], stripe_off, len);
}
static int raid0_release_write_buffer(stasis_write_buffer_t *w) {
  return w->h->release_write_buffer(w);
}
static stasis_read_buffer_t *raid0_read_buffer(stasis_handle_t *h,
                           lsn_t off, lsn_t len) {
  raid0_impl *r = h->impl;
  int stripe = raid0_calc_stripe(r, off, len);
  lsn_t stripe_off = raid0_calc_off(r, off, len);
  return r->h[stripe]->read_buffer(r->h[stripe], stripe_off, len);
}
static int raid0_release_read_buffer(stasis_read_buffer_t *r) {
  return r->h->release_read_buffer(r);
}
static int raid0_force(stasis_handle_t *h) {
  raid0_impl * r = h->impl;
  int ret = 0;
  for(int i = 0; i < r->handle_count; i++) {
    int this_ret = r->h[i]->force(r->h[i]);
    if(this_ret && !ret) ret = this_ret;
  }
  return ret;
}
/**
 * TODO Implement raid0_force_range properly instead of forcing whole file.
 */
static int raid0_force_range(stasis_handle_t *h, lsn_t start, lsn_t stop) {
  return raid0_force(h);
}
static int raid0_async_force(stasis_handle_t *h) {
  raid0_impl * r = h->impl;
  int ret = 0;
  for(int i = 0; i < r->handle_count; i++) {
    int this_ret = r->h[i]->async_force(r->h[i]);
    if(this_ret && !ret) ret = this_ret;
  }
  return ret;
}
static int raid0_fallocate(stasis_handle_t *h, lsn_t off, lsn_t len) {
  raid0_impl * r = h->impl;
  int ret = 0;
  lsn_t start_block = raid0_calc_block(r, off, 0);
  lsn_t start_off = (start_block) * r->stripe_size;
  lsn_t end_block = raid0_calc_block(r, off+len-1, 0);
  lsn_t end_off = (end_block+1) * r->stripe_size;

  for(int i = 0; i < r->handle_count; i++) {
    int this_ret = r->h[i]->fallocate(r->h[i], start_off, end_off-start_off);
    if(this_ret && !ret) ret = this_ret;
  }
  return ret;
}
struct stasis_handle_t raid0_func = {
  .num_copies = raid0_num_copies,
  .num_copies_buffer = raid0_num_copies_buffer,
  .close = raid0_close,
  .dup = raid0_dup,
  .enable_sequential_optimizations = raid0_enable_sequential_optimizations,
  .end_position = raid0_end_position,
  .write = raid0_write,
  .write_buffer = raid0_write_buffer,
  .release_write_buffer = raid0_release_write_buffer,
  .read = raid0_read,
  .read_buffer = raid0_read_buffer,
  .release_read_buffer = raid0_release_read_buffer,
  .force = raid0_force,
  .async_force = raid0_async_force,
  .force_range = raid0_force_range,
  .fallocate = raid0_fallocate,
  .error = 0
};

stasis_handle_t * stasis_handle_open_raid0(int handle_count, stasis_handle_t** h, uint32_t stripe_size) {
  stasis_handle_t * ret = malloc(sizeof(*ret));
  *ret = raid0_func;
  raid0_impl * r = malloc(sizeof(*r));
  r->stripe_size = stripe_size;
  r->handle_count = handle_count;
  r->h = malloc(sizeof(r->h[0]) * handle_count);
  for(int i = 0; i < handle_count; i++) {
    r->h[i] = h[i];
  }
  ret->impl = r;
  return ret;
}

stasis_handle_t * stasis_handle_raid0_factory() {
  if(stasis_handle_raid0_filenames == NULL) {
    stasis_handle_t * h[2];
    h[0] = stasis_handle_file_factory(stasis_store_file_1_name, O_CREAT | O_RDWR | stasis_buffer_manager_io_handle_flags, FILE_PERM);
    h[1] = stasis_handle_file_factory(stasis_store_file_2_name, O_CREAT | O_RDWR | stasis_buffer_manager_io_handle_flags, FILE_PERM);
    return stasis_handle_open_raid0(2, h, stasis_handle_raid0_stripe_size);
  } else {
    int count = 0;
    while(stasis_handle_raid0_filenames[count]) count++;
    stasis_handle_t * h[count];
    for(int i = 0; i < count; i++) {
      h[i] = stasis_handle_file_factory(stasis_handle_raid0_filenames[i], O_CREAT | O_RDWR | stasis_buffer_manager_io_handle_flags, FILE_PERM);
    }
    return stasis_handle_open_raid0(count, h, stasis_handle_raid0_stripe_size);
  }
}
