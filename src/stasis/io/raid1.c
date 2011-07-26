#include <config.h>
#include <stasis/flags.h>
#include <sys/time.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <stasis/io/handle.h>
#include <stasis/util/histogram.h>

#include <assert.h>

#ifdef RAID_LATENCY_PROF
DECLARE_HISTOGRAM(read_hist)
DECLARE_HISTOGRAM(write_hist)
DECLARE_HISTOGRAM(force_hist)
DECLARE_HISTOGRAM(force_range_hist)
#define TICK(hist) stasis_histogram_tick(&hist);
#define TOCK(hist) stasis_histogram_tock(&hist);
#else
#define TICK(hist)
#define TOCK(hist)

typedef struct raid1_impl {
  stasis_handle_t * a;
  stasis_handle_t * b;
} raid1_impl;

static int raid1_num_copies(stasis_handle_t *h) {
  raid1_impl * i = h->impl;
  return i->a->num_copies(i->a);
}
static int raid1_num_copies_buffer(stasis_handle_t *h) {
  raid1_impl * i = h->impl;
  return i->a->num_copies_buffer(i->a);
}
static int raid1_close(stasis_handle_t *h) {
  raid1_impl * i = h->impl;
  int reta = i->a->close(i->a);
  int retb = i->b->close(i->b);
  free(i);
  free(h);
  return reta ? reta : retb;
}
static stasis_handle_t* raid1_dup(stasis_handle_t *h) {
  raid1_impl * i = h->impl;
  return stasis_handle_open_raid1(i->a->dup(i->a), i->b->dup(i->b));
}
static void raid1_enable_sequential_optimizations(stasis_handle_t *h) {
  raid1_impl * i = h->impl;
  i->a->enable_sequential_optimizations(i->a);
  i->b->enable_sequential_optimizations(i->b);
}
static lsn_t raid1_end_position(stasis_handle_t *h) {
  raid1_impl *i = h->impl;
  return i->a->end_position(i->a);
}
static int raid1_read(stasis_handle_t *h, lsn_t off, byte *buf, lsn_t len) {
  raid1_impl *i = h->impl;
  struct timeval tv;
  gettimeofday(&tv, 0);
  // use some low bit that's likely "real" as a source of randomness
  if(tv.tv_usec & 0x040) {
    return i->a->read(i->a, off, buf, len);
  } else {
    return i->b->read(i->b, off, buf, len);
  }
}
static int raid1_write(stasis_handle_t *h, lsn_t off, const byte *dat, lsn_t len) {
  raid1_impl *i = h->impl;
  int retA = i->a->write(i->a, off, dat, len);
  int retB = i->b->write(i->b, off, dat, len);
  return retA ? retA : retB;
}
static stasis_write_buffer_t * raid1_write_buffer(stasis_handle_t *h, lsn_t off, lsn_t len) {
  raid1_impl *i = h->impl;
  stasis_write_buffer_t * ret = i->a->write_buffer(i->a, off, len);
  ret->h = h;
  return ret;
}
static int raid1_release_write_buffer(stasis_write_buffer_t *w) {
  raid1_impl *i = w->h->impl;
  w->h = i->a;
  assert(w->h == i->a);
  int retA = i->b->write(i->b, w->off, w->buf, w->len);
  int retB = w->h->release_write_buffer(w);
  return retA ? retA : retB;
}
static stasis_read_buffer_t *raid1_read_buffer(stasis_handle_t *h,
					       lsn_t off, lsn_t len) {
  raid1_impl *i = h->impl;
  struct timeval tv;
  gettimeofday(&tv, 0);
  // use some low bit that's likely "real" as a source of randomness
  if(tv.tv_usec & 0x040) {
    return i->a->read_buffer(i->a, off, len);
  } else {
    return i->b->read_buffer(i->b, off, len);
  }
}
static int raid1_release_read_buffer(stasis_read_buffer_t *r) {
  // Should not be called.
  abort();
}
static int raid1_force(stasis_handle_t *h) {
  raid1_impl *i = h->impl;
  int retA = i->a->force(i->a);
  int retB = i->b->force(i->b);
  return retA ? retA : retB;
}
static int raid1_force_range(stasis_handle_t *h, lsn_t start, lsn_t stop) {
  raid1_impl *i = h->impl;
  int retA = i->a->force_range(i->a, start, stop);
  int retB = i->b->force_range(i->b, start, stop);
  return retA ? retA : retB;
}
struct stasis_handle_t raid1_func = {
  .num_copies = raid1_num_copies,
  .num_copies_buffer = raid1_num_copies_buffer,
  .close = raid1_close,
  .dup = raid1_dup,
  .enable_sequential_optimizations = raid1_enable_sequential_optimizations,
  .end_position = raid1_end_position,
  .write = raid1_write,
  .write_buffer = raid1_write_buffer,
  .release_write_buffer = raid1_release_write_buffer,
  .read = raid1_read,
  .read_buffer = raid1_read_buffer,
  .release_read_buffer = raid1_release_read_buffer,
  .force = raid1_force,
  .force_range = raid1_force_range,
  .error = 0
};

stasis_handle_t * stasis_handle_open_raid1(stasis_handle_t* a, stasis_handle_t* b) {
  stasis_handle_t * ret = malloc(sizeof(*ret));
  *ret = raid1_func;
  raid1_impl * i = malloc(sizeof(*i));
  i->a = a; i->b = b;
  ret->impl = i;
  return ret;
}

stasis_handle_t * stasis_handle_raid1_factory() {
  stasis_handle_t * a = stasis_handle_file_factory(stasis_store_file_1_name, O_CREAT | O_RDWR | stasis_buffer_manager_io_handle_flags, FILE_PERM);
  stasis_handle_t * b = stasis_handle_file_factory(stasis_store_file_2_name, O_CREAT | O_RDWR | stasis_buffer_manager_io_handle_flags, FILE_PERM);
  return stasis_handle_open_raid1(a, b);
}
#endif
