#include <stasis/io/handle.h>
#include <stdio.h>

/** @file

  This implements a io handle that wraps another handle, logging each
  call (and its result) to stdout.

*/

typedef struct debug_impl {
  stasis_handle_t * h;
} debug_impl;

static int debug_num_copies(stasis_handle_t * h) {
  stasis_handle_t * hh = ((debug_impl*)h->impl)->h;
  printf("tid=%9ld call num_copies(%lx)\n", (long)(intptr_t)pthread_self(), (unsigned long)hh);  fflush(stdout);
  int ret = hh->num_copies(hh);
  printf("tid=%9ld retn num_copies(%lx) = %d\n", (long)(intptr_t)pthread_self(), (unsigned long)hh, ret); fflush(stdout);
  return ret;
}
static int debug_num_copies_buffer(stasis_handle_t * h) {
  stasis_handle_t * hh = ((debug_impl*)h->impl)->h;
  printf("tid=%9ld call num_copies_buffer(%lx)\n", (long)(intptr_t)pthread_self(), (unsigned long)hh); fflush(stdout);
  int ret = hh->num_copies_buffer(hh);
  printf("tid=%9ld retn num_copies_buffer(%lx) = %d\n", (long)(intptr_t)pthread_self(), (unsigned long)hh, ret); fflush(stdout);
  return ret;
}
static int debug_close(stasis_handle_t * h) {
  stasis_handle_t * hh = ((debug_impl*)h->impl)->h;
  printf("tid=%9ld call close(%lx)\n", (long)(intptr_t)pthread_self(), (unsigned long)hh); fflush(stdout);
  int ret = hh->close(hh);
  printf("tid=%9ld retn close(%lx) = %d\n", (long)(intptr_t)pthread_self(), (unsigned long)hh, ret); fflush(stdout);
  free(h->impl);
  free(h);
  return ret;
}
static stasis_handle_t * debug_dup(stasis_handle_t * h) {
  stasis_handle_t * hh = ((debug_impl*)h->impl)->h;
  printf("tid=%9ld call dup(%lx)\n", (long)(intptr_t)pthread_self(), (unsigned long)hh); fflush(stdout);
  stasis_handle_t * ret = hh->dup(hh);
  printf("tid=%9ld retn dup(%lx) = %lx\n", (long)(intptr_t)pthread_self(), (unsigned long)hh, (unsigned long)ret); fflush(stdout);
  return ret;
}
static void debug_enable_sequential_optimizations(struct stasis_handle_t *h) {
  stasis_handle_t * hh = ((debug_impl*)h->impl)->h;
  printf("tid=%9ld call enable_sequential_optimizations(%lx)\n", (long)(intptr_t)pthread_self(), (unsigned long)hh); fflush(stdout);
  hh->enable_sequential_optimizations(hh);
  printf("tid=%9ld retn enable_sequential_optimizations(%lx)\n", (long)(intptr_t)pthread_self(), (unsigned long)hh); fflush(stdout);
}
static lsn_t debug_end_position(stasis_handle_t *h) {
  stasis_handle_t * hh = ((debug_impl*)h->impl)->h;
  printf("tid=%9ld call end_position(%lx)\n", (long)(intptr_t)pthread_self(), (unsigned long)hh); fflush(stdout);
  int ret = hh->end_position(hh);
  printf("tid=%9ld retn end_position(%lx) = %d\n", (long)(intptr_t)pthread_self(), (unsigned long)hh, ret); fflush(stdout);
  return ret;
}
static stasis_write_buffer_t * debug_write_buffer(stasis_handle_t * h,
						lsn_t off, lsn_t len) {
  stasis_handle_t * hh = ((debug_impl*)h->impl)->h;
  printf("tid=%9ld call write_buffer(%lx, %lld, %lld)\n",
         (long)(intptr_t)pthread_self(), (unsigned long)hh, off, len); fflush(stdout);
  stasis_write_buffer_t * ret = hh->write_buffer(hh,off,len);
  stasis_write_buffer_t * retWrap = stasis_alloc(stasis_write_buffer_t);
  *retWrap = *ret;
  retWrap->h = h;
  retWrap->impl = ret;
  printf("tid=%9ld retn write_buffer(%lx, %lld, %lld) = %lx\n",
         (long)(intptr_t)pthread_self(), (unsigned long)hh, off, len, (unsigned long)retWrap); fflush(stdout);
  return retWrap;
}
static int debug_release_write_buffer(stasis_write_buffer_t * w_wrap) {
  stasis_write_buffer_t * w = (stasis_write_buffer_t*)w_wrap->impl;
  stasis_handle_t * hh = w->h;

  // Debugging output adds a parameter, h, for convenience.
  printf("tid=%9ld call release_write_buffer(%lx, %lx)\n", (long)(intptr_t)pthread_self(), (unsigned long)hh, (unsigned long)w_wrap); fflush(stdout);
  int ret = hh->release_write_buffer(w);
  printf("tid=%9ld retn release_write_buffer(%lx, %lx) = %d\n", (long)(intptr_t)pthread_self(), (unsigned long)hh, (unsigned long)w_wrap, ret); fflush(stdout);
  return ret;
}
static stasis_read_buffer_t * debug_read_buffer(stasis_handle_t * h,
					      lsn_t off, lsn_t len) {
  stasis_handle_t * hh = ((debug_impl*)h->impl)->h;
  printf("tid=%9ld call read_buffer(%lx, %lld, %lld)\n",
         (long)(intptr_t)pthread_self(), (unsigned long)hh, off, len); fflush(stdout);
  stasis_read_buffer_t * ret = hh->read_buffer(hh,off,len);
  stasis_read_buffer_t * retWrap = stasis_alloc(stasis_read_buffer_t);
  *retWrap = *ret;
  retWrap->h = h;
  retWrap->impl = ret;
  printf("tid=%9ld retn read_buffer(%lx, %lld, %lld) = %lx\n",
         (long)(intptr_t)pthread_self(), (unsigned long)hh, off, len, (unsigned long)retWrap); fflush(stdout);
  return retWrap;

}

static int debug_release_read_buffer(stasis_read_buffer_t * r_wrap) {
  stasis_read_buffer_t * r = (stasis_read_buffer_t*)r_wrap->impl;
  stasis_handle_t * hh = r->h;

  // Debugging output adds a parameter, h, for convenience.
  printf("tid=%9ld call release_read_buffer(%lx, %lx)\n", (long)(intptr_t)pthread_self(), (unsigned long)hh, (unsigned long)r_wrap); fflush(stdout);
  int ret = hh->release_read_buffer(r);
  printf("tid=%9ld retn release_read_buffer(%lx, %lx) = %d\n", (long)(intptr_t)pthread_self(), (unsigned long)hh, (unsigned long)r_wrap, ret); fflush(stdout);
  return ret;
}

static int debug_write(stasis_handle_t * h, lsn_t off,
		     const byte * dat, lsn_t len) {
  stasis_handle_t * hh = ((debug_impl*)h->impl)->h;
  printf("tid=%9ld call write(%lx, %lld, %lx, %lld)\n", (long)(intptr_t)pthread_self(), (unsigned long)hh, off, (unsigned long)dat, len); fflush(stdout);
  int ret = hh->write(hh, off, dat, len);
  printf("tid=%9ld retn write(%lx) = %d\n", (long)(intptr_t)pthread_self(), (unsigned long)hh, ret); fflush(stdout);
  return ret;
}
static int debug_read(stasis_handle_t * h,
		    lsn_t off, byte * buf, lsn_t len) {
  stasis_handle_t * hh = ((debug_impl*)h->impl)->h;
  printf("tid=%9ld call read(%lx, %lld, %lx, %lld)\n", (long)(intptr_t)pthread_self(), (unsigned long)hh, off, (unsigned long)buf, len); fflush(stdout);
  int ret = hh->read(hh, off, buf, len);
  printf("tid=%9ld retn read(%lx) = %d\n", (long)(intptr_t)pthread_self(), (unsigned long)hh, ret); fflush(stdout);
  return ret;
}
static int debug_force(stasis_handle_t *h) {
  stasis_handle_t * hh = ((debug_impl*)h->impl)->h;
  printf("tid=%9ld call force(%lx)\n", (long)(intptr_t)pthread_self(), (unsigned long)hh); fflush(stdout);
  int ret = hh->force(hh);
  printf("tid=%9ld retn force(%lx) = %d\n", (long)(intptr_t)pthread_self(), (unsigned long)hh, ret); fflush(stdout);
  return ret;
}
static int debug_force_range(stasis_handle_t *h, lsn_t start, lsn_t stop) {
  stasis_handle_t * hh = ((debug_impl*)h->impl)->h;
  printf("tid=%9ld call force(%lx,%lld,%lld)\n", (long)(intptr_t)pthread_self(), (unsigned long)hh, start, stop); fflush(stdout);
  int ret = hh->force_range(hh, start, stop);
  printf("tid=%9ld retn force(%lx) = %d\n", (long)(intptr_t)pthread_self(), (unsigned long)hh, ret); fflush(stdout);
  return ret;
}
struct stasis_handle_t debug_func = {
  .num_copies = debug_num_copies,
  .num_copies_buffer = debug_num_copies_buffer,
  .close = debug_close,
  .dup = debug_dup,
  .enable_sequential_optimizations = debug_enable_sequential_optimizations,
  .end_position = debug_end_position,
  .write = debug_write,
  .write_buffer = debug_write_buffer,
  .release_write_buffer = debug_release_write_buffer,
  .read = debug_read,
  .read_buffer = debug_read_buffer,
  .release_read_buffer = debug_release_read_buffer,
  .force = debug_force,
  .force_range = debug_force_range,
  .error = 0
};


stasis_handle_t * stasis_handle(open_debug)(stasis_handle_t * h) {
  stasis_handle_t * ret = stasis_alloc(stasis_handle_t);
  *ret = debug_func;
  ret->impl = stasis_alloc(debug_impl);
  ((debug_impl*)(ret->impl))->h = h;
  return ret;
}
