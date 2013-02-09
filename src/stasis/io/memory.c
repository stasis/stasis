#include <stasis/io/handle.h>
/** @file */

typedef struct mem_impl {
  pthread_mutex_t mut;
  lsn_t end_pos;
  byte * buf;
  int refcount;
} mem_impl;

static int mem_num_copies(stasis_handle_t * h) { return 1; }
static int mem_num_copies_buffer(stasis_handle_t * h) { return 0; }

static int mem_close(stasis_handle_t * h) {
  mem_impl *impl = (mem_impl *)h->impl;
  (impl->refcount)--;
  if(impl->refcount) { return 0; }

  free(impl->buf);
  pthread_mutex_destroy(&(impl->mut));
  free(h->impl);
  free(h);
  return 0;
}
static stasis_handle_t * mem_dup(stasis_handle_t *h) {
  mem_impl *impl = (mem_impl *)h->impl;
  (impl->refcount)++;
  return h;
}
static void mem_enable_sequential_optimizations(stasis_handle_t * h) {
  // No-op
}

static lsn_t mem_end_position(stasis_handle_t *h) {
  lsn_t ret;
  mem_impl* impl = (mem_impl*)(h->impl);

  pthread_mutex_lock(&impl->mut);
  ret = impl->end_pos;
  pthread_mutex_unlock(&impl->mut);

  return ret;
}
static stasis_write_buffer_t * mem_write_buffer(stasis_handle_t * h,
						lsn_t off, lsn_t len) {
  mem_impl* impl = (mem_impl*)(h->impl);

  stasis_write_buffer_t * ret = stasis_alloc(stasis_write_buffer_t);
  if(!ret) { return NULL; }

  pthread_mutex_lock(&(impl->mut));

  int error = 0;

  if(off < 0) {
    error = EDOM;
  } else if(impl->end_pos > off+len) {
    // Just need to return buffer; h's state is unchanged.
  } else {
    byte * newbuf;
    if(off+len) {
      newbuf = stasis_realloc(impl->buf, off+len, byte);
    } else {
      free(impl->buf);
      newbuf = stasis_malloc(0, byte);
    }
    if(newbuf) {
      impl->buf = newbuf;
      impl->end_pos = off+len;
    } else {
      error = ENOMEM;
    }
  }

  if(error) {
    ret->h = h;
    ret->off = 0;
    ret->buf = 0;
    ret->len = 0;
    ret->impl = 0;
    ret->error = error;
  } else {
    ret->h = h;
    ret->off = off;
    ret->buf = &(impl->buf[off]);
    ret->len = len;
    ret->impl = 0;
    ret->error = 0;
  }

  return ret;
}
static int mem_release_write_buffer(stasis_write_buffer_t * w) {
  mem_impl * impl = (mem_impl*)(w->h->impl);
  pthread_mutex_unlock(&(impl->mut));
  free(w);
  return 0;
}

static stasis_read_buffer_t * mem_read_buffer(stasis_handle_t * h,
					      lsn_t off, lsn_t len) {
  mem_impl * impl = (mem_impl*)(h->impl);
  pthread_mutex_lock(&(impl->mut));

  stasis_read_buffer_t * ret = stasis_alloc(stasis_read_buffer_t);
  if(!ret) { return NULL; }

  if(off < 0 || off + len > impl->end_pos) {
    ret->h = h;
    ret->buf = 0;
    ret->len = 0;
    ret->off = 0;
    ret->impl = 0;
    ret->error = EDOM;
  } else {
    ret->h = h;
    ret->buf = &(impl->buf[off]);
    ret->off = off;
    ret->len = len;
    ret->impl = 0;
    ret->error = 0;
  }
  return ret;
}
static int mem_release_read_buffer(stasis_read_buffer_t * r) {
  mem_impl * impl = (mem_impl*)(r->h->impl);
  pthread_mutex_unlock(&(impl->mut));
  free(r);
  return 0;
}

static int mem_write(stasis_handle_t * h, lsn_t off,
		     const byte * dat, lsn_t len) {
  // Overlapping writes aren't atomic; no latch needed.
  stasis_write_buffer_t * w = mem_write_buffer(h, off, len);
  int ret;
  if(w->error) {
    ret = w->error;
  } else {
    memcpy(w->buf, dat, len);
    ret = 0;
  }
  mem_release_write_buffer(w);
  return ret;
}

static int mem_read(stasis_handle_t * h,
		    lsn_t off, byte * buf, lsn_t len) {
  stasis_read_buffer_t * r = mem_read_buffer(h, off, len);
  int ret;
  if(r->error) {
    ret = r->error;
  } else {
    memcpy(buf, r->buf, len);
    ret = 0;
  }
  mem_release_read_buffer(r);
  return ret;
}
static int mem_force(stasis_handle_t *h) {
  return 0;
}
static int mem_force_range(stasis_handle_t *h,lsn_t start, lsn_t stop) {
  return 0;
}

struct stasis_handle_t mem_func = {
  /*.num_copies =*/ mem_num_copies,
  /*.num_copies_buffer =*/ mem_num_copies_buffer,
  /*.close =*/ mem_close,
  /*.dup =*/ mem_dup,
  /*.enable_sequential_optimizations =*/ mem_enable_sequential_optimizations,
  /*.end_position =*/ mem_end_position,
  /*.write_buffer =*/ mem_write_buffer,
  /*.release_write_buffer =*/ mem_release_write_buffer,
  /*.read_buffer =*/ mem_read_buffer,
  /*.release_read_buffer =*/ mem_release_read_buffer,
  /*.write =*/ mem_write,
  /*.read =*/ mem_read,
  /*.force =*/ mem_force,
  /*.async_force =*/ mem_force,
  /*.force_range =*/ mem_force_range,
  /*.fallocate =*/ NULL,
  /*.error =*/ 0
};

stasis_handle_t * stasis_handle(open_memory)(void) {
  stasis_handle_t * ret = stasis_alloc(stasis_handle_t);
  if(!ret) { return NULL; }
  *ret = mem_func;

  mem_impl * impl = stasis_alloc(mem_impl);
  ret->impl = impl;
  pthread_mutex_init(&(impl->mut), 0);
  impl->end_pos = 0;
  impl->buf = stasis_malloc(0, byte);
  impl->refcount = 1;

  return ret;
}
