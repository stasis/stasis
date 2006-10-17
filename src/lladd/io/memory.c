#include <lladd/io/handle.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>


typedef struct mem_impl {
  pthread_mutex_t mut;
  lsn_t start_pos;
  lsn_t end_pos;
  byte * buf;
} mem_impl;

static int mem_num_copies(stasis_handle_t * h) { return 1; }
static int mem_num_copies_buffer(stasis_handle_t * h) { return 0; }

static int mem_close(stasis_handle_t * h) {
  free(((mem_impl*)h->impl)->buf);
  pthread_mutex_destroy(&(((mem_impl*)h->impl)->mut));
  free(h->impl);
  free(h);
  return 0;
}
static lsn_t mem_start_position(stasis_handle_t *h) {
  lsn_t ret;
  mem_impl* impl = (mem_impl*)(h->impl);
  
  pthread_mutex_lock(&impl->mut);
  ret = impl->start_pos;
  pthread_mutex_unlock(&impl->mut);

  return ret;
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

  stasis_write_buffer_t * ret = malloc(sizeof(stasis_write_buffer_t));
  if(!ret) { return NULL; }

  pthread_mutex_lock(&(impl->mut));

  int error = 0;

  if(impl->start_pos > off) { 
    error = EDOM;
  } else if(impl->end_pos > off+len) { 
    // Just need to return buffer; h's state is unchanged.
  } else { 
    byte * newbuf = realloc(impl->buf, off+len - impl->start_pos);
    
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
    ret->buf = &(impl->buf[off-impl->start_pos]);
    ret->len = len;
    ret->impl = 0;
    ret->error = 0;
  }

  return ret;
}

static stasis_write_buffer_t * mem_append_buffer(stasis_handle_t * h, 
						 lsn_t len) {
  mem_impl * impl = (mem_impl*)(h->impl);

  stasis_write_buffer_t * ret = malloc(sizeof(stasis_write_buffer_t));
  if(!ret) { return 0; }

  pthread_mutex_lock(&(impl->mut));

  lsn_t off = impl->end_pos;
  impl->end_pos += len;
  byte * newbuf = realloc(impl->buf, impl->end_pos - impl->start_pos);
  if(newbuf) { 
    impl->buf = newbuf;
  
    ret->h = h;
    ret->off = off;
    ret->buf = &(impl->buf[off-impl->start_pos]);
    ret->len = len;
    ret->impl = 0;
    ret->error = 0;
  } else { 
    ret->h = h;
    ret->off = 0;
    ret->buf = 0;
    ret->len = 0;
    ret->impl = 0;
    ret->error = ENOMEM;
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
  
  stasis_read_buffer_t * ret = malloc(sizeof(stasis_read_buffer_t));
  if(!ret) { return NULL; }

  if(off < impl->start_pos || off + len > impl->end_pos) { 
    ret->h = h;
    ret->buf = 0;
    ret->len = 0;
    ret->impl = 0;
    ret->error = EDOM;
  } else {
    ret->h = h;
    ret->buf = &(impl->buf[off-impl->start_pos]);
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

static int mem_append(stasis_handle_t * h, lsn_t * off, const byte * dat, lsn_t len) { 
  stasis_write_buffer_t * w = mem_append_buffer(h, len);
  int ret;
  if(w->error) { 
    ret = w->error;
  } else { 
    memcpy(w->buf, dat, len);
    ret = 0;
  }
  *off = w->off;
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


static int mem_truncate_start(stasis_handle_t * h, lsn_t new_start) { 
  mem_impl* impl = (mem_impl*) h->impl;
  pthread_mutex_lock(&(impl->mut));
  if(new_start < impl->start_pos) { 
    pthread_mutex_unlock(&impl->mut);
    return 0;
  } 
  if(new_start > impl->end_pos) { 
    pthread_mutex_unlock(&impl->mut);
    return EDOM;
  }

  byte * new_buf = malloc(impl->end_pos -new_start);
  
  memcpy(new_buf, &(impl->buf[new_start - impl->start_pos]), impl->end_pos - new_start);

  free(impl->buf);

  impl->buf = new_buf;
  impl->start_pos = new_start;

  pthread_mutex_unlock(&(impl->mut));
  return 0;
}

struct stasis_handle_t mem_func = {
  .num_copies = mem_num_copies,
  .num_copies_buffer = mem_num_copies_buffer,
  .close = mem_close,
  .start_position = mem_start_position,
  .end_position = mem_end_position,
  .write = mem_write,
  .append = mem_append,
  .write_buffer = mem_write_buffer,
  .append_buffer = mem_append_buffer,
  .release_write_buffer = mem_release_write_buffer,
  .read = mem_read,
  .read_buffer = mem_read_buffer,
  .release_read_buffer = mem_release_read_buffer,
  .truncate_start = mem_truncate_start,
  .error = 0
};

stasis_handle_t * stasis_handle(open_memory)() {
  stasis_handle_t * ret = malloc(sizeof(stasis_handle_t));
  *ret = mem_func;

  mem_impl * impl = malloc(sizeof(mem_impl));
  ret->impl = impl;
  pthread_mutex_init(&(impl->mut), 0);
  impl->start_pos = 0;
  impl->end_pos = 0;
  impl->buf = malloc(0);
    
  return ret;
}
