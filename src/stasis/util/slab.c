#include <stasis/common.h>
#include <stasis/util/slab.h>
#include <assert.h>
#include <stdio.h>

/**
 * @file A simple (single-threaded) slab allocator
 *
 *  Created on: Nov 12, 2010
 *      Author: sears
 */
struct stasis_util_slab_t {
  byte ** blocks;
  void * freelist_ptr;
  uint32_t obj_sz;
  uint32_t block_sz;
  uint32_t objs_per_block;
  uint32_t this_block_count;
  uint32_t this_block;
  uint32_t refcount; // the #*%(^!!! stl wants to copy-construct us to %$!! and back, so we need a way to refcount...
};
/**
 * Create a slab allocator
 * @param the size of each object to be allocated
 * @return a slab allocator.  Calling stasis_util_slab_destroy() will deallocate it all-at-once.
 */
stasis_util_slab_t * stasis_util_slab_create(uint32_t obj_sz, uint32_t block_sz) {
  stasis_util_slab_t* ret = stasis_alloc(stasis_util_slab_t);

  //  printf("slab init: obj siz = %lld, block_sz = %lld\n", (long long)obj_sz, (long long)block_sz);

  ret->blocks = stasis_alloc(byte*);
  ret->blocks[0] = stasis_malloc(block_sz, byte);
  ret->freelist_ptr = 0;

  ret->obj_sz = obj_sz;
  ret->block_sz = block_sz;
  ret->objs_per_block = block_sz / obj_sz;

  ret->this_block_count = 0;
  ret->this_block = 0;
  ret->refcount = 1;
  return ret;
}
void stasis_util_slab_ref(stasis_util_slab_t * slab) {
  (slab->refcount)++;
}
void stasis_util_slab_destroy(stasis_util_slab_t * slab) {
  (slab->refcount)--;
  if(!slab->refcount) {
    for(int i = 0; i <= slab->this_block; i++) {
      free(slab->blocks[i]);
    }
    free(slab->blocks);
    fprintf(stderr, "Deallocated slab (obj_sz = %lld).  Was using %lld bytes\n", 
	    (long long) (uint64_t)slab->obj_sz,
	    (long long int)
	    ((uint64_t)slab->this_block + 1) * (uint64_t)slab->block_sz);
    free(slab);
  }
}
// XXX get rid of multiplications!  Store next free byte instead of obj count!
void* stasis_util_slab_malloc(stasis_util_slab_t * slab) {
  if(slab->freelist_ptr) {
    void * ret = slab->freelist_ptr;
    slab->freelist_ptr = *(void**)slab->freelist_ptr;
    assert(ret);
    //    printf("ALLOC %llx\n", (long long) ret);
    return ret;
  } else if(slab->this_block_count == slab->objs_per_block) {
    (slab->this_block_count) = 0;
    (slab->this_block) ++;
    slab->blocks = stasis_realloc(slab->blocks, slab->this_block+1, byte*);
    assert(slab->blocks);
    slab->blocks[slab->this_block] = stasis_malloc(slab->block_sz, byte);
    assert(slab->blocks[slab->this_block]);
  }
  slab->this_block_count ++;
  void * ret = (void*) &(slab->blocks[slab->this_block][(slab->this_block_count-1) * slab->obj_sz]);
  assert(ret);
  //  printf("ALLOC %llx\n", (long long) ret);
  return ret;
}
void stasis_util_slab_free(stasis_util_slab_t * slab, void * ptr) {
  //  printf("FREE %llx\n", (long long) ptr);
  assert(ptr);
  *((void**)ptr) = slab->freelist_ptr;
  slab->freelist_ptr = ptr;
}

