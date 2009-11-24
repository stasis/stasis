/*
 * tsearchcompat.c
 *
 *  Created on: Nov 9, 2009
 *      Author: sears
 */
#define _GNU_SOURCE
#include <search.h>
#include <stdlib.h>
#include <stasis/tsearchcompat.h>
#include <stdio.h>
#include <pthread.h>
#define VISIT VISIT_FOO
#define preorder preorder_foo
#define postorder postorder_foo
#define endorder endeorder_foo
#define leaf leaf_foo
#include <stasis/redblack.h>
#undef VISIT
#undef preorder
#undef postorder
#undef endorder
#undef leaf

typedef struct compat_rbtree {
  void * root;
  int (*cmp)(const void *, const void*);
} compat_rbtree;

rbtree * compat_rbinit(int (*cmp)(const void *, const void *), int ignored) {
  compat_rbtree* ret = malloc(sizeof(*ret));
  ret->root = 0;
  ret->cmp = cmp;
  return (void*) ret;
}

const void * compat_rbdelete(const void * key, rbtree * tp) {
  compat_rbtree* t = (compat_rbtree*)tp;
  void** retp = (void**)tfind(key, &(t->root), t->cmp);
  void* ret = retp ? *retp : 0;
  if(ret) {
//  void** check = (void**)
    tdelete(key, &(t->root), t->cmp);
  }
  return ret;
}
const void * compat_rbfind(const void * key, rbtree * tp) {
  compat_rbtree* t = (compat_rbtree*)tp;
  void** ret = (void**)tfind(key, &(t->root), t->cmp);
  return ret ? *ret : 0;
}
const void * compat_rbsearch(const void * key, rbtree * tp) {
  compat_rbtree* t = (compat_rbtree*)tp;
  void** ret = (void**)tsearch(key, &(t->root), t->cmp);
  return ret ? *ret : 0;
}
static void noop_free(void* node) {}
void compat_rbdestroy(rbtree * tp) {
  compat_rbtree* t = (compat_rbtree*)tp;
  if(t->root) {
    tdestroy(&(t->root), noop_free);
  }
}
static pthread_mutex_t mut = PTHREAD_MUTEX_INITIALIZER;
static int done;
static const void * val;
static const void * key;
static int (*cmp)(const void *, const void*);
static int mode;
void action(const void * node, VISIT which, int depth) {
  if(which == leaf || which == postorder) {
//    printf("val %lx %d %d %d %lx\n", node, which, depth, done, val); fflush(stdout);
    if(done == 0) {
      if(mode == RB_LUGTEQ) {
        if(cmp(key, *(void**)node) <= 0) {
          val = *(void**)node; done = 1;
        }
      } else if(mode == RB_LUGREAT) {
        if(cmp(key, *(void**)node) < 0) {
          done = 1;
          val = *(void**)node;
        }
      } else if(mode == RB_LUFIRST) {
        done = 1;
        val = *(void**)node;
      }
    }
  }
}

const void * compat_rblookup(int m, const void * k, rbtree *tp) {
  compat_rbtree* t = (compat_rbtree*)tp;
  pthread_mutex_lock(&mut);
  done = 0;
  val = 0;
  key = k;
  cmp = t->cmp;
  mode = m;
  twalk(t->root, action);
  pthread_mutex_unlock(&mut);
  return val;
}
//const void * compat_rblookup(int m, const void * k, rbtree *tp) {
//  compat_rbtree* t = (compat_rbtree*)tp;
//  return compat_rblookup_help(m, k, tp, t->cmp);
//}
const void * compat_rbmin(rbtree * tp) {
  return compat_rblookup(RB_LUFIRST, 0, tp);
}
