/*
 * stlredblack.cpp
 *
 *  Created on: Nov 10, 2009
 *      Author: sears
 */
#include <set>
#include <stasis/common.h>
#undef STLSEARCH
#include <stasis/stlredblack.h>
#include <stasis/redblack.h>
#include <stdio.h>
#undef end

extern "C" {
  typedef int (*c_cmp_t)(const void*, const void*, const void*);
}

class MyCompare {
  c_cmp_t cmp_;
  const void *arg_;
  public:
  bool operator() (const void* const &arg1, const void* const &arg2) const {
    return cmp_(arg1,arg2,arg_) < 0;
  };
  MyCompare(c_cmp_t cmp, int dummy) : cmp_(cmp), arg_(NULL) {}
};

typedef std::set<const void*,MyCompare> rb;

extern "C" {

rbtree * stl_rbinit(int(*cmp)(const void*,const void*,const void*), const void* ignored) {
  return reinterpret_cast<rbtree*>(new rb(MyCompare(cmp, 0)));
}
const void * stl_rbdelete(const void * key, rbtree * tp) {
  rb::iterator it = reinterpret_cast<rb*>(tp)->find(key);
  if(it != reinterpret_cast<rb*>(tp)->end()) {
    const void* ret = *it;
    reinterpret_cast<rb*>(tp)->erase(it);
    return ret;
  } else {
    return NULL;
  }
}
const void * stl_rbfind(const void * key, rbtree * tp) {
  rb::iterator it = reinterpret_cast<rb*>(tp)->find(key);
  if(it != reinterpret_cast<rb*>(tp)->end()) {
    return *it;
  } else {
    return NULL;
  }
}
const void * stl_rbsearch(const void * key, rbtree * tp) {
  std::pair<rb::iterator, bool> p = reinterpret_cast<rb*>(tp)->insert(key);
//  printf("insert succeeded? %d\n", p.second);
  return *(p.first);
}
const void * stl_rblookup(int m, const void * key, rbtree *tp) {
  rb* t = reinterpret_cast<rb*>(tp);
  rb::iterator it = t->lower_bound(key);
  if(m == RB_LUGREAT) {
//    printf("great\n");
    if(t->find(key) != t->end()) {
//      printf("lookup incremented\n");
      it++;
    }

  } else if(m == RB_LUGTEQ) {
//    printf("greatequal\n");
    // nothing to do.
  } else {
    abort();
  }
  if(it == t->end()) {
//    printf("lookup failed\n");
    return NULL;
  } else {
//    printf("lookup succeeded\n");
    return *it;
  }
}
const void * stl_rbmin(rbtree *tp) {
  return *reinterpret_cast<rb*>(tp)->begin();
}
void stl_rbdestroy(rbtree * tp) {
  delete reinterpret_cast<rb*>(tp);
}
}
