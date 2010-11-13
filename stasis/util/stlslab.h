/*
 * stlslab.h
 *
 *  Created on: Nov 12, 2010
 *      Author: sears
 */

#ifndef STLSLAB_H_
#define STLSLAB_H_
#include <algorithm>
#include <stdio.h>

#include <stasis/util/slab.h>
#include <assert.h>

template <class T>
class stlslab {
public:
  typedef T         value_type;
  typedef T*        pointer;
  typedef T&        reference;
  typedef const T*  const_pointer;
  typedef const T&  const_reference;
  typedef std::size_t    size_type;
  typedef std::ptrdiff_t difference_type;

  stlslab ( ) throw() {
    alloc = stasis_util_slab_create(sizeof(T), 65536);
  }
  stlslab ( const stlslab& s) throw() {
    printf("Expensive!!\n");
    alloc = stasis_util_slab_create(sizeof(T), /*sizeof(U),*/ 65536);
    //    alloc = s.alloc;
    //    assert(alloc);
    stasis_util_slab_ref(alloc);
  }
  template <class U>
    stlslab( const stlslab<U>& s) throw() {
    //    printf("sizeof U %lld\n", (long long)sizeof(U));
    alloc = s.alloc;

    //assert(alloc);
    stasis_util_slab_ref(alloc);
  }
  ~stlslab () {
    stasis_util_slab_destroy(alloc);
  }
  pointer address ( reference x ) const { return &x; }
  const_pointer address ( const_reference x ) const { return &x; }

  template<typename _Tp1>
    struct rebind
    { typedef stlslab<_Tp1> other; };

  pointer allocate (size_type n, stlslab::const_pointer hint=0) {
    assert(n == 1);
    return (T*) stasis_util_slab_malloc(alloc);
  }
  void deallocate (pointer p, size_type n) {
    assert(n == 1);
    stasis_util_slab_free(alloc, p);
  }
  size_type max_size() const throw() { return 1; }
  void construct ( pointer p, const_reference val ) { new ((void*)p) T (val); }
  void destroy (pointer p) {((T*)p)->~T(); }
  stasis_util_slab_t * alloc;
};

#endif /* STLSLAB_H_ */
