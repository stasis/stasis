/*
 * tsearchcompat.h
 *
 *  Created on: Nov 9, 2009
 *      Author: sears
 */

#ifndef TSEARCHCOMPAT_H_
#define TSEARCHCOMPAT_H_
typedef struct rbtree rbtree;
rbtree * compat_rbinit(int (*cmp)(const void *, const void *), int ignored);
const void * compat_rbdelete(const void * key, rbtree * tp);
const void * compat_rbfind(const void * key, rbtree * tp);
const void * compat_rbsearch(const void * key, rbtree * tp);
const void * compat_rblookup(int m, const void * k, rbtree *tp);
const void * compat_rbmin(rbtree *tp);
void compat_rbdestroy(rbtree * tp);

#ifdef TSEARCH
#include <stasis/tsearchcompat.h>
#define rbinit compat_rbinit
#define rbdestroy compat_rbdestroy
#define rbdelete compat_rbdelete
#define rbsearch compat_rbsearch
#define rbfind compat_rbfind
#undef rbmin
#define rbmin compat_rbmin
#define rblookup compat_rblookup
#endif


#endif /* TSEARCHCOMPAT_H_ */
