/*
 * stlredblack.h
 *
 *  Created on: Nov 23, 2009
 *      Author: sears
 */

#ifndef STLREDBLACK_H_
#define STLREDBLACK_H_

BEGIN_C_DECLS

typedef struct rbtree rbtree;
rbtree * stl_rbinit(int (*cmp)(const void *, const void *, const void*), const void *ignored);
const void * stl_rbdelete(const void * key, rbtree * tp);
const void * stl_rbfind(const void * key, rbtree * tp);
const void * stl_rbsearch(const void * key, rbtree * tp);
const void * stl_rblookup(int m, const void * k, rbtree *tp);
const void * stl_rbmin(rbtree *tp);
void stl_rbdestroy(rbtree * tp);

#ifdef STLSEARCH
#define rbinit stl_rbinit
#define rbdestroy stl_rbdestroy
#define rbdelete stl_rbdelete
#define rbsearch stl_rbsearch
#define rbfind stl_rbfind
#undef rbmin
#define rbmin stl_rbmin
#define rblookup stl_rblookup
#endif

/* Modes for rblookup */
//#define RB_NONE -1      /* None of those below */
//#define RB_LUEQUAL 0    /* Only exact match */
#define RB_LUGTEQ 1     /* Exact match or greater */
//#define RB_LULTEQ 2     /* Exact match or less */
//#define RB_LULESS 3     /* Less than key (not equal to) */
#define RB_LUGREAT 4    /* Greater than key (not equal to) */
//#define RB_LUNEXT 5     /* Next key after current */
//#define RB_LUPREV 6     /* Prev key before current */
//#define RB_LUFIRST 7    /* First key in index */
//#define RB_LULAST 8     /* Last key in index */


END_C_DECLS

#endif /* STLREDBLACK_H_ */
