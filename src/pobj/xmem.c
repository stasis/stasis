#include <stdlib.h>
#include "common.h"
#include "xmem.h"


struct xmem {
#ifdef HAVE_XMEM_DETAIL
    /* Extended memory block info. */
    int mtype;
    char *file;
    int line;
    int size;
    struct xmem *prev;
    struct xmem *next;
#endif /* HAVE_XMEM_DETAIL */

    /* Dummy field used to infer alignment. */
    int dummy;
};
#define XMEM_OFFSET  member_offset (struct xmem, dummy)

#ifdef HAVE_XMEM
struct xmem_stat {
    unsigned long count;
#ifdef HAVE_XMEM_DETAIL
    unsigned long size;
    struct xmem *head;
    struct xmem *tail;
#endif /* HAVE_XMEM_DETAIL */
} xmem_stats[XMEM_MAX] = { { 0 } };
#endif /* HAVE_XMEM */


/* Default memory calls. */
void *(*g_memfunc_malloc) (size_t) = malloc;
void (*g_memfunc_free) (void *) = free;


void *
xmem_malloc (int mtype, char *file, int line, size_t size)
{
    struct xmem *x;

    x = (struct xmem *) g_memfunc_malloc (size + XMEM_OFFSET);
    if (! x)
	exit (1);

#ifdef HAVE_XMEM_DETAIL
    /* Update object's extended info. */
    x->mtype = mtype;
    x->file = file;
    x->line = line;
    x->size = size;

    /* Attach to object type list. */
    x->next = NULL;
    x->prev = xmem_stats[mtype].tail;
    if (xmem_stats[mtype].tail)
	xmem_stats[mtype].tail->next = x;
    xmem_stats[mtype].tail = x;
    if (! xmem_stats[mtype].head)
	xmem_stats[mtype].head = x;

    /* Update accumulated object sizes. */
    xmem_stats[mtype].size += (unsigned long) size;
#endif  /* HAVE_XMEM_DETAIL */

#ifdef HAVE_XMEM
    /* Update accumulated type counter. */
    xmem_stats[mtype].count++;
#endif /* HAVE_XMEM */
    
    return (void *) (((char *) x) + XMEM_OFFSET);
}

void
xmem_free (int mtype, void *p)
{
    struct xmem *x = (struct xmem *) (((char *) p) - XMEM_OFFSET);

#ifdef HAVE_XMEM_DETAIL
    /* Verify object type consistency. */
    if (mtype == XMEM_MAX)
	mtype = x->mtype;
    else if (mtype != x->mtype)
	exit (1);

    /* Detach from type list. */
    if (x->prev)
	x->prev->next = x->next;
    else
	xmem_stats[mtype].head = x->next;
    if (x->next)
	x->next->prev = x->prev;
    else
	xmem_stats[mtype].tail = x->prev;

    /* Update accumulated object sizes. */
    xmem_stats[mtype].size -= (unsigned long) x->size;
#endif /* HAVE_XMEM_DETAIL */

#ifdef HAVE_XMEM
    /* Update accumulated type counter. */
    xmem_stats[mtype].count--;
#endif /* HAVE_XMEM */

    g_memfunc_free (x);
}

int
xmem_memfunc (void *(*memfunc_malloc)(size_t), void (*memfunc_free)(void *))
{
    if (memfunc_malloc)
	g_memfunc_malloc = memfunc_malloc;
    if (memfunc_free)
	g_memfunc_free = memfunc_free;

    return 0;
}


int
xmem_obj_mtype (void *p)
{
#ifdef HAVE_XMEM_DETAIL
    struct xmem *x = (struct xmem *) (((char *) p) - XMEM_OFFSET);
    return x->mtype;
#else
    return 0;
#endif /* HAVE_XMEM_DETAIL */
}

char *
xmem_obj_file (void *p)
{
#ifdef HAVE_XMEM_DETAIL
    struct xmem *x = (struct xmem *) (((char *) p) - XMEM_OFFSET);
    return x->file;
#else
    return NULL;
#endif /* HAVE_XMEM_DETAIL */
}

int
xmem_obj_line (void *p)
{
#ifdef HAVE_XMEM_DETAIL
    struct xmem *x = (struct xmem *) (((char *) p) - XMEM_OFFSET);
    return x->line;
#else
    return 0;
#endif /* HAVE_XMEM_DETAIL */
}

size_t
xmem_obj_size (void *p)
{
#ifdef HAVE_XMEM_DETAIL
    struct xmem *x = (struct xmem *) (((char *) p) - XMEM_OFFSET);
    return x->size;
#else
    return 0;
#endif /* HAVE_XMEM_DETAIL */
}

unsigned long
xmem_stat_count (int mtype)
{
#ifdef HAVE_XMEM
    return xmem_stats[mtype].count;
#else
    return 0;
#endif /* HAVE_XMEM */
}

unsigned long
xmem_stat_size (int mtype)
{
#ifdef HAVE_XMEM_DETAIL
    return xmem_stats[mtype].size;
#else
    return 0;
#endif /* HAVE_XMEM_DETAIL */
}

unsigned long
xmem_stat_alloc_list (int mtype, void (*cb) (char *, int, size_t, void *))
{
    unsigned long count = 0;
#ifdef HAVE_XMEM_DETAIL
    struct xmem *x = xmem_stats[mtype].head;

    while (x) {
	cb (x->file, x->line, x->size, ((char *) x) + XMEM_OFFSET);
	count++;
	x = x->next;
    }
#endif /* HAVE_XMEM_DETAIL */

    return count;
}
