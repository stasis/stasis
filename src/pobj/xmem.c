#include <stdlib.h>


#define member_sizeof(s,x)  (sizeof(((s *)NULL)->x))
#define member_offset(s,x)  ((int)&(((s *)NULL)->x))


#ifdef HAVE_XMEM_DETAIL
struct xmem {
    char *file;
    int line;
    int size;
    int dummy;  /* dummy field to infer alignment. */
};
# define XMEM_OFFSET  member_offset (struct xmem, dummy)
#endif /* HAVE_XMEM_DETAIL */


/* Default memory calls. */
void *(*g_memfunc_malloc) (size_t) = malloc;
void *(*g_memfunc_realloc) (void *, size_t) = realloc;
void (*g_memfunc_free) (void *) = free;


#ifdef HAVE_XMEM_DETAIL
void *
xmem_malloc (char *file, int line, size_t size)
{
    struct xmem *x;

    x = (struct xmem *) g_memfunc_malloc (size + XMEM_OFFSET);
    if (! x)
	return NULL;

    x->file = file;
    x->line = line;
    x->size = size;
    
    return (void *) ((char *) x + XMEM_OFFSET);
}

void *
xmem_realloc (char *file, int line, void *p, size_t size)
{
    struct xmem *x = (struct xmem *) ((char *) p - XMEM_OFFSET);

    x = (struct xmem *) g_memfunc_realloc (x, size + XMEM_OFFSET);
    if (! x)
	return NULL;

    x->file = file;
    x->line = line;
    x->size = size;

    return (void *) ((char *) x + XMEM_OFFSET);
}

void
xmem_free (void *p)
{
    struct xmem *x = (struct xmem *) ((char *) p - XMEM_OFFSET);

    g_memfunc_free (x);
}
#endif  /* HAVE_XMEM_DETAIL */


int
xmem_memfunc (void *(*memfunc_malloc) (size_t),
	      void *(*memfunc_realloc) (void *, size_t),
	      void (*memfunc_free) (void *))
{
    if (memfunc_malloc)
	g_memfunc_malloc = memfunc_malloc;
    if (memfunc_realloc)
	g_memfunc_realloc = memfunc_realloc;
    if (memfunc_free)
	g_memfunc_free = memfunc_free;

    return 0;
}


char *
xmem_get_file (void *p)
{
#ifdef HAVE_XMEM_DETAIL
    struct xmem *x = (struct xmem *) ((char *) p - XMEM_OFFSET);
    return x->file;
#else
    return NULL;
#endif /* HAVE_XMEM_DETAIL */
}

int
xmem_get_line (void *p)
{
#ifdef HAVE_XMEM_DETAIL
    struct xmem *x = (struct xmem *) ((char *) p - XMEM_OFFSET);
    return x->line;
#else
    return 0;
#endif /* HAVE_XMEM_DETAIL */
}

size_t
xmem_get_size (void *p)
{
#ifdef HAVE_XMEM_DETAIL
    struct xmem *x = (struct xmem *) ((char *) p - XMEM_OFFSET);
    return x->size;
#else
    return 0;
#endif /* HAVE_XMEM_DETAIL */
}
