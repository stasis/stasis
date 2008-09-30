#ifndef __XMEM_H
#define __XMEM_H

#ifdef HAVE_XMEM_DETAIL
void *xmem_malloc (char *, int, size_t);
void *xmem_realloc (char *, int, void *, size_t);
void xmem_free (void *);
# define XMALLOC(s)     xmem_malloc (__FILE__, __LINE__, s)
# define XREALLOC(p,s)  xmem_malloc (__FILE__, __LINE__, p, s)
# define XFREE(p)       xmem_free (p)
#else
extern void *(*g_memfunc_malloc) (size_t);
extern void *(*g_memfunc_realloc) (void *, size_t);
extern void (*g_memfunc_free) (void *);
# define XMALLOC(s)     g_memfunc_malloc(s)
# define XREALLOC(p,s)  g_memfunc_realloc(p,s)
# define XFREE(p)       g_memfunc_free(p)
#endif /* HAVE_XMEM_DETAIL */

int xmem_memfunc (void *(*) (size_t), void *(*) (void *, size_t), void (*) (void *));
char *xmem_get_file (void *);
int xmem_get_line (void *);
size_t xmem_get_size (void *);

#endif /* __XMEM_H */

