#ifndef __XMEM_H
#define __XMEM_H

enum {
    /* General purpose identifiers. */
    XMEM_TMP = 0,
    XMEM_STR,
    
    /* Dedicated object identifiers. */
    XMEM_POBJ,
    XMEM_HASH,
    
    /* Teminator (don't use). */
    XMEM_MAX
};

#define XMALLOC(t,s) xmem_malloc (t, __FILE__, __LINE__, s)
#define XFREE(t,p)   xmem_free (t, p)
#ifdef HAVE_XMEM_DETAIL
#define XFREE_ANY(p) xmem_free (XMEM_MAX, p)
#endif /* HAVE_XMEM_DETAIL */

void *xmem_malloc (int, char *, int, size_t);
void xmem_free (int, void *);
int xmem_memfunc (void *(*) (size_t), void (*) (void *));
int xmem_obj_mtype (void *);
char *xmem_obj_file (void *);
int xmem_obj_line (void *);
size_t xmem_obj_size (void *);
unsigned long xmem_stat_count (int);
unsigned long xmem_stat_size (int);
unsigned long xmem_stat_alloc_list (int, void (*cb) (char *, int, size_t, void *));

#endif /* __XMEM_H */

