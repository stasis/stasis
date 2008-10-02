#ifndef __POBJ_H
#define __POBJ_H

#define member_sizeof(s,x)  (sizeof(((s *)NULL)->x))
#define member_offset(s,x)  ((long)&(((s *)NULL)->x))


struct pobj_memfunc {
    void *(*malloc) (size_t);
    void *(*calloc) (size_t, size_t);
    void *(*realloc) (void *, size_t);
    void (*free) (void *);
};


/* Transactional markers. */
int pobj_start (void);
int pobj_end (void);


/* Object persistency. */
int pobj_persistify (void *);
int pobj_unpersistify (void *);
int pobj_is_persistent (void *);

/* Object allocation. */
#define POBJ_ALLOC_F_PERSIST  1
#define POBJ_ALLOC_F_ZERO     2
void *pobj_allocate (size_t, void *(*) (size_t), void (*) (void *), unsigned char);
#define pobj_malloc(s)  \
    pobj_allocate(s, NULL, NULL, POBJ_ALLOC_F_PERSIST)
#define pobj_malloc_transient(s)  \
    pobj_allocate(s, NULL, NULL, 0)
#define pobj_calloc(n,s)  \
    pobj_allocate((n)*(s), NULL, NULL, POBJ_ALLOC_F_PERSIST | POBJ_ALLOC_F_ZERO)
#define pobj_calloc_transient(n,s)  \
    pobj_allocate((n)*(s), NULL, NULL, POBJ_ALLOC_F_ZERO)
#define pobj_malloc_adhoc(s,a,d)  \
    pobj_allocate(s, a, d, POBJ_ALLOC_F_PERSIST)
#define pobj_malloc_transient_adhoc (s,a,d)  \
    pobj_allocate(s, a, d, 0)
#define pobj_calloc_adhoc (n,s,a,d)  \
    pobj_allocate((n)*(s), a, d, POBJ_ALLOC_F_PERSIST | POBJ_ALLOC_F_ZERO)
#define pobj_calloc_transient_adhoc (n,s,a,d)  \
    pobj_allocate((n)*(s), a, d, POBJ_ALLOC_F_ZERO)

/* Object dismissal. */
#define POBJ_DISMISS_F_DEALLOC  1
#define POBJ_DISMISS_F_RAW      2
void pobj_dismiss (void *, unsigned char);
#define pobj_free(p)  \
    pobj_dismiss((p), POBJ_DISMISS_F_DEALLOC)
#define pobj_finalize(p)  \
    pobj_dismiss((p), 0)
#define pobj_free_raw(p)  \
    pobj_dismiss((p), POBJ_DISMISS_F_DEALLOC | POBJ_DISMISS_F_RAW)
#define pobj_finalize_raw(p)  \
    pobj_dismiss((p), POBJ_DISMISS_F_RAW)


/* Object typing. */
int pobj_ref_mark_update (void *, void *, int);
#define pobj_ref_mark(o,f)        pobj_ref_mark_update(o,f,1)
#define pobj_ref_unmark(o,f)      pobj_ref_mark_update(o,f,0)
#define POBJ_REF_MARK(obj,fld)    pobj_ref_mark((obj), &((obj)->fld))
#define POBJ_REF_UNMARK(obj,fld)  pobj_ref_unmark((obj), &((obj)->fld))
int pobj_ref_typify (void *, int *);


/* Object modification. */
void *pobj_memcpy (void *, void *, void *, size_t);
void *pobj_memset (void *, void *, long, size_t);
int pobj_set_int (void *, int *, int);
int pobj_set_unsigned (void *, unsigned *, unsigned);
int pobj_set_long (void *, long *, long);
int pobj_set_unsigned_long (void *, unsigned long *, unsigned long);
int pobj_set_short (void *, short *, short);
int pobj_set_unsigned_short (void *, unsigned short *, unsigned short);
int pobj_set_char (void *, char *, char);
int pobj_set_unsigned_char (void *, unsigned char *, unsigned char);
int pobj_set_float (void *, float *, float);
int pobj_set_double (void *, double *, double);
int pobj_set_ref (void *, void *, void *);
#define POBJ_MEMCPY(obj,fld,data,len)  \
    pobj_memcpy((obj), &((obj)->fld), data, len)
#define POBJ_MEMSET(obj,fld,c,len)  \
    pobj_memset((obj), &((obj)->fld), c, len)
#define POBJ_SET_INT(obj,fld,data)  \
    pobj_set_int((obj), &((obj)->fld), data)
#define POBJ_SET_UNSIGNED(obj,fld,data)  \
    pobj_set_unsigned((obj), &((obj)->fld), data)
#define POBJ_SET_LONG(obj,fld,data)  \
    pobj_set_long((obj), &((obj)->fld), data)
#define POBJ_SET_UNSIGNED_LONG(obj,fld,data)  \
    pobj_set_unsigned_long((obj), &((obj)->fld), data)
#define POBJ_SET_SHORT(obj,fld,data)  \
    pobj_set_short((obj), &((obj)->fld), data)
#define POBJ_SET_UNSIGNED_SHORT(obj,fld,data)  \
    pobj_set_unsigned_short((obj), &((obj)->fld), data)
#define POBJ_SET_CHAR(obj,fld,data)  \
    pobj_set_char((obj), &((obj)->fld), data)
#define POBJ_SET_UNSIGNED_CHAR(obj,fld,data)  \
    pobj_set_unsigned_char((obj), &((obj)->fld), data)
#define POBJ_SET_FLOAT(obj,fld,data)  \
    pobj_set_float((obj), &((obj)->fld), data)
#define POBJ_SET_DOUBLE(obj,fld,data)  \
    pobj_set_double((obj), &((obj)->fld), data)
#define POBJ_SET_REF(obj,fld,ref)  \
    pobj_set_ref((obj), &((obj)->fld), ref)


/* Object persistent image update. */
int pobj_update_range (void *, void *, size_t);
#define pobj_update(obj)  \
    pobj_update_range((obj), NULL, 0)
#define POBJ_UPDATE_FLD(obj,fld)  \
    pobj_update_range((obj), &((obj)->fld), sizeof((obj)->fld))
int pobj_update_recursive (void *, int);


/* Static reference update. */
int pobj_static_set_ref (void *, void *);
int pobj_static_update_ref (void *);


/* Init/shutdown. */
int pobj_init (struct pobj_memfunc *, struct pobj_memfunc *);
int pobj_shutdown (void);

#endif /* __POBJ_H */

