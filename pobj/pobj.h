#ifndef __POBJ_H
#define __POBJ_H

#define member_sizeof(s,x)  (sizeof(((s *)NULL)->x))
#define member_offset(s,x)  ((int)&(((s *)NULL)->x))


/* Note: the alignment and pobj header info was placed here in order
 * to allow fast IS_PERSISTENT check. */

/* Architecture specific word size and alignment. */
#define WORDSIZE  sizeof(int)
#define WORDBITS  (WORDSIZE * 8)
#define ALIGN(s)  ((size_t) (((s) + (WORDSIZE - 1)) / WORDSIZE) * WORDSIZE)

/* Persistent object control block (header). */
struct pobj {
    size_t size;
    int type_index;
    int rep_index;
};
#define POBJ_HEADER_SIZE  sizeof(struct pobj)
#define IS_PERSISTENT(p)  \
  (((struct pobj *)(((char *) p) - ALIGN(POBJ_HEADER_SIZE)))->rep_index >= 0)


struct pobj_memfunc {
    void *(*malloc) (size_t);
    void *(*calloc) (size_t, size_t);
    void *(*realloc) (void *, size_t);
    void (*free) (void *);
};


/* Transactional markers. */
int pobj_start (void);
int pobj_end (void);

/* Persistency control. */
int pobj_persistify (void *);
int pobj_unpersistify (void *);

/* Memory management calls. */
void *pobj_malloc (size_t);
void *pobj_malloc_transient (size_t);
void *pobj_calloc (size_t, size_t);
void *pobj_calloc_transient (size_t, size_t);
void pobj_free (void *);

/* GC-oriented memory management calls (experts only!). */
void *pobj_malloc_adhoc (size_t, void *(*) (size_t), void (*) (void *));
void *pobj_malloc_transient_adhoc (size_t, void *(*) (size_t), void (*) (void *));
void *pobj_calloc_adhoc (size_t, size_t, void *(*) (size_t), void (*) (void *));
void *pobj_calloc_transient_adhoc (size_t, size_t, void *(*) (size_t),
				   void (*) (void *));
void pobj_finalize (void *);
void pobj_finalize_raw (void *);
void pobj_free_raw (void *);


int pobj_ref_flag (void *, void *);
int pobj_ref_unflag (void *, void *);
#define POBJ_REF_FLAG(obj,fld)         pobj_ref_flag((obj), &((obj)->fld))
#define POBJ_REF_UNFLAG(obj,fld)       pobj_ref_unflag((obj), &((obj)->fld))
int pobj_ref_typify (void *, int *);

void *pobj_memcpy (void *, void *, void *, size_t);
void *pobj_memset (void *, void *, int, size_t);
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
#define POBJ_MEMCPY(obj,fld,data,len)  pobj_memcpy((obj), &((obj)->fld), data, len)
#define POBJ_MEMSET(obj,fld,c,len)     pobj_memset((obj), &((obj)->fld), c, len)
#define POBJ_SET_INT(obj,fld,data)     pobj_set_int((obj), &((obj)->fld), data)
#define POBJ_SET_UNSIGNED(obj,fld,data)  \
  pobj_set_unsigned((obj), &((obj)->fld), data)
#define POBJ_SET_LONG(obj,fld,data)    pobj_set_long((obj), &((obj)->fld), data)
#define POBJ_SET_UNSIGNED_LONG(obj,fld,data)  \
  pobj_set_unsigned_long((obj), &((obj)->fld), data)
#define POBJ_SET_SHORT(obj,fld,data)   pobj_set_short((obj), &((obj)->fld), data)
#define POBJ_SET_UNSIGNED_SHORT(obj,fld,data)  \
  pobj_set_unsigned_short((obj), &((obj)->fld), data)
#define POBJ_SET_CHAR(obj,fld,data)    pobj_set_char((obj), &((obj)->fld), data)
#define POBJ_SET_UNSIGNED_CHAR(obj,fld,data)  \
  pobj_set_unsigned_char((obj), &((obj)->fld), data)
#define POBJ_SET_FLOAT(obj,fld,data)   pobj_set_float((obj), &((obj)->fld), data)
#define POBJ_SET_DOUBLE(obj,fld,data)  pobj_set_double((obj), &((obj)->fld), data)
#define POBJ_SET_REF(obj,fld,ref)      pobj_set_ref((obj), &((obj)->fld), ref)

int pobj_update_range (void *, void *, size_t);
#define POBJ_UPDATE_FLD(obj,fld)  \
  pobj_update_range((obj), &((obj)->fld), sizeof((obj)->fld))
#define POBJ_UPDATE(obj)               pobj_update_range ((obj), NULL, 0)
int pobj_update_recursive (void *, int);

int pobj_static_set_ref (void *, void *);
int pobj_static_update_ref (void *);

int pobj_init (struct pobj_memfunc *, struct pobj_memfunc *);
int pobj_shutdown (void);

#endif /* __POBJ_H */

