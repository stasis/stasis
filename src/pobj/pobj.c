#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <lladd/transactional.h>
#include <pobj/pobj.h>
#include "hash.h"
#include "debug.h"
#include "xmem.h"


/* FIXME: move switches to make system. */
#define HAVE_IMPLICIT_TYPES


/* Global TODO:
 *
 * - extend to using a user-provided memory manager calls: malloc, calloc,
 *   realloc, and free, all with default arguments plus an additional
 *   user-provided data argument (void *).
 */

#define WORDSIZE  sizeof(int)
#define WORDBITS  (WORDSIZE * 8)
#define ALIGN(s)  ((size_t) (((s) + (WORDSIZE - 1)) / WORDSIZE) * WORDSIZE)

/* Convert an object pointer into a proper hash key (2 lsb's are zero
 * due to alignment, hence make hashing quite redundant...). */
#define OBJ2KEY(obj)  ((unsigned long) (obj) >> 2)


/* Persistent object control block (header). */
struct pobj {
#if 0
    void *objid;  /* FIXME: probably a redundant field...? */
#endif
    size_t size;
    int rep_index;
#if 0
    recordid rid;
#endif
};
#define POBJ_HEADER_SIZE  sizeof(struct pobj)

#define POBJ_NREFS(s)            ((s) / WORDSIZE)
#define POBJ_REFFLAGS_OFFSET(s)  (ALIGN(POBJ_HEADER_SIZE) + ALIGN(s))
#define POBJ_REFFLAGS_SIZE(s)    ((size_t) ALIGN((POBJ_NREFS(s) + 7) / 8))
#define POBJ_SIZE(s)             (POBJ_REFFLAGS_OFFSET(s) + POBJ_REFFLAGS_SIZE(s))

#define OBJ2POBJ(p)  ((struct pobj *)(((char *) p) - ALIGN(POBJ_HEADER_SIZE)))
#define POBJ2OBJ(p)  ((void *)(((char *) p) + ALIGN(POBJ_HEADER_SIZE)))
#define POBJ2REFS(p) ((unsigned long *)(((char *) p) + POBJ_REFFLAGS_OFFSET((p)->size)))


/* Limits.
 * TODO: use dynamic limits instead (switch to growable structures). */
#define STATIC_REP_MAX_EXP        8
#define STATIC_REP_MAX            (1 << STATIC_REP_MAX_EXP)
#define STATIC_REP_HASH_NBUCKEXP  6
#define POBJ_REP_MAX_EXP          14
#define POBJ_REP_MAX              (1 << POBJ_REP_MAX_EXP)

#define CONVERT_HASH_NBUCKEXP     18
#define UPDATE_HASH_NBUCKEXP      18
#define UPDATE_QUEUE_MAX_EXP      10
#define UPDATE_QUEUE_MAX          (1 << UPDATE_QUEUE_MAX_EXP)

#define TMPBUF_INIT_SIZE          1024
#define TMPBUF_GROW_FACTOR        2


/* Persistent object repository. */
struct pobj_rep_list_item {
    int prev_index;
    int next_index;
    void *objid;
    size_t size;
    recordid rid;
};
/* TODO: switch to growable data structure. */
static struct pobj_rep_list_item g_pobj_rep_list[POBJ_REP_MAX];

struct pobj_rep {
    recordid list_rid;
    int vacant_head;
    int occupied_head;
};
static struct pobj_rep g_pobj_rep;



/* Persistent static reference variable binding. */
struct static_rep_list_item {
    int prev_index;
    int next_index;
    void **static_ptr;
    void *objid;
};
/* TODO: switch to a growable data structure. */
static struct static_rep_list_item g_static_rep_list[STATIC_REP_MAX];

struct static_rep {
    recordid hash_rid;
    recordid list_rid;
    int vacant_head;
    int occupied_head;
};
static struct static_rep g_static_rep;

/* Persistent static references fast lookup table. */
/* TODO: switch to a growable data structure. */
static HASH_DECLARE (g_static_rep_hash, STATIC_REP_HASH_NBUCKEXP);


/* Boot record. */
struct boot_record {
    recordid pobj_rep_rid;
    recordid static_rep_rid;
};

static struct boot_record g_boot;
static recordid g_boot_rid = { 1, 0, sizeof (struct boot_record) };


/* Memory manager calls: initialize to libc defaults. */
struct pobj_memfunc g_ext_memfunc = { malloc, calloc, realloc, free };
struct pobj_memfunc g_int_memfunc = { malloc, calloc, realloc, free };


/* Threaded synchronization and specifics. */
static pthread_mutex_t g_pobj_rep_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t g_static_rep_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_key_t g_active_xid_key;
static pthread_key_t g_active_nested_key;
static int g_is_init = 0;


int
pobj_start (void)
{
    int active_xid;
    int active_nested;
    
    if (! g_is_init)
	return -1;

    active_xid = (int) pthread_getspecific (g_active_xid_key) - 1;
    if (active_xid < 0) {
	active_xid = Tbegin ();
	if (active_xid < 0
	    || pthread_setspecific (g_active_xid_key, (void *) (active_xid + 1)))
	{
	    if (active_xid >= 0)
		Tabort (active_xid);
	    return -1;
	}
    }
    else {
	active_nested = (int) pthread_getspecific (g_active_nested_key);
	active_nested++;
	if (pthread_setspecific (g_active_nested_key, (void *) active_nested))
	    return -1;
    }

    return active_xid;
}

int
pobj_end (void)
{
    int active_xid;
    int active_nested;

    if (! g_is_init)
	return -1;

    active_nested = (int) pthread_getspecific (g_active_nested_key);
    if (active_nested) {
	active_nested--;
	if (pthread_setspecific (g_active_nested_key, (void *) active_nested))
	    return -1;
    }
    else {
	active_xid = (int) pthread_getspecific (g_active_xid_key) - 1;
	if (active_xid >= 0) {
	    if (pthread_setspecific (g_active_xid_key, NULL))
		return -1;
	    Tcommit (active_xid);
	}
	else
	    return -1;  /* Attempt to close a non-open transaction. */
    }

    return 0;
}



static void *
pobj_allocate (size_t size, void *(*alloc) (size_t), void (*dealloc) (void *),
	       int persist, int zero)
{
    struct pobj *p;
    void *obj;
    size_t pobj_size;
    recordid tmp_rid;
    int next_vacant, next_occupied;
    int xid;
    int i;

    if (! g_is_init)
	return NULL;

    if (! alloc)
	alloc = g_ext_memfunc.malloc;
    if (! dealloc)
	dealloc = g_ext_memfunc.free;

    /* Allocate augmented object. */
    pobj_size = POBJ_SIZE (size);
    p = (struct pobj *) alloc (pobj_size);
    if (! p) {
	debug ("error: memory allocation failed");
	return NULL;
    }
    obj = POBJ2OBJ (p);

    /* Zero allocated buffer as necessary. */
    if (zero)
	memset (obj, 0, size);

    debug ("allocated object at %p (%p) size %d (%d+%d+%d=%d)",
	   obj, (void *) p, size, ALIGN (POBJ_HEADER_SIZE), ALIGN (size),
       	   POBJ_REFFLAGS_SIZE (size), pobj_size);

    if (persist) {
	if ((xid = pobj_start ()) < 0) {
	    debug ("error: begin transaction failed, allocation aborted");
	    dealloc (p);
	    return NULL;
	}

	/* Lock and verify initialization. */
	debug ("locking object repository");
	pthread_mutex_lock (&g_pobj_rep_mutex);

	/* Update persistent object repository. */
	i = g_pobj_rep.vacant_head;
	if (i >= 0) {
	    /* Untie from vacant list (head). */
	    next_vacant = g_pobj_rep_list[i].next_index;
	    g_pobj_rep.vacant_head = next_vacant;
	    if (next_vacant >= 0)
		g_pobj_rep_list[next_vacant].prev_index = -1;

	    /* Tie into occupied list. */
	    next_occupied = g_pobj_rep.occupied_head;
	    if (next_occupied >= 0)
		g_pobj_rep_list[next_occupied].prev_index = i;
	    g_pobj_rep_list[i].next_index = next_occupied;
	    g_pobj_rep.occupied_head = i;

	    /* Reflect changes to persistent image of repository. */
	    Tset (xid, g_boot.pobj_rep_rid, &g_pobj_rep);
	    
	    tmp_rid.page = g_pobj_rep.list_rid.page;
	    tmp_rid.size = sizeof (struct pobj_rep_list_item);
	    if (next_vacant >= 0) {
		tmp_rid.slot = next_vacant;
		Tset (xid, tmp_rid, g_pobj_rep_list + next_vacant);
	    }
	    if (next_occupied >= 0) {
		tmp_rid.slot = next_occupied;
		Tset (xid, tmp_rid, g_pobj_rep_list + next_occupied);
	    }

	    /* Set object info, attach new record. */
	    debug ("updating repository entry and attaching new record, i=%d", i);
	    g_pobj_rep_list[i].objid = obj;
	    g_pobj_rep_list[i].size = size;
	    g_pobj_rep_list[i].rid = Talloc (xid, pobj_size);
	    tmp_rid.slot = i;
	    Tset (xid, tmp_rid, g_pobj_rep_list + i);
	}

	debug ("unlocking object repository");
	pthread_mutex_unlock (&g_pobj_rep_mutex);

	if (i < 0) {
	    debug ("error: persistent object repository exhausted, allocation aborted");
	    pobj_end ();
	    dealloc (p);
	    return NULL;
	}

	/* Update control block info. */
#if 0
	p->objid = obj;
#endif
	p->size = size;
	p->rep_index = i;
	memset (POBJ2REFS (p), 0, POBJ_REFFLAGS_SIZE (size));

	/* Update persistent image. */
	Tset (xid, g_pobj_rep_list[p->rep_index].rid, p);

	pobj_end ();
    }
    else {
	/* Update transient block info. */
	p->size = size;
	p->rep_index = -1;
    }

    return obj;
}

void *
pobj_malloc (size_t size)
{
    return pobj_allocate (size, NULL, NULL, 1, 0);
}

void *
pobj_malloc_transient (size_t size)
{
    return pobj_allocate (size, NULL, NULL, 0, 0);
}

void *
pobj_calloc (size_t n, size_t size)
{
    return pobj_allocate (n * size, NULL, NULL, 1, 1);
}

void *
pobj_calloc_transient (size_t n, size_t size)
{
    return pobj_allocate (n * size, NULL, NULL, 0, 1);
}


void *
pobj_malloc_adhoc (size_t size, void *(*alloc) (size_t), void (*dealloc) (void *))
{
    return pobj_allocate (size, alloc, dealloc, 1, 0);
}

void *
pobj_malloc_transient_adhoc (size_t size, void *(*alloc) (size_t),
			     void (*dealloc) (void *))
{
    return pobj_allocate (size, alloc, dealloc, 0, 0);
}

void *
pobj_calloc_adhoc (size_t n, size_t size, void *(*alloc) (size_t),
		   void (*dealloc) (void *))
{
    return pobj_allocate (n * size, alloc, dealloc, 1, 1);
}

void *
pobj_calloc_transient_adhoc (size_t n, size_t size, void *(*alloc) (size_t),
			     void (*dealloc) (void *))
{
    return pobj_allocate (n * size, alloc, dealloc, 0, 1);
}


static void
pobj_free_finalize (void *obj, int deallocate, int raw)
{
    struct pobj *p = (raw ? obj : OBJ2POBJ (obj));
    int next_vacant, next_occupied, prev_occupied;
    recordid tmp_rid;
    int xid;
    int i;

    if (! g_is_init)
	return;

    i = p->rep_index;

    /* Destruct persistent image. */
    if (i >= 0) {
	if ((xid = pobj_start ()) < 0) {
	    debug ("error: begin transaction failed, deallocation aborted");
	    return;
	}
	
	/* Update persistent object repository. */
	debug ("locking object repository");
	pthread_mutex_lock (&g_pobj_rep_mutex);

	/* Untie from occupied list (not necessarily head). */
	next_occupied = g_pobj_rep_list[i].next_index;
	prev_occupied = g_pobj_rep_list[i].prev_index;
	if (prev_occupied >= 0)
	    g_pobj_rep_list[prev_occupied].next_index = next_occupied;
	else
	    g_pobj_rep.occupied_head = next_occupied;
	if (next_occupied >= 0)
	    g_pobj_rep_list[next_occupied].prev_index = prev_occupied;

	/* Tie into vacant list. */
	next_vacant = g_pobj_rep.vacant_head;
	if (next_vacant >= 0)
	    g_pobj_rep_list[next_vacant].prev_index = i;
	g_pobj_rep_list[i].next_index = next_vacant;
	g_pobj_rep_list[i].prev_index = -1;
	g_pobj_rep.vacant_head = i;

	/* Reflect changes to persistent image of repository. */
	Tset (xid, g_boot.pobj_rep_rid, &g_pobj_rep);

	tmp_rid.page = g_pobj_rep.list_rid.page;
	tmp_rid.size = sizeof (struct pobj_rep_list_item);
	if (next_occupied >= 0) {
	    tmp_rid.slot = next_occupied;
	    Tset (xid, tmp_rid, g_pobj_rep_list + next_occupied);
	}
	if (prev_occupied >= 0) {
	    tmp_rid.slot = prev_occupied;
	    Tset (xid, tmp_rid, g_pobj_rep_list + prev_occupied);
	}
	if (next_vacant >= 0) {
	    tmp_rid.slot = next_vacant;
	    Tset (xid, tmp_rid, g_pobj_rep_list + next_vacant);
	}
	tmp_rid.slot = i;
	Tset (xid, tmp_rid, g_pobj_rep_list + i);

	debug ("unlocking object repository");
	pthread_mutex_unlock (&g_pobj_rep_mutex);

	/* Detach persistent record. */
	debug ("detaching record");
	Tdealloc (xid, g_pobj_rep_list[i].rid);

	pobj_end ();
    }

    /* Deallocate augmented memory object, or switch to transient mode. */
    if (deallocate) {
	debug ("deallocating memory");
	g_ext_memfunc.free (p);
    }
    else
	p->rep_index = -1;
}

void
pobj_free (void *obj)
{
    pobj_free_finalize (obj, 1, 0);
}

void
pobj_finalize (void *obj)
{
    pobj_free_finalize (obj, 0, 0);
}

void
pobj_free_raw (void *obj)
{
    pobj_free_finalize (obj, 1, 1);
}

void
pobj_finalize_raw (void *obj)
{
    pobj_free_finalize (obj, 0, 1);
}


/* Note: behavior of type enforcement is not symmetrical, in the sense that
 * a type mismatch is treated depending on the mismatch direction -- if
 * a field is claimed to be a reference but is not flagged, then the flag
 * is re-adjusted (set) to reflect its being a reference; on the contrary, if
 * a field is not a reference yet is flagged, then a type mismatch occurs.
 *
 * Return values:
 *  0  okay
 *  1  type mismatch
 *  2  flags adjusted (requires reflection to persistent image!)
 * -1  error
 */
#ifdef HAVE_IMPLICIT_TYPES
static int
pobj_ref_type_enforce (void *obj, void *fld, size_t len, int is_ref)
{
    void **fld_aligned;
    struct pobj *p = OBJ2POBJ (obj);
    int ref_offset;
    int nrefs;
    int flags_offset;
    int bit;
    unsigned long *flags_ptr;
    unsigned long flags, mask;
    size_t len_aligned;
    int ret;

    /* Safety check. */
    /* TODO: is this worthwhile? */
    if ((char *) obj > (char *) fld
	|| (char *) obj + p->size < (char *) fld + len)
    {
	debug ("error: field(s) out of object bounds, aborted");
	return -1;
    }
    
    /* Align pointer and length to word boundaries. */
    fld_aligned = (void **) (((unsigned long) fld / WORDSIZE) * WORDSIZE);
    len_aligned = len + ((char *) fld - (char *) fld_aligned);
    nrefs = (len_aligned + (WORDSIZE - 1)) / WORDSIZE;

    /* Update reference flags to allow reference adjustment during recovery. */
    /* TODO: do we want to protect manipulation of object meta-data? Basically,
     * it should be protected, although changes are monotonic, but that will
     * definitely induce some overhead... we could also revert to a single-byte
     * flag, instead of a bit-grained one, thus ensuring no interference
     * (is this a correct observation?). */
    ref_offset = fld_aligned - (void **) obj;
    flags_offset = ref_offset / WORDBITS;
    flags_ptr = POBJ2REFS (p) + flags_offset;
    flags = *flags_ptr;
    bit = ref_offset - flags_offset * WORDBITS;
    mask = ((unsigned long) 1 << bit);
    ret = 0;
    /* TODO: optimize this loop such that sequences of flags (up to the limit
     * of a word bounds) are all checked/updated together. */
    while (nrefs > 0) {
	if (flags & mask) {
	    if (! is_ref)
		return 1;  /* Flagged but non-ref, mismatch. */
	}
	else {
	    if (is_ref) {
		flags = *flags_ptr = flags | mask;  /* Non-flagged ref, flag it! */
		ret = 2;
	    }
	}
	
	nrefs--;
	if (++bit == WORDBITS) {
	    flags = *(++flags_ptr);
	    bit = 0;
	    mask = 1;
	}
	else
	    mask <<= 1;
    }

    return ret;
}
#endif /* HAVE_IMPLICIT_TYPES */


static int
pobj_ref_flag_update (void *obj, void **fld, int set)
{
    struct pobj *p = OBJ2POBJ (obj);
    int ref_offset;
    int flags_offset;
    int bit;
    unsigned long *flags_ptr;
    unsigned long flags, mask;
    int xid;
    int ret;

    if (p->rep_index < 0) {
	debug ("error: object is non-persistent");
	return -1;
    }
    
    /* Bound check. */
    /* TODO: is this worthwhile? */
    if ((char *) obj > (char *) fld
	|| (char *) obj + p->size < (char *) fld + sizeof (void *))
    {
	debug ("error: field out of object bounds, aborted");
	return -1;
    }
    if (((unsigned long) fld / WORDSIZE) * WORDSIZE != (unsigned long) fld) {
	debug ("error: reference field is not aligned, aborted");
	return -1;
    }

    if ((xid = pobj_start ()) < 0) {
	debug ("error: begin transaction failed, aborted");
	return -1;
    }
    
    /* Update reference flags to allow reference adjustment during recovery. */
    /* TODO: do we want to protect manipulation of object meta-data? Basically,
     * it should be protected, although changes are monotonic, but that will
     * definitely induce some overhead... we could also revert to a single-byte
     * flag, instead of a bit-grained one, thus ensuring no interference
     * (is this a correct observation?). */
    ret = 0;
    ref_offset = fld - (void **) obj;
    flags_offset = ref_offset / WORDBITS;
    bit = ref_offset - flags_offset * WORDBITS;
    flags_ptr = POBJ2REFS (p) + flags_offset;
    flags = *flags_ptr;
    mask = ((unsigned long) 1 << bit);
    if (set) {
	if (! (flags & mask))
	    ret = 1;
	*flags_ptr = flags | mask;
    }
    else {
	if (flags & mask)
	    ret = 1;
	*flags_ptr = flags & ~mask;
    }

    /* Update corresponding record. */
    /* TODO: switch to set_range update. */
    Tset (xid, g_pobj_rep_list[p->rep_index].rid, p);

    pobj_end ();

    return ret;
}

int
pobj_ref_flag (void *obj, void *fld)
{
    return pobj_ref_flag_update (obj, (void **) fld, 1);
}

int
pobj_ref_unflag (void *obj, void *fld)
{
    return pobj_ref_flag_update (obj, (void **) fld, 0);
}

int
pobj_ref_typify (void *obj, int *reflist)
{
    struct pobj *p = OBJ2POBJ (obj);
    int nrefs;
    int ref_offset;
    int flags_offset;
    int bit;
    unsigned long *flags_ptr;
    int xid;
    int count;

    if (p->rep_index < 0) {
	debug ("error: object is non-persistent");
	return -1;
    }

    if (! reflist) {
	debug ("error: null reference list provided, aborted");
	return -1;
    }
    
    if ((xid = pobj_start ()) < 0) {
	debug ("error: begin transaction failed, aborted");
	return -1;
    }
    
    debug ("unsetting reference flags (%d)", POBJ_NREFS (p->size));

    /* Unset all reference flags. */
    nrefs = POBJ_NREFS (p->size);
    flags_ptr = POBJ2REFS (p);
    while (nrefs > 0) {
	*flags_ptr++ = 0;
	nrefs -= WORDBITS;
    }

    debug ("traversing reference offsets list...");

    /* Traverse offsets list, set per-reference flag. */
    count = 0;
    while ((ref_offset = *reflist++) >= 0) {
	count++;
	ref_offset /= WORDSIZE;
	debug (" offset=%d", ref_offset);

	/* Safety check. */
	if (ref_offset >= p->size) {
	    debug ("warning: reference offset out-of-bounds, ignored");
	    continue;
	}

	flags_offset = ref_offset / WORDBITS;
	bit = ref_offset - (flags_offset * WORDBITS);
	flags_ptr = POBJ2REFS (p) + flags_offset;
	*flags_ptr = *flags_ptr | ((unsigned long) 1 << bit);
    }

    debug ("...done (%d total)", count);

    /* Update corresponding record. */
    /* TODO: switch to set_range update. */
    Tset (xid, g_pobj_rep_list[p->rep_index].rid, p);

    pobj_end ();

    return 0;
}

       
static int
pobj_memcpy_typed (void *obj, void *fld, void *data, size_t len,
		   int is_typed, int is_ref)
{
    struct pobj *p = OBJ2POBJ (obj);
#ifdef HAVE_IMPLICIT_TYPES
    int is_changed = 0;
#endif /* HAVE_IMPLICIT_TYPES */
    int xid;

    if (p->rep_index < 0) {
	debug ("error: object is non-persistent");
	return -1;
    }

    /* Safety check. */
    /* TODO: is this worthwhile? */
    if ((char *) obj > (char *) fld
	|| (char *) obj + p->size < (char *) fld + len)
    {
	debug ("error: field out of object bounds, aborted");
	return -1;
    }

#ifdef HAVE_IMPLICIT_TYPES
    if (is_typed) {
	switch (pobj_ref_type_enforce (obj, fld, len, is_ref)) {
	case -1:
	    debug ("error: type check failed");
	    return -1;
	case 1:
	    debug ("error: type conflict, aborted");
	    return -1;
	case 2:
	    /* Flags have been adjusted, don't forget to reflect
	     * to persistent image. */
	    is_changed = 1;
	    break;
	}
    }
#endif /* HAVE_IMPLICIT_TYPES */

    if ((xid = pobj_start ()) < 0) {
	debug ("error: begin transaction failed, aborted");
    	return -1;
    }
    
    /* Update memory object field. */
    memcpy (fld, data, len);

    /* Update corresponding record. */
    /* TODO: switch to set_range update; not the is_changed field! */
    Tset (xid, g_pobj_rep_list[p->rep_index].rid, p);

    pobj_end ();

    return 0;
}


void *
pobj_memcpy (void *obj, void *fld, void *data, size_t len)
{
    if (pobj_memcpy_typed (obj, fld, data, len, 0, 0) < 0)
	return NULL;
    return obj;
}

#define POBJ_SET_NONREF(name,type)  \
    int name (void *obj, type *fld, type data) {  \
	return pobj_memcpy_typed (obj, fld, &data, sizeof (data), 1, 0);  \
    }
POBJ_SET_NONREF (pobj_set_int, int)
POBJ_SET_NONREF (pobj_set_unsigned, unsigned)
POBJ_SET_NONREF (pobj_set_long, long)
POBJ_SET_NONREF (pobj_set_unsigned_long, unsigned long)
POBJ_SET_NONREF (pobj_set_short, short)
POBJ_SET_NONREF (pobj_set_unsigned_short, unsigned short)
POBJ_SET_NONREF (pobj_set_char, char)
POBJ_SET_NONREF (pobj_set_unsigned_char, unsigned char)
POBJ_SET_NONREF (pobj_set_float, float)
POBJ_SET_NONREF (pobj_set_double, double)

/* Note: we use void * instead of void ** as the fld pointer, in order
 * to avoid redundant (yet justified) type warnings. */
int
pobj_set_ref (void *obj, void *fld, void *ref)
{
    /* Verify alignment. */
    if (((unsigned long) fld / WORDSIZE) * WORDSIZE != (unsigned long) fld) {
	debug ("error: reference field not aligned, aborted");
	return -1;
    }
	
    return pobj_memcpy_typed (obj, fld, &ref, sizeof (ref), 1, 1);
}



int
pobj_update (void *obj)
{
    struct pobj *p = OBJ2POBJ (obj);
    int xid;

    if (p->rep_index < 0) {
	debug ("error: object is non-persistent");
	return -1;
    }

    if ((xid = pobj_start ()) < 0) {
	debug ("error: begin transaction failed, aborted");
	return -1;
    }
    
    /* Update corresponding record. */
    Tset (xid, g_pobj_rep_list[p->rep_index].rid, p);

    pobj_end ();

    return 0;
}


int
pobj_update_recursive (void *obj)
{
    void *pobj_update_queue[UPDATE_QUEUE_MAX];
    HASH_DECLARE (pobj_update_hash, UPDATE_HASH_NBUCKEXP);
    int q_head, q_tail;
    void *tmp;
    size_t tmp_size = TMPBUF_INIT_SIZE;
    struct pobj *p;
    int xid;
    int processed, enqueued;
    int nrefs;
    unsigned long *flags_ptr;
    unsigned long flags;
    unsigned long mask;
    int bit, complement;
    void **ref;
    void *next;
    int ret;

    /* Allocate temporary (growable) buffer. */
    tmp = XMALLOC (XMEM_TMP, tmp_size);
    if (! tmp) {
	debug ("error: allocation of temporary buffer failed");
	return -1;
    }

    if ((xid = pobj_start ()) < 0) {
	debug ("error: begin transaction failed, aborted");
	XFREE (XMEM_TMP, tmp);
	return -1;
    }

    /* Initialize visited objects table. */
    hash_init (pobj_update_hash, UPDATE_HASH_NBUCKEXP);

    /* Enqueue root object. */
    pobj_update_queue[0] = obj;
    q_head = 0;
    q_tail = 1;

    debug ("traversing reachable objects...");

    /* Iterate on objects in work queue. */
    processed = 0;
    ret = 0;
    while (q_head != q_tail) {
	obj = pobj_update_queue[q_head++];
	if (q_head == UPDATE_QUEUE_MAX)
	    q_head = 0;
	p = OBJ2POBJ (obj);
	
	/* Mark visited. */
	hash_insert (pobj_update_hash, UPDATE_HASH_NBUCKEXP,
		     OBJ2KEY (obj), 1);
	
	/* Don't process a non-persistent object. */
	if (p->rep_index < 1)
	    continue;
	    
	processed++;

	/* Grow temporary buffer as necessary. */
	if (POBJ_SIZE (p->size) > tmp_size) {
	    XFREE (XMEM_TMP, tmp);
	    tmp_size *= TMPBUF_GROW_FACTOR;
	    tmp = XMALLOC (XMEM_TMP, tmp_size);
	    if (! tmp) {
		debug ("error: allocation of temporary buffer failed");
		ret = -1;
		break;
	    }
	}
	
	/* Read persistent copy into temporary buffer and compare. */
	Tread (xid, g_pobj_rep_list[p->rep_index].rid, tmp);
	if (memcmp (p, tmp, p->size)) {
	    debug (" processing %p (%p): object changed, updating...",
		   obj, (void *) p);

	    /* Update persistent image. */
	    Tset (xid, g_pobj_rep_list[p->rep_index].rid, p);

	    /* Enqueue successors, if not yet processed. */
	    ref = (void **) obj;
	    nrefs = POBJ_NREFS (p->size);
	    flags_ptr = POBJ2REFS (p);
	    flags = *flags_ptr;
	    bit = 0;
	    mask = 1;
	    enqueued = 0;
	    while (nrefs > 0) {
		if (flags & mask) {
		    next = *ref;

		    if (next
			&& ! hash_lookup (pobj_update_hash, UPDATE_HASH_NBUCKEXP,
			   		  OBJ2KEY (next)))
		    {
			debug ("  enqueuing %p (%p)", next, (void *) OBJ2POBJ (next));

			pobj_update_queue[q_tail++] = next;
			if (q_tail == UPDATE_QUEUE_MAX)
			    q_tail = 0;
			enqueued++;
		    }
		}

		/* Advance to next potential reference. */
		ref++;
		nrefs--;
		if (++bit == WORDBITS) {
		    flags = *(++flags_ptr);
		    bit = 0;
		    mask = 1;
		}
		else
		    mask <<= 1;

		/* Skip word-full of flags, if flags suffix indicates no references. */
		while (nrefs > 0 && flags == (flags & (mask - 1))) {
		    complement = WORDBITS - bit;
		    ref += complement;
		    nrefs -= complement;
		    flags = *(++flags_ptr);

		    if (bit) {
			bit = 0;
			mask = 1;
		    }
		}
	    }

	    debug (" ...done (%d enqueued)", enqueued);
	}
	else
	    debug (" processing %p (%p): object unchanged", obj, (void *) p);
    }

    pobj_end ();

    if (ret < 0)
	debug ("...interrupted");
    else
	debug ("...done");

    hash_close (pobj_update_hash, UPDATE_HASH_NBUCKEXP);
    if (tmp)
	XFREE (XMEM_TMP, tmp);

    return ret;
}



static int
pobj_static_set_update_ref (void *static_tmp_ptr, void *obj, int is_set)
{
    void **static_ptr = (void **) static_tmp_ptr;
    int next_vacant, next_occupied;
    recordid tmp_rid;
    int xid;
    int i;

    if ((xid = pobj_start ()) < 0) {
	debug ("error: begin transaction failed, aborted");
	return -1;
    }
    
    /* Don't use given object pointer, read static pointer instead
     * (this implies the "update" behavior). */
    if (! is_set)
	obj = *static_ptr;

    /* Update static reference repository. */
    debug ("locking static reference repository");
    pthread_mutex_lock (&g_static_rep_mutex);

    tmp_rid.page = g_static_rep.list_rid.page;
    tmp_rid.size = sizeof (struct static_rep_list_item);

    if ((i = hash_lookup (g_static_rep_hash, STATIC_REP_HASH_NBUCKEXP,
			  OBJ2KEY (static_ptr))))
	i--;  /* Note: adjust back to original index value... (see below) */
    else {
	i = g_static_rep.vacant_head;
	if (i >= 0) {
	    /* Untie from vacant list (head). */
	    next_vacant = g_static_rep_list[i].next_index;
	    g_static_rep.vacant_head = next_vacant;
	    if (next_vacant >= 0)
		g_static_rep_list[next_vacant].prev_index = -1;

	    /* Tie into occupied list. */
	    next_occupied = g_static_rep.occupied_head;
	    if (next_occupied >= 0)
		g_static_rep_list[next_occupied].prev_index = i;
	    g_static_rep_list[i].next_index = next_occupied;
	    g_static_rep.occupied_head = i;

	    /* Update key field in list item. */
	    g_static_rep_list[i].static_ptr = static_ptr;

	    /* Insert index to fast lookup table.
	     * Note: we use a hard-coded offset of 1, to distinguish 0 lookup
	     * return value for "no element found" value. */
	    hash_insert (g_static_rep_hash, STATIC_REP_HASH_NBUCKEXP,
			 OBJ2KEY (static_ptr), i + 1);

	    /* Reflect structural changes to persistent image of repository. */
	    Tset (xid, g_boot.static_rep_rid, &g_static_rep);
	    
	    if (next_vacant >= 0) {
		tmp_rid.slot = next_vacant;
		Tset (xid, tmp_rid, g_static_rep_list + next_vacant);
	    }
	    if (next_occupied >= 0) {
		tmp_rid.slot = next_occupied;
		Tset (xid, tmp_rid, g_static_rep_list + next_occupied);
	    }
	}
    }

    /* Update object pointer in list element, and reflect to persistent image. */
    if (i >= 0) {
	g_static_rep_list[i].objid = obj;
	tmp_rid.slot = i;
	Tset (xid, tmp_rid, g_static_rep_list + i);
    }

    debug ("unlocking static reference repository");
    pthread_mutex_unlock (&g_static_rep_mutex);

    if (i < 0) {
	debug ("error: static reference repository exhausted, assignment aborted");
	pobj_end ();
	return -1;
    }

    /* Update static reference. */
    if (is_set)
	*static_ptr = obj;

    pobj_end ();

    return 0;
}

int
pobj_static_set_ref (void *static_tmp_ptr, void *obj)
{
    return pobj_static_set_update_ref (static_tmp_ptr, obj, 1);
}

int
pobj_static_update_ref (void *static_tmp_ptr)
{
    return pobj_static_set_update_ref (static_tmp_ptr, NULL, 0);
}



static void
pobj_restore (void *objid, size_t size, recordid *rid,
	      hash_table pobj_convert_hash,
	      int pobj_convert_hash_nbuckexp, int xid)
{
    struct pobj *p;
    void *obj;
    
    debug ("restoring objid=%p size=%d (%d) rid={%d,%d,%ld}",
	   objid, size, POBJ_SIZE (size), rid->page, rid->slot, rid->size);

    /* Allocate memory. */
    p = (struct pobj *) g_ext_memfunc.malloc (POBJ_SIZE (size));
    obj = POBJ2OBJ (p);

    debug ("new object allocated at %p (%p)", obj, (void *) p);

    /* Read object from backing store. */
    Tread (xid, *rid, p);

    debug ("inserting %p->%p conversion pair",
	   objid, obj);

    /* Insert old/new objid pair to conversion table. */
    hash_insert (pobj_convert_hash, pobj_convert_hash_nbuckexp,
		 OBJ2KEY (objid), (unsigned long) obj);
}

static void *
pobj_adjust (void *old_objid, hash_table pobj_convert_hash,
	     int pobj_convert_hash_nbuckexp, int xid)
{
    void *new_objid;
    struct pobj *p;
    int nrefs;
    unsigned long *flags_ptr;
    unsigned long flags;
    unsigned long mask;
    int bit, complement;
    void **ref;
    void *newref;
    int changed = 0;
    int i;
    recordid tmp_rid;
    
    new_objid =	(void *) hash_lookup (pobj_convert_hash, pobj_convert_hash_nbuckexp,
				      OBJ2KEY (old_objid));
    p = OBJ2POBJ (new_objid);

    debug ("adjusting objid at object's repository entry: %p->%p",
	   old_objid, new_objid);
    
    /* Update objid field in repository. */
    i = p->rep_index;
    g_pobj_rep_list[i].objid = new_objid;

    /* Update persistent image of repository item. */
    tmp_rid.page = g_pobj_rep.list_rid.page;
    tmp_rid.size = sizeof (struct pobj_rep_list_item);
    tmp_rid.slot = i;
    Tset (xid, tmp_rid, g_pobj_rep_list + i);

#if 0
    debug ("adjusting objid at persistent object header: %p->%p",
	   old_objid, new_objid);

    /* Update objid at object header. */
    p->objid = new_objid;
#endif

    debug ("adjusting object reference fields...");
    
    /* Traverse persistent reference fields in object, convert old
     * pointer value with new one (using conversion table lookup). */
    ref = (void **) new_objid;
    nrefs = POBJ_NREFS (p->size);
    flags_ptr = POBJ2REFS (p);
    flags = *flags_ptr;
    bit = 0;
    mask = 1;
    while (nrefs > 0) {
	if (flags & mask) {
	    newref = (void *) hash_lookup (pobj_convert_hash,
		     			   pobj_convert_hash_nbuckexp,
		     			   OBJ2KEY (*ref));

	    debug (" offset %d: %p->%p",
		   (char *) ref - (char *) new_objid,
		   *ref, newref);

	    *ref = newref;
	    changed++;
	}

	/* Advance to next potential reference. */
	ref++;
	nrefs--;
	if (++bit == WORDBITS) {
	    flags = *(++flags_ptr);
	    bit = 0;
	    mask = 1;
	}
	else
	    mask <<= 1;

	/* Skip word-full of flags, if flags suffix indicates no references. */
	while (nrefs > 0 && flags == (flags & (mask - 1))) {
	    complement = WORDBITS - bit;
	    ref += complement;
	    nrefs -= complement;
	    flags = *(++flags_ptr);

	    if (bit) {
		bit = 0;
		mask = 1;
	    }
	}
	
    }

    debug ("...done (%d adjusted)", changed);

    /* Update persistent image of object. */
    /* TODO: use delta updates, via set_range method. */
    Tset (xid, g_pobj_rep_list[p->rep_index].rid, p);

    return new_objid;
}


/* TODO: possibly use distinct transactions for read/write?
 * Right now doesn't seem to matter, as reads do not generate log entries.
 */
static void
pobj_boot_restore (void)
{
    HASH_DECLARE (pobj_convert_hash, CONVERT_HASH_NBUCKEXP);
    recordid tmp_rid;
    void *new_objid;
    int i;
    int changed = 0;
    int xid;
    int count;

    debug ("reading boot record");

    xid = Tbegin ();
    
    /* Read boot record. */
    Tread (xid, g_boot_rid, &g_boot);

    debug ("restoring persistent object repository");

    /* Restore persistent object repository. */
    Tread (xid, g_boot.pobj_rep_rid, &g_pobj_rep);
    tmp_rid.page = g_pobj_rep.list_rid.page;
    tmp_rid.size = sizeof (struct pobj_rep_list_item);
    for (i = 0; i < POBJ_REP_MAX; i++) {
	tmp_rid.slot = i;
	Tread (xid, tmp_rid, g_pobj_rep_list + i);
    }

    debug ("reconstructing objects and forming conversion table...");

    /* Reconstruct heap and build object conversion table. */
    hash_init (pobj_convert_hash, CONVERT_HASH_NBUCKEXP);
    for (count = 0, i = g_pobj_rep.occupied_head; i >= 0;
	 count++, i = g_pobj_rep_list[i].next_index) {
	pobj_restore (g_pobj_rep_list[i].objid, g_pobj_rep_list[i].size,
		      &g_pobj_rep_list[i].rid, pobj_convert_hash,
		      CONVERT_HASH_NBUCKEXP, xid);
    }
    debug ("...done (%d reconstructed)", count);
    
    debug ("restoring static references repository");
    
    /* Reconstruct static references repository. */
    Tread (xid, g_boot.static_rep_rid, &g_static_rep);
    tmp_rid.page = g_static_rep.list_rid.page;
    tmp_rid.size = sizeof (struct static_rep_list_item);
    for (i = 0; i < STATIC_REP_MAX; i++) {
	tmp_rid.slot = i;
	Tread (xid, tmp_rid, g_static_rep_list + i);
    }

    debug ("adjusting object references");

    /* Translate reference fields in objects. */
    for (i = g_pobj_rep.occupied_head; i >= 0; i = g_pobj_rep_list[i].next_index)
	pobj_adjust (g_pobj_rep_list[i].objid, pobj_convert_hash,
    		     CONVERT_HASH_NBUCKEXP, xid);

    debug ("tying adjusted static references, building fast lookup table...");
    
    /* Tie static references to adjusted objects, build fast lookup table. */
    hash_init (g_static_rep_hash, STATIC_REP_HASH_NBUCKEXP);
    tmp_rid.page = g_static_rep.list_rid.page;
    tmp_rid.size = sizeof (struct pobj_rep_list_item);
    for (i = g_static_rep.occupied_head; i >= 0; i = g_static_rep_list[i].next_index) {
	new_objid = (void *) hash_lookup (pobj_convert_hash,
					  CONVERT_HASH_NBUCKEXP,
					  OBJ2KEY (g_static_rep_list[i].objid));
	
	debug (" adjusting %p: %p->%p", (void *) g_static_rep_list[i].static_ptr,
	       g_static_rep_list[i].objid, new_objid);
	*g_static_rep_list[i].static_ptr = new_objid;
	g_static_rep_list[i].objid = new_objid;
	
	/* Note: we apply a deliberate offset of 1 to index values,
	 * so as to be able to distinguish a 0 index value from "key not found"
	 * on subsequent table lookups. */
	hash_insert (g_static_rep_hash, STATIC_REP_HASH_NBUCKEXP,
		     OBJ2KEY (g_static_rep_list[i].static_ptr), i + 1);

	tmp_rid.slot = i;
	Tset (xid, tmp_rid, g_static_rep_list + i);
	changed++;
    }

    debug ("...done (%d tied)", changed);

    Tcommit (xid);

    hash_close (pobj_convert_hash, CONVERT_HASH_NBUCKEXP);
}


static void
pobj_boot_init (void)
{
    recordid tmp_rid;
    int i;
    int xid;

    debug ("creating persistent object repository");

    /* Create persistent object repository: first, the list-over-array... */
    for (i = 0; i < POBJ_REP_MAX; i++) {
	g_pobj_rep_list[i].prev_index = i - 1;
	g_pobj_rep_list[i].next_index = i + 1;
    }
    g_pobj_rep_list[POBJ_REP_MAX - 1].next_index = -1;

    xid = Tbegin ();

    g_pobj_rep.list_rid =
	TarrayListAlloc (xid, POBJ_REP_MAX, 2, sizeof (struct pobj_rep_list_item));
    TarrayListExtend (xid, g_pobj_rep.list_rid, POBJ_REP_MAX);

    Tcommit (xid);
    xid = Tbegin ();
    
    tmp_rid.page = g_pobj_rep.list_rid.page;
    tmp_rid.size = sizeof (struct pobj_rep_list_item);
    for (i = 0; i < POBJ_REP_MAX; i++) {
	tmp_rid.slot = i;
	Tset (xid, tmp_rid, g_pobj_rep_list + i);
    }

    /* ...then, the list control block. */
    g_pobj_rep.vacant_head = 0;
    g_pobj_rep.occupied_head = -1;  /* i.e. empty list. */
    g_boot.pobj_rep_rid = Talloc (xid, sizeof (struct pobj_rep));
    Tset (xid, g_boot.pobj_rep_rid, &g_pobj_rep);

    Tcommit (xid);

    debug ("creating static reference repository");

    /* Create static references repository: first, the list-over-array... */
    for (i = 0; i < STATIC_REP_MAX; i++) {
	g_static_rep_list[i].prev_index = i - 1;
	g_static_rep_list[i].next_index = i + 1;
    }
    g_static_rep_list[STATIC_REP_MAX - 1].next_index = -1;
    
    xid = Tbegin ();

    g_static_rep.list_rid =
	TarrayListAlloc (xid, STATIC_REP_MAX, 2, sizeof (struct static_rep_list_item));
    TarrayListExtend (xid, g_static_rep.list_rid, STATIC_REP_MAX);
    
    Tcommit (xid);
    xid = Tbegin ();
    
    tmp_rid.page = g_static_rep.list_rid.page;
    tmp_rid.size = sizeof (struct pobj_rep_list_item);
    for (i = 0; i < STATIC_REP_MAX; i++) {
	tmp_rid.slot = i;
	Tset (xid, tmp_rid, g_static_rep_list + i);
    }
    
    /* ...second, the list control block... */
    g_static_rep.vacant_head = 0;
    g_static_rep.occupied_head = -1;  /* i.e. empty list. */
    g_boot.static_rep_rid = Talloc (xid, sizeof (struct static_rep));
    Tset (xid, g_boot.static_rep_rid, &g_static_rep);

    Tcommit (xid);

    /* ...last, the fast lookup hash table. */
    hash_init (g_static_rep_hash, STATIC_REP_HASH_NBUCKEXP);

    debug ("writing boot record");

    xid = Tbegin ();
    
    /* Write boot record. */
    Tset (xid, g_boot_rid, &g_boot);

    Tcommit (xid);

    debug ("done");
}



int
pobj_init (struct pobj_memfunc *ext_memfunc, struct pobj_memfunc *int_memfunc)
{
    recordid tmp_rid;
    int xid;
    int ret;
    int lock_ret = 0;

    debug ("locking");
    if ((lock_ret = pthread_mutex_trylock (&g_pobj_rep_mutex))
	|| pthread_mutex_trylock (&g_static_rep_mutex))
    {
	debug ("error: cannot lock repositories for construction");
	if (lock_ret == 0)
	    pthread_mutex_unlock (&g_pobj_rep_mutex);
	return -1;
    }
    
    if (g_is_init) {
	debug ("error: already initialized");
	pthread_mutex_unlock (&g_pobj_rep_mutex);
	pthread_mutex_unlock (&g_static_rep_mutex);
	return -1;
    }

    /* Copy memory manager handles, if provided. */
    if (ext_memfunc) {
	if (ext_memfunc->malloc)
	    g_ext_memfunc.malloc = ext_memfunc->malloc;
	if (ext_memfunc->calloc)
	    g_ext_memfunc.calloc = ext_memfunc->calloc;
	if (ext_memfunc->realloc)
	    g_ext_memfunc.realloc = ext_memfunc->realloc;
	if (ext_memfunc->free)
	    g_ext_memfunc.free = ext_memfunc->free;
    }

    /* Initialize internal memory manager. */
    if (int_memfunc)
	xmem_memfunc (int_memfunc->malloc, int_memfunc->free);

    Tinit ();

    /* Recover boot record: since record naming is not supported,
       we try to allocate a record just to see if it occupies the
       "boot" slot (page 1, slot 0) -- otherwise, such a record
       already exists and should be restored. */
    /* TODO: switch to new method (courtesy of Rusty Sears). */
    xid = Tbegin ();
    if (xid < 0) {
	debug ("begin transaction failed, aborted");
	pthread_mutex_unlock (&g_pobj_rep_mutex);
	pthread_mutex_unlock (&g_static_rep_mutex);
	return -1;
    }

    tmp_rid = Talloc (xid, sizeof (struct boot_record));

    if (g_boot_rid.page != tmp_rid.page || g_boot_rid.slot != tmp_rid.slot) {
	Tabort (xid);

	debug ("restoring previous state");
	pobj_boot_restore ();
	ret = 1;
    }
    else {
	Tcommit (xid);

	debug ("initializing structures on first time use");
	pobj_boot_init ();
	ret = 0;
    }

    /* Create thread specific active-xid key. */
    pthread_key_create (&g_active_xid_key, NULL);
    pthread_key_create (&g_active_nested_key, NULL);

    g_is_init = 1;

    debug ("unlocking");
    pthread_mutex_unlock (&g_pobj_rep_mutex);
    pthread_mutex_unlock (&g_static_rep_mutex);

    debug ("init complete");

    return ret;
}


int
pobj_shutdown (void)
{
    int lock_ret;

    debug ("locking");
    if ((lock_ret = pthread_mutex_trylock (&g_pobj_rep_mutex))
	|| pthread_mutex_trylock (&g_static_rep_mutex))
    {
	debug ("error: cannot lock repositories for destruction");
	if (lock_ret == 0)
	    pthread_mutex_unlock (&g_pobj_rep_mutex);
	return -1;
    }

    if (! g_is_init) {
	debug ("error: already shutdown");
	pthread_mutex_unlock (&g_pobj_rep_mutex);
	pthread_mutex_unlock (&g_static_rep_mutex);
	return -1;
    }
    
    hash_close (g_static_rep_hash, STATIC_REP_HASH_NBUCKEXP);
    Tdeinit ();
    g_is_init = 0;

    debug ("unlocking");
    pthread_mutex_unlock (&g_pobj_rep_mutex);
    pthread_mutex_unlock (&g_static_rep_mutex);

    debug ("shutdown complete");

    return 0;
}
