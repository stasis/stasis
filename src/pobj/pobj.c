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


/* Convert an object pointer into a proper hash key (2 lsb's are zero
 * due to alignment, hence make hashing quite redundant...). */
#define OBJ2KEY(obj)  ((unsigned long) (obj) >> 2)


/* Limits.
 * TODO: use dynamic limits instead (switch to growable structures). */
#define STATIC_REP_SEG_MAX_EXP    8
#define STATIC_REP_SEG_MAX        (1 << STATIC_REP_SEG_MAX_EXP)
#define STATIC_REP_HASH_NBUCKEXP  6
#define POBJ_REP_SEG_MAX_EXP      10
#define POBJ_REP_SEG_MAX          (1 << POBJ_REP_SEG_MAX_EXP)

#define CONVERT_HASH_NBUCKEXP     10
#define UPDATE_HASH_NBUCKEXP      10
#define UPDATE_QUEUE_MAX_EXP      10
#define UPDATE_QUEUE_MAX          (1 << UPDATE_QUEUE_MAX_EXP)

#define TMPBUF_INIT_SIZE          1024
#define TMPBUF_GROW_FACTOR        2


/* Note: persistent object header has been moved to pobj.h in order to
 * allow fast IS_PERSISTENT check. */
#define POBJ_NREFS(s)            ((s) / WORDSIZE)
#define POBJ_REFFLAGS_OFFSET(s)  (ALIGN(POBJ_HEADER_SIZE) + ALIGN(s))
#define POBJ_REFFLAGS_SIZE(s)    ((size_t) ALIGN((POBJ_NREFS(s) + 7) / 8))
#define POBJ_SIZE(s)             (POBJ_REFFLAGS_OFFSET(s) + POBJ_REFFLAGS_SIZE(s))

#define OBJ2POBJ(p)  ((struct pobj *)(((char *) p) - ALIGN(POBJ_HEADER_SIZE)))
#define POBJ2OBJ(p)  ((void *)(((char *) p) + ALIGN(POBJ_HEADER_SIZE)))
#define POBJ2REFS(p) ((unsigned long *)(((char *) p) + POBJ_REFFLAGS_OFFSET((p)->size)))

#define STATIC_REP_SEG(i)  ((i) >> STATIC_REP_SEG_MAX_EXP)
#define STATIC_REP_OFF(i)  ((i) & ((1 << STATIC_REP_SEG_MAX_EXP) - 1))
#define POBJ_REP_SEG(i)    ((i) >> POBJ_REP_SEG_MAX_EXP)
#define POBJ_REP_OFF(i)    ((i) & ((1 << POBJ_REP_SEG_MAX_EXP) - 1))
#define POBJ2REPSEG(p)     (POBJ_REP_SEG((p)->rep_index))
#define POBJ2REPOFF(p)     (POBJ_REP_OFF((p)->rep_index))



/* General purpose repository. */
struct rep_seg {
    recordid list_rid;
    recordid next_seg_rid;
};

struct rep {
    recordid first_seg_rid;
    int nseg;
    int vacant_head;
    int occupied_head;
};


/* Persistent object repository. */
struct pobj_rep_list_item {
    int prev_index;
    int next_index;
    void *objid;
    size_t size;
    recordid rid;
};

struct pobj_rep_list {
    recordid rid;
    struct pobj_rep_list_item *list;
};

static struct pobj_rep_list *g_pobj_rep_list;
static int g_pobj_rep_list_max;
static recordid g_pobj_last_seg_rid;
struct rep_seg g_pobj_last_seg;
static struct rep g_pobj_rep;
#define POBJ2REPSLOT(p)  (g_pobj_rep_list[POBJ2REPSEG(p)].list + POBJ2REPOFF(p))


/* Persistent static reference repository. */
struct static_rep_list_item {
    int prev_index;
    int next_index;
    void **static_ptr;
    void *objid;
};

struct static_rep_list {
    recordid rid;
    struct static_rep_list_item *list;
};

static struct static_rep_list *g_static_rep_list;
static int g_static_rep_list_max;
static recordid g_static_last_seg_rid;
struct rep_seg g_static_last_seg;
static struct rep g_static_rep;


/* Persistent static references fast lookup table. */
/* TODO: switch to a growable data structure. */
static struct hash_table *g_static_rep_hash;


/* Boot record. */
struct boot_record {
    recordid pobj_rep_rid;
    recordid static_rep_rid;
};

static struct boot_record g_boot;
static recordid g_boot_rid = { 1, 0, sizeof (struct boot_record) };


/* Memory manager calls: initialize to libc defaults. */
struct pobj_memfunc g_memfunc = { malloc, calloc, realloc, free };


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



int
pobj_persistify (void *obj)
{
    struct pobj *p = OBJ2POBJ (obj);
    recordid tmp_rid;
    int next_occupied;
    int xid;
    int i, j, next_index;
    struct pobj_rep_list *tmp_pobj_rep_list;
    struct pobj_rep_list_item *pobj_slot;
    size_t pobj_size;

    if (! g_is_init)
	return -1;

    if (p->rep_index >= 0)
	return 0;  /* already persistent. */
    pobj_size = POBJ_SIZE (p->size);

    if ((xid = pobj_start ()) < 0) {
	debug ("error: begin transaction failed");
	return -1;
    }

    debug ("locking persistent object repository");
    pthread_mutex_lock (&g_pobj_rep_mutex);

    tmp_rid.size = sizeof (struct pobj_rep_list_item);

    i = g_pobj_rep.vacant_head;
    /* Allocate new list segment if space exhausted. */
    if (i == g_pobj_rep_list_max) {
	debug ("allocating new segment of persistent objects repository...");

	debug (" allocating memory");
	tmp_pobj_rep_list = (struct pobj_rep_list *)
	    XREALLOC (g_pobj_rep_list,
		      sizeof (struct pobj_rep_list) * (g_pobj_rep.nseg + 1));
	if (tmp_pobj_rep_list) {
	    g_pobj_rep_list = tmp_pobj_rep_list;
	    g_pobj_rep_list[g_pobj_rep.nseg].list =
		(struct pobj_rep_list_item *)
		XMALLOC (sizeof (struct pobj_rep_list_item) * POBJ_REP_SEG_MAX);
	}
	if (! (tmp_pobj_rep_list && g_pobj_rep_list[g_pobj_rep.nseg].list)) {
	    debug (" error: allocation failed");
	    pthread_mutex_unlock (&g_pobj_rep_mutex);
	    pobj_end ();
	    return -1;
	}

	debug (" initializing new segment list");
	/* Note: don't care for back pointers in vacant list. */
	pobj_slot = g_pobj_rep_list[g_pobj_rep.nseg].list;
	next_index = POBJ_REP_SEG_MAX * g_pobj_rep.nseg + 1;
	for (j = 0; j < POBJ_REP_SEG_MAX; j++) {
	    pobj_slot->next_index = next_index++;
	    pobj_slot++;
	}

	debug (" allocating new segment record");
	g_pobj_last_seg.next_seg_rid =
	    Talloc (xid, sizeof (struct rep_seg));
	Tset (xid, g_pobj_last_seg_rid, &g_pobj_last_seg);
	g_pobj_last_seg_rid = g_pobj_last_seg.next_seg_rid;
	memset (&g_pobj_last_seg, 0, sizeof (struct rep_seg));

	debug (" allocating new segment list record");
	g_pobj_last_seg.list_rid =
	    g_pobj_rep_list[g_pobj_rep.nseg].rid =
	    TarrayListAlloc (xid, POBJ_REP_SEG_MAX, 2,
			     sizeof (struct pobj_rep_list_item));
	TarrayListExtend (xid, g_pobj_last_seg.list_rid, POBJ_REP_SEG_MAX);

	debug (" dumping new segment and list to persistent storage");
	/* Note: don't write first element as it's being written due to
	 * update anyway. */
	Tset (xid, g_pobj_last_seg_rid, &g_pobj_last_seg);
	tmp_rid.page = g_pobj_last_seg.list_rid.page;
	for (j = 1; j < POBJ_REP_SEG_MAX; j++) {
	    tmp_rid.slot = j;
	    Tset (xid, tmp_rid, g_pobj_rep_list[g_pobj_rep.nseg].list + j);
	}

	/* Update segment count.
	 * Note: don't write to persistent image, done later anyway. */
	g_pobj_rep.nseg++;
	g_pobj_rep_list_max += POBJ_REP_SEG_MAX;

	debug ("...done (%d segments, %d total slots)",
	       g_pobj_rep.nseg, g_pobj_rep_list_max);
    }

    pobj_slot = g_pobj_rep_list[POBJ_REP_SEG (i)].list + POBJ_REP_OFF (i);

    /* Untie from vacant list (head). */
    g_pobj_rep.vacant_head = pobj_slot->next_index;

    /* Tie into occupied list. */
    next_occupied = g_pobj_rep.occupied_head;
    if (next_occupied >= 0)
	g_pobj_rep_list[POBJ_REP_SEG (next_occupied)].
	    list[POBJ_REP_OFF (next_occupied)].prev_index = i;
    pobj_slot->next_index = next_occupied;
    g_pobj_rep.occupied_head = i;

    /* Reflect structural changes to persistent image of repository. */
    Tset (xid, g_boot.pobj_rep_rid, &g_pobj_rep);
    if (next_occupied >= 0) {
	tmp_rid.page = g_pobj_rep_list[POBJ_REP_SEG (next_occupied)].rid.page;
	tmp_rid.slot = POBJ_REP_OFF (next_occupied);
	Tset (xid, tmp_rid,
	      g_pobj_rep_list[POBJ_REP_SEG (next_occupied)].list
	      + POBJ_REP_OFF (next_occupied));
    }

    /* Set object info, attach new record. */
    debug ("updating repository entry and attaching new record, i=%d", i);
    pobj_slot->objid = obj;
    pobj_slot->size = p->size;
    pobj_slot->rid = Talloc (xid, pobj_size);
    tmp_rid.page = g_pobj_rep_list[POBJ_REP_SEG (i)].rid.page;
    tmp_rid.slot = POBJ_REP_OFF (i);
    Tset (xid, tmp_rid,
	  g_pobj_rep_list[POBJ_REP_SEG (i)].list + POBJ_REP_OFF (i));

    debug ("unlocking persistent object repository");
    pthread_mutex_unlock (&g_pobj_rep_mutex);

    /* Update header and dump to persistent image. */
    p->rep_index = i;
    Tset (xid, pobj_slot->rid, p);

    pobj_end ();
    
    return 0;
}

int
pobj_unpersistify (void *obj)
{
    struct pobj *p = OBJ2POBJ (obj);
    struct pobj_rep_list_item *pobj_slot;
    int next_vacant, next_occupied, prev_occupied;
    recordid tmp_rid;
    int xid;
    int i;

    if (! g_is_init)
	return -1;

    if ((i = p->rep_index) < 0)
	return 0;  /* already transient */
    pobj_slot = POBJ2REPSLOT (p);

    if ((xid = pobj_start ()) < 0) {
	debug ("error: begin transaction failed");
	return -1;
    }
    
    debug ("locking persistent object repository");
    pthread_mutex_lock (&g_pobj_rep_mutex);

    /* Untie from occupied list (not necessarily head). */
    next_occupied = pobj_slot->next_index;
    prev_occupied = pobj_slot->prev_index;
    if (prev_occupied >= 0)
	g_pobj_rep_list[POBJ_REP_SEG (prev_occupied)].
	    list[POBJ_REP_OFF (prev_occupied)].next_index = next_occupied;
    else
	g_pobj_rep.occupied_head = next_occupied;
    if (next_occupied >= 0)
	g_pobj_rep_list[POBJ_REP_SEG (next_occupied)].
	    list[POBJ_REP_OFF (next_occupied)].prev_index = prev_occupied;

    /* Tie into vacant list.
     * Note: don't care about back pointers when tying. */
    next_vacant = g_pobj_rep.vacant_head;
    if (next_vacant < g_pobj_rep_list_max)
	g_pobj_rep_list[POBJ_REP_SEG (next_vacant)].
	    list[POBJ_REP_OFF (next_vacant)].prev_index = i;
    pobj_slot->next_index = next_vacant;
    g_pobj_rep.vacant_head = i;

    /* Reflect structural changes to persistent image of repository. */
    Tset (xid, g_boot.pobj_rep_rid, &g_pobj_rep);
    tmp_rid.size = sizeof (struct pobj_rep_list_item);
    if (next_occupied >= 0) {
	tmp_rid.page = g_pobj_rep_list[POBJ_REP_SEG (next_occupied)].rid.page;
	tmp_rid.slot = POBJ_REP_OFF (next_occupied);
	Tset (xid, tmp_rid,
	      g_pobj_rep_list[POBJ_REP_SEG (next_occupied)].list
	      + POBJ_REP_OFF (next_occupied));
    }
    if (prev_occupied >= 0) {
	tmp_rid.page = g_pobj_rep_list[POBJ_REP_SEG (prev_occupied)].rid.page;
	tmp_rid.slot = POBJ_REP_OFF (prev_occupied);
	Tset (xid, tmp_rid,
	      g_pobj_rep_list[POBJ_REP_SEG (prev_occupied)].list
	      + POBJ_REP_OFF (prev_occupied));
    }
    if (next_vacant >= 0) {
	tmp_rid.page = g_pobj_rep_list[POBJ_REP_SEG (next_vacant)].rid.page;
	tmp_rid.slot = POBJ_REP_OFF (next_vacant);
	Tset (xid, tmp_rid,
	      g_pobj_rep_list[POBJ_REP_SEG (next_vacant)].list
	      + POBJ_REP_OFF (next_vacant));
    }
    tmp_rid.page = g_pobj_rep_list[POBJ_REP_SEG (i)].rid.page;
    tmp_rid.slot = POBJ_REP_OFF (i);
    Tset (xid, tmp_rid,
	  g_pobj_rep_list[POBJ_REP_SEG (i)].list + POBJ_REP_OFF (i));

    /* Detach persistent record. */
    debug ("detaching record");
    Tdealloc (xid, pobj_slot->rid);

    debug ("unlocking persistent object repository");
    pthread_mutex_unlock (&g_pobj_rep_mutex);

    /* Update object header (transient mode). */
    p->rep_index = -1;

    pobj_end ();

    return 0;
}

static void *
pobj_allocate (size_t size, void *(*alloc) (size_t), void (*dealloc) (void *),
	       int persist, int zero)
{
    struct pobj *p;
    void *obj;
    size_t pobj_size;

    if (! g_is_init)
	return NULL;

    if (! alloc)
	alloc = g_memfunc.malloc;
    if (! dealloc)
	dealloc = g_memfunc.free;

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

    /* Initialize persistent object header and type flags. */
    p->size = size;
    p->type_index = -1;  /* i.e. untyped. */
    p->rep_index = -1;
    memset (POBJ2REFS (p), 0, POBJ_REFFLAGS_SIZE (size));

    debug ("allocated object at %p (%p) size %d (%d+%d+%d=%d)",
	   obj, (void *) p, size, ALIGN (POBJ_HEADER_SIZE), ALIGN (size),
       	   POBJ_REFFLAGS_SIZE (size), pobj_size);

    if (persist && pobj_persistify (obj) < 0) {
	debug ("error: persistification failed");
	dealloc (p);
       	return NULL;
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

    if (! g_is_init)
	return;

    if (raw)
	obj = POBJ2OBJ (p);

    /* Destruct persistent image. */
    if (pobj_unpersistify (obj) < 0) {
	debug ("error: unpersistification failed");
	return;
    }

    /* Deallocate augmented memory object, or switch to transient mode. */
    if (deallocate) {
	debug ("deallocating memory");
	g_memfunc.free (p);
    }
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
    struct pobj_rep_list_item *pobj_slot;
    int ref_offset;
    int flags_offset;
    int bit;
    unsigned long *flags_ptr;
    unsigned long flags, mask;
    int xid = -1;
    int ret;

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

    /* Open transaction context (persistent objects only). */
    if (p->rep_index >= 0 && (xid = pobj_start ()) < 0) {
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

    /* Update corresponding record (persistent objects only). */
    if (p->rep_index >= 0) {
	pobj_slot = POBJ2REPSLOT (p);
    
	if (ret)
	    TsetRange (xid, pobj_slot->rid, (char *) flags_ptr - (char *) p,
		       sizeof (unsigned long), flags_ptr);

	pobj_end ();
    }

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
    struct pobj_rep_list_item *pobj_slot;
    int nrefs;
    int ref_offset;
    int flags_offset;
    int bit;
    unsigned long *flags_ptr;
    int xid = -1;
    int count;

    if (! reflist) {
	debug ("error: null reference list provided, aborted");
	return -1;
    }
    
    /* Open transaction context (persistent objects only). */
    if (p->rep_index >= 0 && (xid = pobj_start ()) < 0) {
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

    /* Update corresponding record (persistent objects only). */
    if (p->rep_index >= 0) {
	pobj_slot = POBJ2REPSLOT (p);

	TsetRange (xid, pobj_slot->rid, POBJ_REFFLAGS_OFFSET (p->size),
		   POBJ_REFFLAGS_SIZE (p->size), POBJ2REFS (p));
	
	pobj_end ();
    }

    return 0;
}

       
static int
pobj_memcpy_memset_typed (void *obj, void *fld, void *data, int c, size_t len,
			  int is_typed, int is_ref, int is_copy)
{
    struct pobj *p = OBJ2POBJ (obj);
    struct pobj_rep_list_item *pobj_slot;
#ifdef HAVE_IMPLICIT_TYPES
    int is_changed = 0;
#endif /* HAVE_IMPLICIT_TYPES */
    int xid = -1;

    /* Safety check. */
    /* TODO: is this worthwhile? */
    if ((char *) obj > (char *) fld
	|| (char *) obj + p->size < (char *) fld + len)
    {
	debug ("error: field out of object bounds, aborted");
	return -1;
    }

    /* Open transaction context (persistent objects only). */
    if (p->rep_index >= 0 && (xid = pobj_start ()) < 0) {
	debug ("error: begin transaction failed, aborted");
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

    /* Update memory object field. */
    if (is_copy)
	memcpy (fld, data, len);
    else
	memset (fld, c, len);

    /* Update corresponding record (persistent objects only). */
    if (p->rep_index >= 0) {
	pobj_slot = POBJ2REPSLOT (p);

	TsetRange (xid, pobj_slot->rid, (char *) fld - (char *) p, len, fld);
#ifdef HAVE_IMPLICIT_TYPES
	if (is_changed)
	    TsetRange (xid, pobj_slot->rid, POBJ_REFFLAGS_OFFSET (p->size),
		       POBJ_REFFLAGS_SIZE (p->size), POBJ2REFS (p));
#endif /* HAVE_IMPLICIT_TYPES */
	
	pobj_end ();
    }

    return 0;
}


void *
pobj_memcpy (void *obj, void *fld, void *data, size_t len)
{
    if (pobj_memcpy_memset_typed (obj, fld, data, 0, len, 0, 0, 1) < 0)
	return NULL;
    return obj;
}

void *
pobj_memset (void *obj, void *fld, int c, size_t len)
{
    if (pobj_memcpy_memset_typed (obj, fld, NULL, c, len, 0, 0, 0) < 0)
	return NULL;
    return obj;
}

#define POBJ_SET_NONREF(name,type)  \
    int name (void *obj, type *fld, type data) {  \
	return pobj_memcpy_memset_typed (obj, fld, &data, 0, sizeof (data), 1, 0, 1);  \
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
	
    return pobj_memcpy_memset_typed (obj, fld, &ref, 0, sizeof (ref), 1, 1, 1);
}



int
pobj_update_range (void *obj, void *fld, size_t len)
{
    struct pobj *p = OBJ2POBJ (obj);
    struct pobj_rep_list_item *pobj_slot;
    int xid;

    if (p->rep_index < 0)
	return 0;  /* transient mode. */

    pobj_slot = POBJ2REPSLOT (p);

    /* Safety check (only if len is non-zero). */
    /* TODO: is this worthwhile? */
    if (len && ((char *) obj > (char *) fld
		|| (char *) obj + p->size < (char *) fld + len))
    {
	debug ("error: field out of object bounds, aborted");
	return -1;
    }

    if ((xid = pobj_start ()) < 0) {
	debug ("error: begin transaction failed, aborted");
    	return -1;
    }
    
    /* Update corresponding record. */
    if (len)
	TsetRange (xid, pobj_slot->rid, (char *) fld - (char *) p, len, fld);
    else
	Tset (xid, pobj_slot->rid, p);

    pobj_end ();

    return 0;
}


int
pobj_update_recursive (void *obj, int persist)
{
    void *pobj_update_queue[UPDATE_QUEUE_MAX];
    struct hash_table *pobj_update_hash;
    int q_head, q_tail;
    void *tmp;
    size_t tmp_size = TMPBUF_INIT_SIZE;
    struct pobj *p;
    struct pobj_rep_list_item *pobj_slot;
    int xid;
    int processed, enqueued;
    int nrefs;
    unsigned long *flags_ptr;
    unsigned long flags;
    unsigned long mask;
    int bit, complement;
    void **ref;
    void *next;
    int fresh;
    int ret;

    /* Allocate temporary (growable) buffer. */
    tmp = XMALLOC (tmp_size);
    if (! tmp) {
	debug ("error: allocation of temporary buffer failed");
	return -1;
    }

    if ((xid = pobj_start ()) < 0) {
	debug ("error: begin transaction failed, aborted");
	XFREE (tmp);
	return -1;
    }

    /* Initialize visited objects table. */
    pobj_update_hash = hash_new (UPDATE_HASH_NBUCKEXP);

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
	hash_insert (pobj_update_hash, OBJ2KEY (obj), 1);
	
	/* Persistify / skip object, as necessary. */
	if (p->rep_index < 1) {
	    if (persist) {
		debug ("persistifying %p (%p)", obj, (void *) p);

		if (pobj_persistify (obj) < 0) {
		    debug ("error: persistification failed");
		    break;
		}
		fresh = 1;
	    }
	    else
		continue;
	}
	else
	    fresh = 0;
	    
	processed++;
	pobj_slot = POBJ2REPSLOT (p);

	/* Read persistent copy into temporary buffer (stale objects only). */
	if (! fresh) {
	    /* Grow temporary buffer as necessary. */
	    if (POBJ_SIZE (p->size) > tmp_size) {
		XFREE (tmp);
		tmp_size *= TMPBUF_GROW_FACTOR;
		tmp = XMALLOC (tmp_size);
		if (! tmp) {
		    debug ("error: allocation of temporary buffer failed");
		    ret = -1;
		    break;
		}
	    }
	    
	    Tread (xid, pobj_slot->rid, tmp);
	}

	if (fresh || memcmp (p, tmp, p->size)) {
	    debug (" processing %p (%p): object changed, updating...",
		   obj, (void *) p);

	    /* Update persistent image (stale only). */
	    if (! fresh)
		Tset (xid, pobj_slot->rid, p);

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
			&& ! hash_lookup (pobj_update_hash, OBJ2KEY (next)))
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

    hash_free (pobj_update_hash);
    if (tmp)
	XFREE (tmp);

    return ret;
}


static int
pobj_static_set_update_ref (void *static_tmp_ptr, void *obj, int is_set)
{
    void **static_ptr = (void **) static_tmp_ptr;
    int next_occupied;
    recordid tmp_rid;
    int xid;
    int i, next_index;
    struct static_rep_list_item *static_slot;
    struct static_rep_list *tmp_static_rep_list;

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

    tmp_rid.size = sizeof (struct static_rep_list_item);

    if ((i = hash_lookup (g_static_rep_hash, OBJ2KEY (static_ptr))))
    {
	i--;  /* Note: adjust back to original index value... (see below) */
	static_slot = g_static_rep_list[STATIC_REP_SEG (i)].list + STATIC_REP_OFF (i);
    }
    else {
	i = g_static_rep.vacant_head;

	/* Allocate new list segment if space exhausted. */
	if (i == g_static_rep_list_max) {
	    debug ("allocating new segment of static references repository...");
	    
	    debug (" allocating memory");
	    tmp_static_rep_list = (struct static_rep_list *)
		XREALLOC (g_static_rep_list,
			  sizeof (struct static_rep_list) * (g_static_rep.nseg + 1));
	    if (tmp_static_rep_list) {
		g_static_rep_list = tmp_static_rep_list;
		g_static_rep_list[g_static_rep.nseg].list =
		    (struct static_rep_list_item *)
		    XMALLOC (sizeof (struct static_rep_list_item) * STATIC_REP_SEG_MAX);
	    }
	    if (! (tmp_static_rep_list && g_static_rep_list[g_static_rep.nseg].list)) {
		debug (" error: allocation failed");
		pthread_mutex_unlock (&g_static_rep_mutex);
		pobj_end ();
		return -1;
	    }

	    debug (" initializing new segment list");
	    /* Note: don't care for back pointers in vacant list. */
	    static_slot = g_static_rep_list[g_static_rep.nseg].list;
	    next_index = STATIC_REP_SEG_MAX * g_static_rep.nseg + 1;
	    for (i = 0; i < STATIC_REP_SEG_MAX; i++) {
		static_slot->next_index = next_index++;
		static_slot++;
	    }

	    debug (" allocating new segment record");
	    g_static_last_seg.next_seg_rid =
		Talloc (xid, sizeof (struct rep_seg));
	    Tset (xid, g_static_last_seg_rid, &g_static_last_seg);
	    g_static_last_seg_rid = g_static_last_seg.next_seg_rid;
	    memset (&g_static_last_seg, 0, sizeof (struct rep_seg));

	    debug (" allocating new segment list record");
	    g_static_last_seg.list_rid =
		g_static_rep_list[g_static_rep.nseg].rid =
		TarrayListAlloc (xid, STATIC_REP_SEG_MAX, 2,
				 sizeof (struct static_rep_list_item));
	    TarrayListExtend (xid, g_static_last_seg.list_rid, STATIC_REP_SEG_MAX);

	    debug (" dumping new segment and list to persistent storage");
	    /* Note: don't write first element as it's written due to update
	     * anyway. */
	    Tset (xid, g_static_last_seg_rid, &g_static_last_seg);
	    tmp_rid.page = g_static_last_seg.list_rid.page;
	    for (i = 1; i < STATIC_REP_SEG_MAX; i++) {
		tmp_rid.slot = i;
		Tset (xid, tmp_rid, g_static_rep_list[g_static_rep.nseg].list + i);
	    }
	    
	    /* Update segment count.
	     * Note: don't write to persistent image, done later anyway. */
	    g_static_rep.nseg++;
	    g_static_rep_list_max += STATIC_REP_SEG_MAX;

	    debug ("...done (%d segments, %d total slots)",
		   g_static_rep.nseg, g_static_rep_list_max);
	}

	static_slot = g_static_rep_list[STATIC_REP_SEG (i)].list + STATIC_REP_OFF (i);

	/* Untie from vacant list (head). */
	g_static_rep.vacant_head = static_slot->next_index;

	/* Tie into occupied list. */
	next_occupied = g_static_rep.occupied_head;
	if (next_occupied >= 0)
	    g_static_rep_list[STATIC_REP_SEG (next_occupied)].
		list[STATIC_REP_OFF (next_occupied)].prev_index = i;
	static_slot->next_index = next_occupied;
	g_static_rep.occupied_head = i;

	/* Update key field in list item. */
	static_slot->static_ptr = static_ptr;

	/* Insert index to fast lookup table.
	 * Note: we use a hard-coded offset of 1, to distinguish 0 lookup
	 * return value for "no element found" value. */
	hash_insert (g_static_rep_hash, OBJ2KEY (static_ptr), i + 1);

	/* Reflect structural changes to persistent image of repository. */
	Tset (xid, g_boot.static_rep_rid, &g_static_rep);
	if (next_occupied >= 0) {
	    tmp_rid.page = g_static_rep_list[STATIC_REP_SEG (next_occupied)].rid.page;
	    tmp_rid.slot = STATIC_REP_OFF (next_occupied);
	    Tset (xid, tmp_rid,
		  g_static_rep_list[STATIC_REP_SEG (next_occupied)].list
		  + STATIC_REP_OFF (next_occupied));
	}
    }

    /* Update object pointer in list element, and reflect to persistent image. */
    static_slot->objid = obj;
    tmp_rid.page = g_static_rep_list[STATIC_REP_SEG (i)].rid.page;
    tmp_rid.slot = i;
    Tset (xid, tmp_rid,
	  g_static_rep_list[STATIC_REP_SEG (i)].list + STATIC_REP_OFF (i));

    debug ("unlocking static reference repository");
    pthread_mutex_unlock (&g_static_rep_mutex);

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
	      struct hash_table *pobj_convert_hash, int xid)
{
    struct pobj *p;
    void *obj;
    
    debug ("restoring objid=%p size=%d (%d) rid={%d,%d,%ld}",
	   objid, size, POBJ_SIZE (size), rid->page, rid->slot, rid->size);

    /* Allocate memory. */
    p = (struct pobj *) g_memfunc.malloc (POBJ_SIZE (size));
    obj = POBJ2OBJ (p);

    debug ("new object allocated at %p (%p)", obj, (void *) p);

    /* Read object from backing store. */
    Tread (xid, *rid, p);

    debug ("inserting %p->%p conversion pair",
	   objid, obj);

    /* Insert old/new objid pair to conversion table. */
    hash_insert (pobj_convert_hash, OBJ2KEY (objid), (unsigned long) obj);
}

static void *
pobj_adjust (void *old_objid, struct hash_table *pobj_convert_hash, int xid)
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
    int i, i_seg, i_off;
    recordid tmp_rid;
    struct pobj_rep_list_item *pobj_slot;
    
    new_objid =	(void *) hash_lookup (pobj_convert_hash, OBJ2KEY (old_objid));
    debug ("adjusting objid at object's repository entry: %p->%p",
	   old_objid, new_objid);
    
    /* Update objid field in repository. */
    p = OBJ2POBJ (new_objid);
    pobj_slot = POBJ2REPSLOT (p);
    pobj_slot->objid = new_objid;

    /* Update persistent image of repository item. */
    i = p->rep_index;
    i_seg = POBJ_REP_SEG (i);
    i_off = POBJ_REP_OFF (i);
    tmp_rid.page = g_pobj_rep_list[i_seg].rid.page;
    tmp_rid.size = sizeof (struct pobj_rep_list_item);
    tmp_rid.slot = i_off;
    Tset (xid, tmp_rid, g_pobj_rep_list[i_seg].list + i_off);

    /* Traverse persistent reference fields in object, convert old
     * pointer value with new one (using conversion table lookup). */
    debug ("adjusting object reference fields...");
    ref = (void **) new_objid;
    nrefs = POBJ_NREFS (p->size);
    flags_ptr = POBJ2REFS (p);
    flags = *flags_ptr;
    bit = 0;
    mask = 1;
    while (nrefs > 0) {
	if (flags & mask) {
	    newref = (void *) hash_lookup (pobj_convert_hash, OBJ2KEY (*ref));

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

    debug ("...done (%d references adjusted)", changed);

    /* Update persistent image of object. */
    /* TODO: use delta updates, via set_range method -- is that beneficial? */
    Tset (xid, pobj_slot->rid, p);

    return new_objid;
}

/* TODO: possibly use distinct transactions for read/write?
 * Right now doesn't seem to matter, as reads do not generate log entries.
 */
static int
pobj_boot_restore (int xid)
{
    struct hash_table *pobj_convert_hash;
    recordid tmp_rid;
    void *new_objid;
    int i = 0, j;
    int changed = 0;
    int count;
    struct pobj_rep_list_item *pobj_slot;
    struct static_rep_list_item *static_slot;


    debug ("reading boot record");
    Tread (xid, g_boot_rid, &g_boot);

    /*
     * Restore persistent object repository.
     */
    debug ("restoring persistent object repository...");

    debug (" restoring repository control block");
    Tread (xid, g_boot.pobj_rep_rid, &g_pobj_rep);
    
    debug (" allocating memory for %d segments", g_pobj_rep.nseg);
    g_pobj_rep_list = (struct pobj_rep_list *)
	XMALLOC (sizeof (struct pobj_rep_list) * g_pobj_rep.nseg);
    if (g_pobj_rep_list)
	for (i = 0; i < g_pobj_rep.nseg; i++) {
	    g_pobj_rep_list[i].list = (struct pobj_rep_list_item *)
		XMALLOC (sizeof (struct pobj_rep_list_item) * POBJ_REP_SEG_MAX);
	    if (! g_pobj_rep_list[i].list)
		break;
	}
    if (! (g_pobj_rep_list && i == g_pobj_rep.nseg)) {
	debug (" error: allocation failed");
	if (g_pobj_rep_list) {
	    while (i-- > 0)
		XFREE (g_pobj_rep_list[i].list);
	    XFREE (g_pobj_rep_list);
	}
	return -1;
    }

    debug (" reading list segments...");
    g_pobj_last_seg_rid = g_pobj_rep.first_seg_rid;
    g_pobj_rep_list_max = 0;
    for (i = 0; i < g_pobj_rep.nseg; i++) {
	Tread (xid, g_pobj_last_seg_rid, &g_pobj_last_seg);
	g_pobj_rep_list[i].rid = g_pobj_last_seg.list_rid;

	tmp_rid.page = g_pobj_last_seg.list_rid.page;
	tmp_rid.size = sizeof (struct pobj_rep_list_item);
	for (j = 0; j < POBJ_REP_SEG_MAX; j++) {
	    tmp_rid.slot = j;
	    Tread (xid, tmp_rid, g_pobj_rep_list[i].list + j);
	}
	
	g_pobj_rep_list_max += POBJ_REP_SEG_MAX;
	if (g_pobj_last_seg.next_seg_rid.size > 0)
	    g_pobj_last_seg_rid = g_pobj_last_seg.next_seg_rid;
    }
    debug (" ...done (%d segments read)", i);
    
    debug ("...done");
    
    /*
     * Reconstruct heap objects.
     */
    debug ("reconstructing objects and forming conversion table...");
    pobj_convert_hash = hash_new (CONVERT_HASH_NBUCKEXP);
    for (count = 0, i = g_pobj_rep.occupied_head; i >= 0;
	 count++, i = pobj_slot->next_index)
    {
	pobj_slot = g_pobj_rep_list[POBJ_REP_SEG (i)].list + POBJ_REP_OFF (i);
	pobj_restore (pobj_slot->objid, pobj_slot->size, &pobj_slot->rid,
		      pobj_convert_hash, xid);
    }
    debug ("...done (%d objects reconstructed)", count);
    
    /*
     * Restore static references repository.
     */
    debug ("restoring static references repository...");
    
    debug (" restoring repository control block");
    Tread (xid, g_boot.static_rep_rid, &g_static_rep);

    debug (" allocating memory for %d segments", g_static_rep.nseg);
    g_static_rep_list = (struct static_rep_list *)
	XMALLOC (sizeof (struct static_rep_list) * g_static_rep.nseg);
    if (g_static_rep_list)
	for (i = 0; i < g_static_rep.nseg; i++) {
	    g_static_rep_list[i].list = (struct static_rep_list_item *)
		XMALLOC (sizeof (struct static_rep_list_item) * STATIC_REP_SEG_MAX);
	    if (! g_static_rep_list[i].list)
		break;
	}
    if (! (g_static_rep_list && i == g_static_rep.nseg)) {
	debug (" error: allocation failed");
	if (g_static_rep_list) {
	    while (i-- > 0)
		XFREE (g_static_rep_list[i].list);
	    XFREE (g_static_rep_list);
	}
	return -1;
    }

    debug (" reading list segments...");
    g_static_last_seg_rid = g_static_rep.first_seg_rid;
    g_static_rep_list_max = 0;
    for (i = 0; i < g_static_rep.nseg; i++) {
	Tread (xid, g_static_last_seg_rid, &g_static_last_seg);
	g_static_rep_list[i].rid = g_static_last_seg.list_rid;

	tmp_rid.page = g_static_last_seg.list_rid.page;
	tmp_rid.size = sizeof (struct static_rep_list_item);
	for (j = 0; j < STATIC_REP_SEG_MAX; j++) {
	    tmp_rid.slot = j;
	    Tread (xid, tmp_rid, g_static_rep_list[i].list + j);
	}
	
	g_static_rep_list_max += STATIC_REP_SEG_MAX;
	if (g_static_last_seg.next_seg_rid.size > 0)
	    g_static_last_seg_rid = g_static_last_seg.next_seg_rid;
    }
    debug (" ...done (%d segments read)", i);
    
    debug ("...done");

    /*
     * Adjust references within objects.
     */
    debug ("adjusting object references...");
    for (i = g_pobj_rep.occupied_head, count = 0; i >= 0;
	 i = pobj_slot->next_index, count++) {
	pobj_slot = g_pobj_rep_list[POBJ_REP_SEG (i)].list + POBJ_REP_OFF (i);
	pobj_adjust (pobj_slot->objid, pobj_convert_hash, xid);
    }
    debug ("...done (%d objects adjusted)", count);

    /*
     * Adjust/tie static references to objects.
     */
    debug ("tying adjusted static references, building fast lookup table...");
    g_static_rep_hash = hash_new (STATIC_REP_HASH_NBUCKEXP);
    tmp_rid.size = sizeof (struct static_rep_list_item);
    for (i = g_static_rep.occupied_head; i >= 0; i = static_slot->next_index) {
	static_slot = g_static_rep_list[STATIC_REP_SEG(i)].list + STATIC_REP_OFF(i);

	new_objid = (void *) hash_lookup (pobj_convert_hash,
					  OBJ2KEY (static_slot->objid));
	
	debug (" adjusting %p: %p->%p",
	       (void *) static_slot->static_ptr, static_slot->objid, new_objid);
	*static_slot->static_ptr = new_objid;
	static_slot->objid = new_objid;
	
	/* Note: we apply a deliberate offset of 1 to index values,
	 * so as to be able to distinguish a 0 index value from "key not found"
	 * on subsequent table lookups. */
	hash_insert (g_static_rep_hash, OBJ2KEY (static_slot->static_ptr), i + 1);

	tmp_rid.page = g_static_rep_list[STATIC_REP_SEG(i)].rid.page;
	tmp_rid.slot = i;
	Tset (xid, tmp_rid, static_slot);
	changed++;
    }
    debug ("...done (%d tied)", changed);

    hash_free (pobj_convert_hash);
    return 0;
}


static int
pobj_boot_init (int xid)
{
    recordid tmp_rid;
    int i, next_index;
    struct pobj_rep_list_item *pobj_slot;
    struct static_rep_list_item *static_slot;

    /*
     * Create a single-segment persistent object repository.
     */
    debug ("creating persistent object repository...");

    debug (" allocating memory");
    g_pobj_rep_list = (struct pobj_rep_list *)
	XMALLOC (sizeof (struct pobj_rep_list));
    if (g_pobj_rep_list)
	g_pobj_rep_list[0].list = (struct pobj_rep_list_item *)
	    XMALLOC (sizeof (struct pobj_rep_list_item) * POBJ_REP_SEG_MAX);
    if (! (g_pobj_rep_list && g_pobj_rep_list[0].list)) {
	debug (" error: allocation failed");
	if (g_pobj_rep_list)
	    XFREE (g_pobj_rep_list);
	return -1;
    }

    debug (" initializing in-memory single-segment object list");
    /* Note: don't care for back pointers in vacant list. */
    pobj_slot = g_pobj_rep_list[0].list;
    next_index = 1;
    for (i = 0; i < POBJ_REP_SEG_MAX; i++) {
	pobj_slot->next_index = next_index++;
	pobj_slot++;
    }
    g_pobj_rep_list_max = POBJ_REP_SEG_MAX;

    debug (" allocating first segment record");
    g_pobj_last_seg_rid = Talloc (xid, sizeof (struct rep_seg));
    memset (&g_pobj_last_seg, 0, sizeof (struct rep_seg));

    debug (" allocating first segment object list record");
    g_pobj_last_seg.list_rid = g_pobj_rep_list[0].rid =
	TarrayListAlloc (xid, POBJ_REP_SEG_MAX, 2,
			 sizeof (struct pobj_rep_list_item));
    TarrayListExtend (xid, g_pobj_last_seg.list_rid, POBJ_REP_SEG_MAX);

    debug (" dumping first segment and object list");
    Tset (xid, g_pobj_last_seg_rid, &g_pobj_last_seg);
    tmp_rid.page = g_pobj_last_seg.list_rid.page;
    tmp_rid.size = sizeof (struct pobj_rep_list_item);
    for (i = 0; i < POBJ_REP_SEG_MAX; i++) {
	tmp_rid.slot = i;
	Tset (xid, tmp_rid, g_pobj_rep_list[0].list + i);
    }

    debug (" initializing repository control block");
    g_pobj_rep.first_seg_rid = g_pobj_last_seg_rid;
    g_pobj_rep.nseg = 1;
    g_pobj_rep.vacant_head = 0;
    g_pobj_rep.occupied_head = -1;  /* i.e. empty list. */
    g_boot.pobj_rep_rid = Talloc (xid, sizeof (struct rep));
    Tset (xid, g_boot.pobj_rep_rid, &g_pobj_rep);

    debug ("...done");

    /*
     * Create a single-segment static reference repository.
     */
    debug ("creating static reference repository...");

    debug (" allocating memory");
    g_static_rep_list = (struct static_rep_list *)
	XMALLOC (sizeof (struct static_rep_list));
    if (g_static_rep_list)
	g_static_rep_list[0].list = (struct static_rep_list_item *)
	    XMALLOC (sizeof (struct static_rep_list_item) * STATIC_REP_SEG_MAX);
    if (! (g_static_rep_list && g_static_rep_list[0].list)) {
	debug (" error: allocation failed");
	if (g_static_rep_list)
	    XFREE (g_static_rep_list);
	return -1;
    }

    debug (" initializing in-memory single-segment static reference list");
    /* Note: don't care for back pointers in vacant list. */
    static_slot = g_static_rep_list[0].list;
    next_index = 1;
    for (i = 0; i < STATIC_REP_SEG_MAX; i++) {
	static_slot->next_index = next_index++;
	static_slot++;
    }
    g_static_rep_list_max = STATIC_REP_SEG_MAX;

    debug (" allocating first segment record");
    g_static_last_seg_rid = Talloc (xid, sizeof (struct rep_seg));
    memset (&g_static_last_seg, 0, sizeof (struct rep_seg));

    debug (" allocating first segment static list record");
    g_static_last_seg.list_rid = g_static_rep_list[0].rid =
	TarrayListAlloc (xid, STATIC_REP_SEG_MAX, 2,
			 sizeof (struct static_rep_list_item));
    TarrayListExtend (xid, g_static_last_seg.list_rid, STATIC_REP_SEG_MAX);

    debug (" dumping first segment and static list");
    Tset (xid, g_static_last_seg_rid, &g_static_last_seg);
    tmp_rid.page = g_static_last_seg.list_rid.page;
    tmp_rid.size = sizeof (struct static_rep_list_item);
    for (i = 0; i < STATIC_REP_SEG_MAX; i++) {
	tmp_rid.slot = i;
	Tset (xid, tmp_rid, g_static_rep_list[0].list + i);
    }

    debug (" initializing repository control block");
    g_static_rep.first_seg_rid = g_static_last_seg_rid;
    g_static_rep.nseg = 1;
    g_static_rep.vacant_head = 0;
    g_static_rep.occupied_head = -1;  /* i.e. empty list. */
    g_boot.static_rep_rid = Talloc (xid, sizeof (struct rep));
    Tset (xid, g_boot.static_rep_rid, &g_static_rep);

    g_static_rep_hash = hash_new (STATIC_REP_HASH_NBUCKEXP);

    debug ("...done");
    

    debug ("writing boot record...");
    Tset (xid, g_boot_rid, &g_boot);
    debug ("...done");

    return 0;
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
	    g_memfunc.malloc = ext_memfunc->malloc;
	if (ext_memfunc->calloc)
	    g_memfunc.calloc = ext_memfunc->calloc;
	if (ext_memfunc->realloc)
	    g_memfunc.realloc = ext_memfunc->realloc;
	if (ext_memfunc->free)
	    g_memfunc.free = ext_memfunc->free;
    }

    /* Initialize internal memory manager. */
    if (int_memfunc)
	xmem_memfunc (int_memfunc->malloc, int_memfunc->realloc,
		      int_memfunc->free);

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
	Tdealloc (xid, tmp_rid);

	debug ("restoring previous state");
	if (pobj_boot_restore (xid) < 0) {
	    debug ("error: restore failed, init aborted");
	    ret = -1;
	}
	else
	    ret = 1;
    }
    else {
	debug ("initializing structures on first time use");
	if (pobj_boot_init (xid) < 0) {
	    debug ("error: first time init failed, init aborted");
	    ret = -1;
	}
	else
	    ret = 0;
    }

    if (ret < 0) {
	Tabort (xid);
	pthread_mutex_unlock (&g_pobj_rep_mutex);
	pthread_mutex_unlock (&g_static_rep_mutex);
	return -1;
    }

    Tcommit (xid);

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
    
    hash_free (g_static_rep_hash);
    Tdeinit ();
    g_is_init = 0;

    debug ("unlocking");
    pthread_mutex_unlock (&g_pobj_rep_mutex);
    pthread_mutex_unlock (&g_static_rep_mutex);

    debug ("shutdown complete");

    return 0;
}
