#include <stdio.h>
#include <stdlib.h>
#include <pobj/pobj.h>


struct item {
    int val;
    struct item *next;
    int dummy[45];
};
int item_ref_fields[] = {
    member_offset(struct item, next),
    -1
};

int
main (int argc, char **argv)
{
    static struct item *list = NULL;
    struct item *tmp, *next;
    int i;
    
    switch (pobj_init (NULL, NULL)) {
    case 0:
	printf ("first-time init\n");
	/* Build list. */
	next = NULL;
	for (i = 0; i < 10; i++) {
	    pobj_start ();
	    tmp = (struct item *) pobj_malloc (sizeof (struct item));
	    if (! tmp) {
		printf ("allocation error\n");
		abort ();
	    }
	    
	    /* pobj_ref_typify (tmp, item_ref_fields); */
	    
#if 0
	    tmp->val = i;
	    tmp->next = next;
#endif
	    pobj_static_set_ref (&list, tmp);
	    POBJ_SET_INT (tmp, val, i);
	    /* Intended crash code... */
#if 0
	    if (i == 7)
		abort ();
#endif
	    POBJ_SET_REF (tmp, next, next);
	    pobj_end ();
	    
	    /* pobj_update (tmp); */
	    next = tmp;
	}
	/* pobj_update_recursive (tmp); */
	/* pobj_static_set_ref (&list, tmp); */
	/* list = tmp; */
	/* pobj_static_update_ref (&list); */
	break;

    case 1:
	printf ("subsequent init\n");
	break;

    case -1:
	printf ("init error\n");
	break;

    default:
	printf ("unknown return value\n");
    }

    /* Print list. */
    printf ("printing list...\n");
    for (tmp = list; tmp; tmp = tmp->next)
	printf ("%p: val=%d next=%p\n",
		(void *) tmp, tmp->val, (void *) tmp->next);
    printf ("...done\n");

    pobj_shutdown ();

    return 0;
}
