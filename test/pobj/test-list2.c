#include <stdio.h>
#include <stdlib.h>
#include <pobj/pobj.h>


struct data {
    int val;
};

struct item {
    int val;
    struct data *data;
    struct item *next;
    int dummy[45];
};
int item_ref_fields[] = {
    member_offset(struct item, data),
    member_offset(struct item, next),
    -1
};

int
main (int argc, char **argv)
{
    static struct item *list = NULL;
    struct data *data;
    struct item *tmp, *next;
    int i;
    
    switch (pobj_init (NULL, NULL)) {
    case 0:
	printf ("first-time init\n");
	/* Build list. */
	next = NULL;
	for (i = 0; i < 15; i++) {
	    tmp = (struct item *) pobj_malloc_transient (sizeof (struct item));
	    data = (struct data *) pobj_malloc_transient (sizeof (struct data));
	    if (! (tmp && data)) {
		printf ("allocation error\n");
		abort ();
	    }
	    pobj_ref_typify (tmp, item_ref_fields);
	    
	    data->val = -i;
	    tmp->val = i;
	    tmp->data = data;
	    tmp->next = next;

	    next = tmp;
	}
	pobj_update_recursive (tmp, 1);
	pobj_static_set_ref (&list, tmp);
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
	printf ("%p: val=%d next=%p data=%p data->val=%d\n",
		(void *) tmp, tmp->val, (void *) tmp->next,
		(void *) tmp->data, (tmp->data ? tmp->data->val : 0));
    printf ("...done\n");

    pobj_shutdown ();


    /* Open again, see what happens... */
    switch (pobj_init (NULL, NULL)) {
    case 0:
	printf ("first-time init (error)\n");
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
	printf ("%p: val=%d next=%p data=%p data->val=%d\n",
		(void *) tmp, tmp->val, (void *) tmp->next,
		(void *) tmp->data, (tmp->data ? tmp->data->val : 0));
    printf ("...done\n");

    pobj_shutdown ();


    return 0;
}
