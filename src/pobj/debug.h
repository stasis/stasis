#ifndef __DEBUG_H
#define __DEBUG_H

#include <stdio.h>

/* FIXME: send that to the make system. */
/* #define HAVE_DEBUG */

#ifdef HAVE_DEBUG
#define debug(format,...)  \
    fprintf (stderr, "[%.13s%*c:%4d] " format "\n",      \
	     __FUNCTION__, (strlen(__FUNCTION__) > 13 ? 1 : 14 - strlen(__FUNCTION__)),  \
	     ' ', __LINE__, ##__VA_ARGS__),            \
    fflush (stderr)
#else
#define debug(format,...)  ((void)0)
#endif /* HAVE_DEBUG */

#endif /* __DEBUG_H */

