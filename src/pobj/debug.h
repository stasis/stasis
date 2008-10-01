#ifndef __DEBUG_H
#define __DEBUG_H

#include <stdio.h>

/* FIXME: send that to the make system. */
#define HAVE_DEBUG
#define HAVE_DEBUG_CONTEXTS

#ifdef HAVE_DEBUG
# ifdef HAVE_DEBUG_CONTEXTS
#  define debug(...)  debug_f_printf(__FUNCTION__, __LINE__,  \
				     ##__VA_ARGS__)
#  define debug_start()      debug_f_start(__FUNCTION__)
#  define debug_end()        debug_f_end()
int debug_f_printf (const char *, const int, const char *, ...);
int debug_f_start(const char *);
int debug_f_end();
# else
#  define debug(format,...)  \
    fprintf (stderr, "[%.13s%*c%4d] ",  \
	     __FUNCTION__,  \
	     (strlen(__FUNCTION__) > 13 ? 1 : 14 - strlen(__FUNCTION__)),  \
	     ':', __LINE__),  \
    fprintf (stderr, format, ##__VA_ARGS__), fprintf (stderr, "\n"), fflush (stderr)
#  define debug_start()  ((void)0)
#  define debug_end()    ((void)0)
# endif /* HAVE_DEBUG_CONTEXTS */
#else
# define debug(format,...)  ((void)0)
# define debug_start()      ((void)0)
# define debug_end()        ((void)0)
#endif /* HAVE_DEBUG */

#endif /* __DEBUG_H */

