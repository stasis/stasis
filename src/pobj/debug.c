#include <stdarg.h>
#include <string.h>
#include "debug.h"


#ifdef HAVE_DEBUG_CONTEXTS

#define NCONTEXT_MAX  128
static char g_context_prefix[NCONTEXT_MAX * 2];
static char *g_context_funcs[NCONTEXT_MAX];
static int g_ncontexts = 0;
static int g_high_ncontexts = 0;
static int g_low_ncontexts = 0;

int
debug_f_printf (const char *func_str, const int line, const char *format, ...)
{
    static int is_first = 1;
    va_list args;
    int n;
    int adhoc;
    int i;

    
    if (is_first) {
	for (i = 0; i < NCONTEXT_MAX * 2; i += 2) {
	    g_context_prefix[i] = '|';
	    g_context_prefix[i + 1] = ' ';
	}
	is_first = 0;
    }

    if (g_ncontexts == 0 || strcmp (func_str, g_context_funcs[g_ncontexts - 1]))
	adhoc = 1;
    else
	adhoc = 0;
    
    while (--g_high_ncontexts > g_low_ncontexts) {
	fprintf (stderr, "    %.*s\n",
		 g_high_ncontexts * 2, g_context_prefix);
    }
    g_high_ncontexts = g_ncontexts;
    while (g_low_ncontexts < g_ncontexts) {
	fprintf (stderr, "    %.*s+-[%s]\n",
		 g_low_ncontexts * 2, g_context_prefix,
		 g_context_funcs[g_low_ncontexts]);
	g_low_ncontexts++;
    }
    
    va_start (args, format);
    n = fprintf (stderr, "%4d%.*s%s", line, g_ncontexts * 2, g_context_prefix,
		 (adhoc ? "~" : ""));
    n += vfprintf (stderr, format, args);
    n += fprintf (stderr, "\n");
    va_end (args);

    return n;
}

int
debug_f_start (const char *func_str)
{
    if (g_ncontexts == NCONTEXT_MAX)
	return -1;
    g_context_funcs[g_ncontexts++] = (char *) func_str;
    return 0;
}

int
debug_f_end (void)
{
    if (g_ncontexts == 0)
	return -1;
    g_ncontexts--;
    if (g_ncontexts < g_low_ncontexts)
	g_low_ncontexts = g_ncontexts;
    return 0;
}

#endif /* HAVE_DEBUG_CONTEXTS */

