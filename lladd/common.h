/*---
This software is copyrighted by the Regents of the University of
California, and other parties. The following terms apply to all files
associated with the software unless explicitly disclaimed in
individual files.

The authors hereby grant permission to use, copy, modify, distribute,
and license this software and its documentation for any purpose,
provided that existing copyright notices are retained in all copies
and that this notice is included verbatim in any distributions. No
written agreement, license, or royalty fee is required for any of the
authorized uses. Modifications to this software may be copyrighted by
their authors and need not follow the licensing terms described here,
provided that the new terms are clearly indicated on the first page of
each file where they apply.

IN NO EVENT SHALL THE AUTHORS OR DISTRIBUTORS BE LIABLE TO ANY PARTY
FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES
ARISING OUT OF THE USE OF THIS SOFTWARE, ITS DOCUMENTATION, OR ANY
DERIVATIVES THEREOF, EVEN IF THE AUTHORS HAVE BEEN ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

THE AUTHORS AND DISTRIBUTORS SPECIFICALLY DISCLAIM ANY WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
NON-INFRINGEMENT. THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, AND
THE AUTHORS AND DISTRIBUTORS HAVE NO OBLIGATION TO PROVIDE
MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.

GOVERNMENT USE: If you are acquiring this software on behalf of the
U.S. government, the Government shall have only "Restricted Rights" in
the software and related documentation as defined in the Federal
Acquisition Regulations (FARs) in Clause 52.227.19 (c) (2). If you are
acquiring the software on behalf of the Department of Defense, the
software shall be classified as "Commercial Computer Software" and the
Government shall have only "Restricted Rights" as defined in Clause
252.227-7013 (c) (1) of DFARs. Notwithstanding the foregoing, the
authors grant the U.S. Government and others acting in its behalf
permission to use and distribute the software in accordance with the
terms specified in this license.
---*/
/**
 * @file 
 *
 * A standard header file, adopted from Autobook. 
 *
 * http://sources.redhat.com/autobook/
 *
 * The idea behind this file is twofold.  First, we want to keep as
 * much of the #ifdef portability nonsense in here as possible.
 * Second, we allow users to #include headers that in turn #include
 * common.h.  If they do so, then their code should continue to 'do
 * the right thing' and build, even though they do not #include the
 * config.h file that all of the LLADD stuff uses.
 *
 * @todo Need to make sure every .c file actually includes this thing, and
 * also includes constants.h, and that no .h files include config.h
 *
 * $Id$
 */

//#define NDEBUG 1

#ifndef __lladd_common_h
#define __lladd_common_h

#ifdef __cplusplus
#  define BEGIN_C_DECLS extern "C" {
#  define END_C_DECLS   }
#else /* !__cplusplus */
#  define BEGIN_C_DECLS
#  define END_C_DECLS
#endif /* __cplusplus */

#include <stdint.h> // uint32, et. al.
#include <limits.h>

/* Should be included by the .c files only. :( */
/*#if HAVE_CONFIG_H
 #  include "config.h"
 #endif */

#if STDC_HEADERS
#  include <stdlib.h>
#  include <string.h>
#elif HAVE_STRINGS_H
#  include <strings.h>
#endif /*STDC_HEADERS*/

#if HAVE_UNISTD_H
#  include <unistd.h>
#endif

#if HAVE_ERRNO_H
#  include <errno.h>
#endif /*HAVE_ERRNO_H*/
#ifndef errno
/* Some systems #define this! */
extern int errno;
#endif

//#define byte unsigned char
//#define lsn_t long

typedef unsigned char byte;
//@todo lsn_t should be unsigned.
// If it were unsigned, it could be typedef'ed from size_t.
typedef int64_t lsn_t;
#define LSN_T_MAX (INT64_MAX)


/*#define DEBUGGING   */
/*#define PROFILE_LATCHES*/
/*#define NO_LATCHES */

#ifdef DEBUGGING 
/** @todo Files that use DEBUG have to pull in stdio.h, which is a pain! */
#define DEBUG(...) \
  printf(__VA_ARGS__); fflush(NULL) 
#else 
#define DEBUG(...)

#include "compensations.h"

#endif /*DEBUGGING*/

#endif /* __lladd_common_h */

