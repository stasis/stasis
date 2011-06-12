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
 * much of the \#ifdef portability nonsense in here as possible.
 * Second, we allow users to \#include headers that in turn \#include
 * common.h.  If they do so, then their code should continue to 'do
 * the right thing' and build, even if they do not \#include the
 * config.h file that all of the Stasis stuff uses.
 *
 * $Id$
 */

//#define NDEBUG 1

#ifndef __stasis_common_h
#define __stasis_common_h
#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 600
#endif
#ifndef _BSD_SOURCE
#define _BSD_SOURCE
#endif

#include <stdint.h> // uint32, et. al.  (has to be before sys/types.h for mcpp atop some broken gcc headers)
#include <fcntl.h>
#include <sys/types.h> // for size_t

#ifdef __cplusplus
#  define BEGIN_C_DECLS extern "C" {
#  define END_C_DECLS   }
#else /* !__cplusplus */
#  define BEGIN_C_DECLS
#  define END_C_DECLS
#endif /* __cplusplus */

#include <limits.h>

//#if STDC_HEADERS
//#  include <stdlib.h>
//#  include <string.h>
//#elif HAVE_STRINGS_H
//#  include <strings.h>
//#endif /*STDC_HEADERS*/
//
//#if HAVE_UNISTD_H
//#  include <unistd.h>
//#endif
//
//#if HAVE_ERRNO_H
//#  include <errno.h>
//#endif /*HAVE_ERRNO_H*/
//#ifndef errno
// /* Some systems define this! */
//extern int errno;
//#endif

#include<stdlib.h>
#include<string.h>
#include<unistd.h>
#include<errno.h>

//#define byte unsigned char
//#define lsn_t long

typedef unsigned char byte;
//@todo lsn_t should be unsigned.
// If it were unsigned, it could be typedef'ed from size_t.
// Setting these to int64_t makes printf cranky. :(
typedef long long lsn_t;
#define LSN_T_MAX    INT64_MAX
typedef long long pageid_t;
#define PAGEID_T_MAX INT64_MAX
typedef int32_t slotid_t;
#define SLOTID_T_MAX INT32_MAX
typedef uint16_t pageoff_t;
#define PAGEOFF_T_MAX UINT16_MAX;

typedef int16_t pagetype_t;
#define PAGETYPE_T_MAX INT16_MAX;

/*#define DEBUGGING   */
/*#define PROFILE_LATCHES*/
/*#define NO_LATCHES */


// ICK: Only way to portably shut up GCC warnings about variadic macros
#pragma GCC system_header

#ifdef DEBUGGING
/** @todo Files that use DEBUG have to pull in stdio.h, which is a pain! */
#define DEBUG(...) \
  printf(__VA_ARGS__); fflush(NULL)
#else
#define DEBUG(...)
#endif /*DEBUGGING*/

/**
 * represents how to look up a record on a page
 * @todo int64_t (for recordid.size) is a stopgap fix.
 */
#pragma pack(push,1)
typedef struct {
  pageid_t page;
  slotid_t slot;
  int64_t size;
} recordid;
#pragma pack(pop)

// TODO move blob_record_t into an operation header.
#pragma pack(push,1)
typedef struct {
  size_t offset;
  size_t size;
  // unsigned fd : 1;
} blob_record_t;
#pragma pack(pop)

/*
   Define Page as an incomplete type to hide its implementation from clients.

   Include stasis/page.h for the complete definition.
*/
typedef struct Page_s Page;

extern long long *stasis_dbug_timestamp;

#define STLSEARCH

#include <pthread.h>

#endif /* __stasis_common_h */
