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
 * @todo:  Need to make sure everyone actually includes this thing, and also includes constants.h
 *
 * @ingroup LLADD_CORE
 *
 * $Id$
 */

#ifndef __lladd_common_h
#define __lladd_common_h


#ifdef __cplusplus
#  define BEGIN_C_DECLS extern "C" {
#  define END_C_DECLS   }
#else /* !__cplusplus */
#  define BEGIN_C_DECLS
#  define END_C_DECLS
#endif /* __cplusplus */

/* Should be included by the .c files only. :( */
/*#if HAVE_CONFIG_H
 #  include "config.h"
 #endif */

#include <stdio.h>
#include <sys/types.h>

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

#define byte unsigned char
#define lsn_t  off_t



#define DEBUGGING
/**
   @todo  Currently, latches obtained using rw.h are not currently profiled.
*/
#define PROFILE_LATCHES


#ifdef DEBUGGING 
#define DEBUG(...) \
  printf(__VA_ARGS__); fflush(NULL) 
#else 
#define DEBUG(...)

#endif /*DEBUGGING*/



#include <pthread.h>
#include <pbl/pbl.h>
#include <lladd/stats.h>

/** 
    @todo Is it useful to track per-line spin counts? 
*/
/*typedef struct {
  double sum_x_hold;
  double sum_xsquared_hold;
  long   max_hold;
  long   count;
} lladd_locking_profile_tuple;
*/

/**
   A data structure for profiling latching behavior.

   All time values recorded in this struct are in microseconds.
 */
typedef struct {
  const char * file;
  int line;
  const char * name;
  pthread_mutex_t mutex;
  profile_tuple tup;
  char * last_acquired_at;
  pblHashTable_t * lockpoints;
} lladd_pthread_mutex_t;

#include <libdfa/rw.h>

/**
   Keeps some profiling information along with a read/write lock. 
*/

typedef struct {
  const char * file;
  int line;
  rwl * lock;
  profile_tuple tup;
  char * last_acquired_at;
  pblHashTable_t * lockpoints;
} __profile_rwl;

#ifdef PROFILE_LATCHES

#define pthread_mutex_t lladd_pthread_mutex_t

#define pthread_mutex_init(x, y) __lladd_pthread_mutex_init((x), (y), __FILE__, __LINE__, #x)
#define pthread_mutex_destroy(x) __lladd_pthread_mutex_destroy((x))
#define pthread_mutex_lock(x) __lladd_pthread_mutex_lock((x), __FILE__, __LINE__)
#define pthread_mutex_unlock(x) __lladd_pthread_mutex_unlock((x))
#define pthread_mutex_trylock(x) NO_PROFILING_EQUIVALENT_TO_PTHREAD_TRYLOCK

int __lladd_pthread_mutex_init(lladd_pthread_mutex_t  *mutex,  const  pthread_mutexattr_t *mutexattr, const char * file, int line, const char * mutex_name);
int __lladd_pthread_mutex_lock(lladd_pthread_mutex_t *mutex, char * file, int line);
int __lladd_pthread_mutex_unlock(lladd_pthread_mutex_t *mutex);
int __lladd_pthread_mutex_destroy(lladd_pthread_mutex_t *mutex);

#define initlock() __profile_rw_initlock(__FILE__, __LINE__)
#define readlock(x, y) __profile_readlock((x),(y), __FILE__, __LINE__)
#define writelock(x, y) __profile_writelock((x), (y), __FILE__, __LINE__)
#define readunlock(x) __profile_readunlock((x))
#define writeunlock(x) __profile_writeunlock((x))
#define deletelock(x) __profile_deletelock((x))

#define rwl __profile_rwl

rwl *__profile_rw_initlock (char * file, int line);
void __profile_readlock (rwl *lock, int d, char * file, int line);
void __profile_writelock (rwl *lock, int d, char * file, int line);
void __profile_readunlock (rwl *lock);
void __profile_writeunlock (rwl *lock);
void __profile_deletelock (rwl *lock);


#endif  

#endif /* __lladd_common_h */

