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
 * Manages the page buffer

    pageManager - Provides cached page handling, delegates to blob
    manager when necessary.  Doesn't implement an eviction policy.
    That is left to a cacheManager.  (Multiple cacheManagers could be
    used with a single page manager.)

  @todo Allow error checking!  
 
  @todo Refactoring for lock manager
 
  Possible interface for lockManager:

       Define three classes of objects that the lock manager is interested in:

         Transactions,
	 Operations,
	 Predicates.

       LLADD already has operations and transactions, and these can be
       relatively unchanged.  Predicates are read only operations that
       return a set of tuples.  Tread() is the simplest predicate.
       Index scans provide a motivating example.  

       See http://research.microsoft.com/%7Eadya/pubs/icde00.pdf
       (Generalized Isolation Level Definitions, Adya, Liskov, O'Neil,
       2000) for a theoretical discussion of general locking schemes..

       Locking functions can return errors such as DEADLOCK, etc.
       When such a value is returned, the transaction aborts, and an
       error is passed up to the application.

 * @ingroup LLADD_CORE
 * $Id$
 */

#include <lladd/constants.h>
#include <lladd/transactional.h>

#ifndef __BUFFERMANAGER_H__
#define __BUFFERMANAGER_H__

BEGIN_C_DECLS

/**
   Page is defined in bufferManager.h as an incomplete type to enforce
   an abstraction barrier between page.h and the rest of the system.

   If you need to muck with page internals, first consider the
   implications that doing so has on locking.  In particular, rwlatch
   is currently entirely handled in page.c.
*/
typedef struct Page_s Page_s;
typedef struct Page_s Page;

/**
 * @param pageid ID of the page you want to load
 * @return fully formed Page type
 */
compensated_function Page * loadPage(int xid, int pageid);

/**
   loadPage aquires a lock when it is called, effectively pinning it
   in memory.  releasePage releases this lock.
*/
void releasePage(Page * p);

/**
 * initialize buffer manager
 * @return 0 on success
 * @return error code on failure
 */
int bufInit();

/**
 * will write out any dirty pages, assumes that there are no running
 * transactions
 */
void bufDeinit();

END_C_DECLS

#endif
