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
   @file

   @ingroup ARRAY_LIST

   @defgroup ARRAY_LIST ArrayList

   O(1) growable array.

   ArrayList provides an growable array of fixed length records with
   O(1) complexity for all operations.  The list grows by doubling the
   amount of space it reserves each time the list is extended.
   Therefore, this data structure can use up to twice as much storage
   as is strictly necessary.

   These arrays are similar to those in Java's ArrayList class.
   Essentially, the base page contains a fixed size array of recordid's
   pointing at contiguous blocks of pages.  Each block is twice as big
   as the previous block.

   The base block is of type FIXED_PAGE, of int values. The first few slots
   are reserved and store information about the arrayList (size of
   array entries, number/size of allocated regions, etc.)

   @todo arrayList's base block should store pageid_t values, not ints.

   @ingroup COLLECTIONS

   $Id$
*/


/** @ingroup ARRAY_LIST */
/** @{ */
#ifndef __ARRAY_LIST_H
#define __ARRAY_LIST_H
#include <stasis/operations.h>
/** Allocate a new array list.

    @param xid The transaction allocating the new arrayList.

    @param numPages The number of pages to be allocated as part of the
           first region.  (The arraylist starts with zero capacity
           regardless of this parameter's value.)

    @param multiplier Each time the array list runs out of space, the
           allocate multipler times more space than last time.

    @param recordSize The size of the things stored in this arrayList.
           Must fit on a single page (arrayList cannot store blobs.)

*/

recordid TarrayListAlloc(int xid, pageid_t numPages, int multiplier, int recordSize);
void TarrayListDealloc(int xid, recordid rid);

/**
   Extend the ArrayList in place and zero out the newly allocated records.

   @param xid the transaction performing the expansion
   @param rid the recordid pointing to the ArrayList.
   @param slots the number of slots to end to the end of the ArrayList.
 */
compensated_function int TarrayListExtend(int xid, recordid rid, int slots);
/**
   Get the length of an ArrayList.

   @param xid the transaction performing the expansion
   @param rid the recordid pointing to the ArrayList.
   @return The number of items stored in the ArrayList.
 */
compensated_function int TarrayListLength(int xid, recordid rid);

/** Used by Tread() and Tset() to map from arrayList index to recordid. */
recordid stasis_array_list_dereference_recordid(int xid, Page * p, int offset);

stasis_operation_impl stasis_op_impl_array_list_header_init();
/** @} */
#endif
