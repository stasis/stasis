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
 * Methods that change the contents of a record.  An entire record can
 * be updated at once, or portions of the record can be updated.
 *
 * @ingroup OPERATIONS
 *
 * $Id$
 * 
 **********************************************/

#ifndef __SET_H__
#define __SET_H__

/**
    Changes the value of a record.
    @param xid the transaction id
    @param rid the recordid of the record to be changed.
    @param dat the new value of the record.
*/ 
int Tset(int xid, recordid rid, const void * dat);
Page * TsetWithPage(int xid, recordid rid, Page *p, const void * dat);

int TsetRaw(int xid, recordid rid, const void * dat);

stasis_operation_impl stasis_op_impl_set();
stasis_operation_impl stasis_op_impl_set_inverse();

stasis_operation_impl stasis_op_impl_set_range();
stasis_operation_impl stasis_op_impl_set_range_inverse();

/**

  Change an interval of bytes within a record.

  @todo TsetRange could be faster.  Although it uses log space
  efficiently, it performs a number of extra memcpy() calls over the
  entire record.
*/
void TsetRange(int xid, recordid rid, int offset, int length, const void * dat);

#endif
