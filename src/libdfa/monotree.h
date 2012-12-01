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
#ifndef _MONOTREE_H
#define _MONOTREE_H
/**
   Provides a binary tree that provides constant time insertion, but
   only accepts monotonically increasing values.  It's stored in an
   array, and periodically compacts itself with a little garbage
   collector.  Even though items must be inserted in sorted order, but
   can be removed in an arbitrary order.
  
   This data structure is hardcoded to operate on StateMachine
   structs, but could be easily generalized with a small performance
   hit.

   This library should be stable.

   TODO: If you pass NULL_STATE (ULONG_MAX) into any of these functions, the
   result is undefined.  This libary should check this instead of
   causing arbitrary memory corruption.
*/

#include <libdfa/statemachine.h>
#include <libdfa/messages.h>

typedef struct monoTree {
  StateMachine * buffer;
  int              size;
  int              low_water_mark;
  int              high_water_mark;
  state_machine_id next_id;
} MonoTree;

/**
   Usage:

    rb = stasis_alloc (MonoTree);
    rb->buffer = stasis_malloc(rb_size, StateMachine);

    init_MonoTree(rb, rb_size);

*/
void init_MonoTree(MonoTree * rb, int size);
/** 
   Returns a pointer to a new machine, or null if the 
   buffer is full. 
*/
StateMachine *   allocMachine (MonoTree * rb);
void             freeMachine  (MonoTree * rb, state_machine_id id);
StateMachine *   getMachine   (MonoTree * rb, state_machine_id id);
StateMachine *   insertMachine(MonoTree * rb, state_machine_id id); 
StateMachine *   enumerateMachines(MonoTree * rb, int * count);
#endif
