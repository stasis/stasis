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


#ifndef _STATEMACHINE_H
#define _STATEMACHINE_H

#include <time.h>
#include <lladd/transactional.h>
#include <libdfa/messages.h>
#include "../lladd/lladd/page.h"
#include <sys/types.h>
#include <pthread.h>

#define MAX_STATE_NAME UCHAR_MAX-1
/* 

   If a transition's post state is OVERRIDDEN_STATE, then the new
   state is read from the return value of the callback_fcn.  If
   callback_fcn returns OVERRIDDEN_STATE, then no transition is
   performed.

   Otherwise, then callback_fcn returns a standard boolean value.
   (0-> no transition, !=0 -> take transition.)

*/

#define MAX_STATE_COUNT UCHAR_MAX+1

#define MAX_APP_STATE_SIZE (sizeof(int) * 32)

#define NULL_MACHINE (ULONG_MAX-1)
#define NULL_STATE MAX_STATE_NAME
#define NULL_STATE_TOMBSTONE (MAX_STATE_NAME-1)
#define OVERRIDDEN_STATE (MAX_STATE_NAME-2)
#define START_STATE 0

typedef unsigned char dfa_bool;

typedef unsigned char state_name;

/**
   This struct contains all of the information associated with an
   instance of a state machine.  When combined with a set of states and
   transitions, this provides enough information to actually execute a machine.

   The page record contains application specific state, which must be
   stored in transactional, or otherwise reliable storage.
   */

typedef struct stateMachine {
   /**  The unique identifier of this state machine.  */
  state_machine_id machine_id;
  /**  The wall-clock time of the last transition made by this
       machine.  (Used to abort hung machines.)*/
  time_t           last_transition;
  /**  A pointer to this state machine's page in memory. */
  Page *           page;
  /**  The recordid of page  (for recovery) */
  recordid         page_id;
  /**  The current state of this machine, or NULL_STATE for machines
       that should be garbage collected. */
  state_name       current_state;
  pthread_t        worker_thread;
  int              pending;
  pthread_mutex_t * mutex;
  pthread_cond_t  * sleepCond;
  Message          message;
  char             message_recipient[MAX_ADDRESS_LENGTH];
  int              app_state[MAX_APP_STATE_SIZE];
} StateMachine;


typedef state_name(callback_fcn)(void * dfaSet, StateMachine * stateMachine, Message * m, char * from); 


/* All function pointers follow this prototype: 

   TODO

   and may be null if no function needs to be executed.

*/

typedef struct state {
  /** The name of this state.  Usually just the index of this state in the states array.
   */
  state_name    name;
  /** A function pointer that will be periodically executed when the machine is in this state.  Most network
      transmits belong here, while 'guards' belong in fcn_ptr in the Transistion struct. 
   */
  callback_fcn* retry_fcn;
  /** NULL unless the machine can be aborted while in this state.  If
      not-null, then it should point to a function that performs a
      No-op or does any house-keeping that should be performed before
      the machine gets nuked.

      Should normally be a callback_fcn, as defined in libdfa.h */

  callback_fcn* abort_fcn;
} State; 


typedef struct transition {
  /** A unique (per machine type) identifier for this transition. */
  state_name     remote_state;
  /** The start state for this transition */
  state_name     pre_state;
  /** The stop state. */
  state_name     post_state;
  /** Executed when the machine traverses this arc.  Returns false if
      the arc should not be traversed.  For sane semantics, exactly
      one transition from a given start state should return true given
      the same network message. */
  callback_fcn * fcn_ptr;
  /** If true, this transitions causes a force to disk after fcn_ptr
      returns, but before the retry_fcn for the state is automatically
      executed the first time. */
  dfa_bool force;

} Transition;


#endif
