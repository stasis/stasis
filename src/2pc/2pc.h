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
#include <libdfa/libdfa.h>

#define MAX_SUBORDINATES 10

/** To use this library, you need to implement:

<ul>
<li> get_broadcast_group, which maps from message to broadcast group.
<li> prepare_or_veto_2pc, which, given a message, returns SUBORDINATE_VETO_2PC or SUBORDINATE_PREPARED_2PC, and calls abort, if appropriate
<li> abort, which handles the application specific actions for abort
<li> commit, which handles the application specific commit.
</ul>

A note on transaction ID's: clients specify transaction ID's by
addressing messages to the state machine with machine_id =
transaction_id.  

The coordinator sends an ack to the client once the transaction
commits or aborts, and keeps the machine around until the client ack's
the ack.  This is done in order to provide exactly once semantics.  

No locking, or ordering of requests is performed by the library.

The 64-bit transaction id's could, in principle, be reused.  

Distinct requests with identical transaction id's are serialized by
the current implementation, but no scheduling is done.  The first
request that retries after a transaction completes is the one that
gets to reuse the transaction id.


*/
/* 
   These will generally be defined by the user of the library. 

   (If you need more than one instance of 2pc per binary, memcpy the tranistions_2pc and states_2pc arrays...)

*/

/* STATES */
#define COORDINATOR_START_2PC      101
#define COORDINATOR_COMMITTING_2PC 102
#define COORDINATOR_ABORTING_2PC   103

#define SUBORDINATE_VETO_2PC       201
#define SUBORDINATE_PREPARED_2PC   202
#define SUBORDINATE_ACKING_2PC     203

#define AWAIT_ARRIVAL 211
#define AWAIT_COMMIT_POINT 212
#define AWAIT_RESULT 213

/** 
    The callbacks are called whenever the transition 'should' succeed.
    Other than tally_2pc, they are always called when a
    corresponding message comes in.  tally_2pc is only called after
    the last subordinate votes to prepare.

    All callbacks (other than veto_or_prepare) should return TRUE or
    FALSE, depending on whether or not the transition should
    succeed. veto_or_prepare returns SUBORDINATE_PREPARED_2PC,
    SUBORDINATE_VETO_2PC, or OVERRIDDEN_STATE.

    Under normal operations, they should never return OVERRIDDEN_STATE
    or FALSE, since that would violate the normal 2pc protocol.

*/
typedef struct {
  /** The global transaction ID.  Provided by caller of api's commit() function. */
  int xid;
  /*  char initiator[MAX_ADDRESS_LENGTH]; */

  /** Right now, get number of subordinates from network setup.  (Need
      to do something more fancy for recursive transactions.) 
  */
  char subordinate_votes[MAX_SUBORDINATES];

} TwoPCMachineState;



typedef struct {
  /** TRUE if this instance of the program is the coordinator. A
      single process could be both a subordinate and the coordinator,
      in priniciple.  In that case, is_coordinator should be set to
      true, (and the coordinator process's address sould be present in
      two broadcast groups.) */
  char is_coordinator;
  callback_fcn *init_xact_2pc;
  callback_fcn *veto_or_prepare_2pc;
  callback_fcn *abort_2pc;
  callback_fcn *commit_2pc;
  callback_fcn *tally_2pc;
  /**
    The get_broadcast_group function should return the number of the
    broadcast group that the message should be forwarded to.  (The
    coordinator is in broadcast group 0, so this function should
    return a number greater than zero.
  */
  short (*get_broadcast_group)(DfaSet *, Message * m);
  recordid app_state_record_id;
  void * app_state;
} TwoPCAppState;

/* #ifndef _TWO_PC */
extern const int transition_count_2pc;
extern const int client_transition_count_2pc;
extern const int state_count_2pc;
extern Transition client_transitions_2pc[];
extern Transition transitions_2pc[];
extern State states_2pc[];
/* #endif */
