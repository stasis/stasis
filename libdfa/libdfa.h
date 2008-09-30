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


#ifndef _LIBDFA_H
#define _LIBDFA_H
#include <libdfa/statemachine.h>
/*#include "monotree.h"*/
#include <libdfa/smash.h>
#include <libdfa/messages.h>
#include <libdfa/rw.h>

/** 
    Maximum number of concurrent state machines.
*/
#define DFA_MACHINE_COUNT 100

/** Defined in libdfa.c */
callback_fcn callback_false;
callback_fcn callback_true;

typedef struct dfaSet {
  /*  MonoTree monoTree; */
  smash_t * smash;
  NetworkSetup networkSetup;
  State * states;
  state_name state_count;
  Transition * transitions;
  state_name transition_count;
  /**
     The locking scheme for libdfa currently works as follows:

     There is a single, per process, read/write lock.  When a writer
     (either the main loop, or a worker thread that is deallocating
     its machine) holds the lock, then none of the worker threads may
     make progress.  When any reader holds the lock, all worker
     threads obtain an implicit write lock on their statemachine.

     One final note: Since a writer can change the in-memory location
     of state machines, the readers must re-initialize their state
     machine pointer each time they obtain a read lock.  This is one
     of the primary barriers to finer grained locking.  (See below for
     a straightforward, improved, locking scheme.)

     Performance issues:

     Finer grained locking may be necessary, as all of the
     user-defined callbacks are executed in the main loop while it
     holds the global write lock.  A better scheme might work as
     follows:

       Workers obtain a pointer to their state machine, and that
       pointer is immutable over the lifetime of the worker.

       The global write lock is only held when the main loop is
       allocating or deallocating machines; the global read lock is
       only used when the main loop needs to lookup a machine to
       service a request, or when creating new worker threads.

       Each machine has a pthreads_mutex associated with it, and
       worker threads obtain that mutex whenever they access the
       machine.  (Therefore user supplied callbacks only would block
       progress on the machine that they are running against.)

  */
  rwl * lock;
  void * app_setup;
} DfaSet;


/**
   Clears DfaSet, and establishes a new empty set of state machines in
   its place.  Also zeroes out the callback tables.  Does not
   initialize transient state such as network sockets.
*/
void dfa_initialize_new(DfaSet * dfaSet, unsigned short port, int count);
/**
*/
int dfa_start (DfaSet *dfaSet, 
	       Transition transitions[], int transition_count, 
	       State states[], state_name state_count);
/**

   Establishes all of the transient state of DfaSet, such as network
   connections and callback tables.  Should be called after dfa_initialize_new
   (or, upon recovery, without dfa_initialize_new)

   Returns -1 on error.
   
*/
int dfa_reinitialize (DfaSet *dfaSet, char * localhost,
	      Transition transitions[], int transition_count, 
	      State states[], state_name state_count);


/**
   Spawns a new thread to handle incoming requests.  (There should
   only be one such thread per dfaSet.)  

   @see main_loop, which does the same thing, but blocks indefinitely.
*/

pthread_t spawn_main_thread(DfaSet * dfaSet);
/**
   Use the current thread as the worker thread for state machine machine_id.  Returns when the machine is freed.

   Should be called after spawn_main_thread (Could be called after main_loop if the application manually manages threads.)

   @return TODO  (Not sure what this returns / should return.)
*/

void * request(DfaSet * dfaSet, state_name start_state, char * recipient_addr, state_machine_id recipient_machine_id, Message * message);

/**
   Runs an infinite loop to handle network requests.  
*/
void* main_loop(DfaSet *dfaSet);

DfaSet * dfa_malloc_old(int count, short port, 
		    char *** broadcast_lists, 
		    int broadcast_lists_count, 
		    int * broadcast_list_host_count);
DfaSet * dfa_malloc(int count, NetworkSetup * ns);
#endif
