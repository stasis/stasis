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
#define _GNU_SOURCE  // For asprintf
#include <libdfa/libdfa.h>
#include <unistd.h>
#include <sys/wait.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <sys/time.h>
#include <time.h>


void * run_request(DfaSet * dfaSet, state_machine_id machine_id);

typedef struct {
  DfaSet * dfaSet;
  const Transition * transitions;
  int transition_count;
  const State * states;
  state_name state_count;
  rwl* lock;
} main_wrap_args;

void dfa_initialize_new(DfaSet * dfaSet, unsigned short port, int count) {
  Tinit();
  dfaSet->smash = init_Smash(count);
  dfaSet->networkSetup.localport = port;
}

void recover(DfaSet * dfaSet);

int dfa_reinitialize(DfaSet *dfaSet, char * localhost,
	      Transition transitions[], int transition_count, 
	      State states[], state_name state_count) {

  dfaSet->lock = initlock();
  dfaSet->states = states;
  dfaSet->state_count = state_count;
  dfaSet->transitions = transitions;
  dfaSet->transition_count = transition_count;

  if(init_network_broadcast(&(dfaSet->networkSetup), dfaSet->networkSetup.localport, localhost,
			    dfaSet->networkSetup.broadcast_lists,
			    dfaSet->networkSetup.broadcast_lists_count,
			    dfaSet->networkSetup.broadcast_list_host_count) < 0) {
    return -1;
  }
  writelock(dfaSet->lock, 100);
  recover(dfaSet);
  writeunlock(dfaSet->lock);

  return 0;

}

/* Processes incoming events, and updates state machine states. */

typedef struct {
  DfaSet *dfaSet;
  state_machine_id machine_id;
} WorkerLoopArgs;

void * worker_loop(void * worker_loop_args);
pthread_t spawn_worker_thread(DfaSet * dfaSet, state_machine_id machine_id);
pthread_t spawn_main_thread(DfaSet * dfaSet);

void recover(DfaSet * dfaSet) {
  StateMachine * sm;
  StateMachine * this;
  byte * bsm; 
  byte * bsm_id; // Need these to make gcc -O2 happy (avoiding type-punning...)
  int keySize = sizeof(state_machine_id);
  state_machine_id * sm_id;
  int valueSize = sizeof(StateMachine);
  lladd_hash_iterator * it = ThashIterator(dfaSet->smash->xid, 
					   dfaSet->smash->hash, 
					   keySize, valueSize);

  while(ThashNext(dfaSet->smash->xid, it, &bsm_id, &keySize, &bsm, &valueSize)) {
    sm = (StateMachine*)bsm;
    sm_id = (state_machine_id*)bsm_id;
    assert(*sm_id == sm->machine_id);
    this = getSmash(dfaSet->smash, *sm_id);
    DEBUG("StateMachine %ld\n", sm->machine_id);
    this->worker_thread = spawn_worker_thread(dfaSet, sm->machine_id);
    free(sm_id);
    free(sm);
  }
}

void* main_loop(DfaSet *dfaSet) {

  Message * message = malloc(sizeof(Message));
  char * from = malloc(sizeof(char) * MAX_ADDRESS_LENGTH);
  NetworkSetup * networkSetup = malloc(sizeof(NetworkSetup));
  int recieved_message = 0;

  StateMachine * stateMachine;
  
  writelock(dfaSet->lock, 300);
  memcpy(networkSetup, &(dfaSet->networkSetup), sizeof(NetworkSetup));
  writeunlock(dfaSet->lock);

  /** @todo the locking scheme for this loop could be improved.  The loop assumes that 
	    pointers to existing state machines could be invalidated by unrelated threads, 
            and this forces it to hold a global write lock during processing. */
  
  while(1) {
    int i;
    state_name new_state, current_state;
    int new_machine = 0;

    /*Listen on socket... */
    if(recieved_message) {
      writeunlock(dfaSet->lock); 
      recieved_message = 0;
    }

    if(receive_message(networkSetup, message, from) <= 0) {
      continue;
    }

    recieved_message = 1;
    
    writelock(dfaSet->lock, 200);

    stateMachine = getSmash(dfaSet->smash, message->to_machine_id);
    DEBUG("Lookup %ld, ret = %d\n", message->to_machine_id, ret);

    /** @todo Check states to make sure they actually exist? */
    
    if(stateMachine == NULL) {
    
      DEBUG("Allocate machine %ld->", message->to_machine_id); fflush(NULL); 
      
      if(message->to_machine_id == NULL_MACHINE) {
	
	stateMachine = allocSmash(dfaSet->smash);
	
      } else {
	
	/* @todo: Check id. */	  
	stateMachine = insertSmash(dfaSet->smash, message->to_machine_id);
      }
      
      if(stateMachine == NULL) {
	fprintf(stderr, "Too many state machines.  Dropping request for new one.\n");
	continue;
      }
      
      new_machine = 1;

      stateMachine->worker_thread  = (pid_t)0;
      
      stateMachine->page      = NULL; 
      /* @todo libdfa doesn't use the 'conventional' null recordid, {0,0,-1} */
      stateMachine->page_id.page = 0; 
      stateMachine->page_id.slot = 0;
      stateMachine->page_id.size = 0;  
      
      current_state = NULL_STATE;
    } else {
      current_state = stateMachine->current_state;
    }
    
    new_state = current_state; 
    
    /* Find the appropriate transition */

    assert(message->to_machine_id == stateMachine->machine_id  || message->to_machine_id == NULL_MACHINE);
    
    for(i = 0; i< dfaSet->transition_count; i++) {
      if(dfaSet->transitions[i].remote_state == message->type && 
	 dfaSet->transitions[i].pre_state    == current_state) { 
	
	break;
      }
    }
    
    if(i == dfaSet->transition_count) {
      
      fprintf(stderr, "%ld received: %ld-%d:%d->? (bad message from %s)\n",  stateMachine->machine_id, message->from_machine_id, 
	     message->type, current_state, from);
      continue;
      
    } 
    
    
    if(dfaSet->transitions[i].fcn_ptr == NULL) {
      
      new_state = dfaSet->transitions[i].post_state;
      
    } else {
      
      /* Evaluate callback -- This produces a new state, and
	 overwrites m with a new message for the state machine. */

      int ret = (dfaSet->transitions[i].fcn_ptr)(dfaSet, stateMachine, message, from);
      
      if (dfaSet->transitions[i].post_state == OVERRIDDEN_STATE) {
	if( ret != OVERRIDDEN_STATE) {
	  
	  new_state = ret;
	
	} /* else leave new_state alone; the transition failed. */
      } else if (ret) {
	
 	new_state = dfaSet->transitions[i].post_state;
	
      }
      
    }
    
    
    /* Update machine state. */
    
    if(new_state == NULL_STATE) {
      
      /* Time to de-allocate machine */ 
      
      if(stateMachine->worker_thread == (pid_t)0) {
	
	/* No worker thread, so just deallocate, and move on */

	freeSmash(dfaSet->smash, stateMachine->machine_id);
	continue;

      } else {
	
	/* NULL_STATE_TOMBSTONE tells the worker thread that it's
	   time to shut down.  (NULL_STATE is reserved by monoTree
	   for machines that have already been deleted..) */
	
	new_state = NULL_STATE_TOMBSTONE;
      }
      assert(!new_machine);
    }

    if(new_state != current_state) {
      
      DEBUG("%ld transitioned on: %ld-%d:%d->%d from %s\n", stateMachine->machine_id, message->from_machine_id, 
	     dfaSet->transitions[i].remote_state, dfaSet->transitions[i].pre_state, dfaSet->transitions[i].post_state, from); 
      DEBUG(" -> %d %ld\n", new_state, message->from_machine_id);

      assert(new_state != NULL_STATE);
      stateMachine->current_state = new_state;	
      stateMachine->last_transition = time(NULL);
      
      /* @todo Is this general enough?  The transition function is
	 able to overwrite both variables, so it should be
	 good enough. */
      
      memcpy(&(stateMachine->message_recipient), from, MAX_ADDRESS_LENGTH);
      memcpy(&(stateMachine->message), message, sizeof(Message));
      
      /* We don't want to just swap the sender and recipient,
	 since we might have just allocated this machine.  If not,
	 then the original message's recipient should be the
	 same as stateMachine->machine_id anyway. 
	 
	 At any rate, the transition function should overwrite
	 message->from_machine_id to change the machine that the
	 state machine will deal with.
	 
      */
      
      stateMachine->message.from_machine_id = stateMachine->machine_id;
      stateMachine->message.to_machine_id   = message->from_machine_id;
      
      if(dfaSet->transitions[i].force) {
	setSmash(dfaSet->smash, stateMachine->machine_id);
	forceSmash(dfaSet->smash); 
      }

      /* Fork or signal the process if there already is one. */
      
      if(stateMachine->worker_thread == (pthread_t)NULL) {
 	assert ((stateMachine->current_state != NULL_STATE) && 
 		(stateMachine->current_state != NULL_STATE_TOMBSTONE));

	stateMachine->worker_thread = spawn_worker_thread(dfaSet, stateMachine->machine_id);

      } else {
	// This was a broadcast, but was recently changed to signal for 
	// performance reasons.
	pthread_cond_signal(stateMachine->sleepCond);
      }
    }
  }
}

state_name callback_false(void * dfaSet, StateMachine * stateMachine, 
		 Message * message, char * message_recipient) {
  return 0;
}
state_name callback_true(void * dfaSet, StateMachine * stateMachine, 
		 Message * message, char * message_recipient) {
  return 1;
}

void * inner_worker_loop(void * arg_void) {

  WorkerLoopArgs * arg = arg_void;
  DfaSet * dfaSet = arg->dfaSet;
  const state_machine_id machine_id = arg->machine_id;

  int timeout = 0; /* Run through the loop immediately the first time around. */
  int state = 0;
  int first = 1;
  StateMachine* stateMachine;

  
  free(arg_void);

  readlock(dfaSet->lock, machine_id); 



  stateMachine = getSmash(dfaSet->smash, machine_id);

  pthread_mutex_lock(stateMachine->mutex);
  
  while(1) {
    int rc = 0;

    state_name i, state_idx = NULL_STATE; 
    
    /** @todo inner worker loop doesn't seem to 'do the right thing' with respect to timing */
    if(1|| !stateMachine->pending) {  /* If no pending messages, go to sleep */
      struct timeval now;
      struct timespec timeout_spec;

      pthread_cond_t * cond;
      pthread_mutex_t * mutex;
      
      long usec;

      cond = stateMachine->sleepCond;
      mutex = stateMachine->mutex;
      
      readunlock(dfaSet->lock);

      /* A note on locking: This loop maintains a read lock everywhere
	 except for this call to sleep, and upon termination when it
	 requires a write lock. */

      gettimeofday(&now, NULL);

      usec = now.tv_usec + timeout;

      if(usec > 1000000) {
	now.tv_sec++;
	usec-=1000000;
      }
      
      timeout_spec.tv_sec = now.tv_sec;
      timeout_spec.tv_nsec = 1000 * usec;
      

      rc =  pthread_cond_timedwait (cond, mutex, &timeout_spec );

      if(rc == EINVAL) {
	perror("pthread");
      } 
      
      readlock(dfaSet->lock, machine_id);     
      
      /* Some other thread may have invalidated our pointer while we
	 were sleeping witout a lock... no longer true, *but* since
	 our pointer is local to this thread, we still need to re-read
	 from the store.*/

      assert(stateMachine == getSmash(dfaSet->smash, machine_id));
    }

    DEBUG("Current State: %d, %d\n", stateMachine->current_state, NULL_STATE_TOMBSTONE);

    if(stateMachine->current_state == NULL_STATE_TOMBSTONE) {
      DEBUG("Freeing statemachine\n");
      break;
    }
    if(state != stateMachine->current_state) { first = 1; }
    state = stateMachine->current_state;
    stateMachine->message.type = stateMachine->current_state;
    timeout = 690000 +(int) (300000.0*rand()/(RAND_MAX+1.0)); 
    for(i = 0; i < dfaSet->state_count; i++) {
      if(dfaSet->states[i].name == stateMachine->current_state) {
	state_idx = i;
      }
    } 

    assert(state_idx != NULL_STATE);
    DEBUG("Worker loop for state machine: %ld still active\n", machine_id);

    int send = 1;
    if(dfaSet->states[state_idx].retry_fcn != NULL) {
      send = dfaSet->states[state_idx].retry_fcn(dfaSet, stateMachine, &(stateMachine->message), stateMachine->message_recipient);
    }
    if(send) {
      if(first) {
	first = 0;
      } else {
	printf("Resending message. Machine # %ld State # %d\n", stateMachine->machine_id, stateMachine->current_state);
      } 
      send_message(&(dfaSet->networkSetup), &(stateMachine->message), stateMachine->message_recipient);
    }

  }

  setSmash(dfaSet->smash, stateMachine->machine_id);
  pthread_mutex_unlock(stateMachine->mutex);
  readunlock(dfaSet->lock);
  return 0;

}

void * worker_loop(void * arg_void) {

  WorkerLoopArgs * arg = arg_void;
  
  StateMachine * stateMachine;
  DfaSet * dfaSet = arg->dfaSet;
  state_machine_id machine_id = arg->machine_id;
  readlock(dfaSet->lock, machine_id);

  DEBUG("Worker loop: %ld\n", machine_id);
  
  stateMachine = getSmash(dfaSet->smash, machine_id);

  assert(stateMachine->machine_id == machine_id);

  if(pthread_detach(stateMachine->worker_thread) != 0) {
    perror("pthread_detach");
  }

  readunlock(dfaSet->lock);
  inner_worker_loop(arg_void);

  writelock(dfaSet->lock, machine_id); 
  DEBUG("Freeing machine %ld\n", machine_id);

  freeSmash(dfaSet->smash, machine_id);
  writeunlock(dfaSet->lock);

  return 0;
}

pthread_t spawn_main_thread(DfaSet * dfaSet) {
  pthread_t worker_thread;
  int ret;

  ret = pthread_create(&worker_thread, NULL, (void*(*)(void*))&main_loop, dfaSet);
  
  if(ret != 0) {
    perror("libdfa: pthread_create:");
    fflush(NULL); 
  }


  return worker_thread;
}

pthread_t spawn_worker_thread(DfaSet * dfaSet, state_machine_id machine_id) {

  pthread_t worker_thread;
  int ret;

  WorkerLoopArgs * worker_loop_args;

  // Freed in inner_worker_loop.
  worker_loop_args = malloc(sizeof(WorkerLoopArgs));

  DEBUG("spawn_worker_thread(state_machine_id=%ld)\n", machine_id);

  worker_loop_args->dfaSet = dfaSet;
  worker_loop_args->machine_id = machine_id; 

  ret = pthread_create(&worker_thread, NULL, &worker_loop, worker_loop_args);
  
  if(ret != 0) {
    perror("libdfa: pthread_create:");
    worker_thread = (pthread_t)NULL;
    fflush(NULL);
  }

  return worker_thread;
}

void * request(DfaSet * dfaSet, state_name start_state, char * recipient_addr, state_machine_id recipient_machine_id, Message * message) {
  StateMachine * initial_sm;
  state_machine_id machine_id;
  void * ret;
  writelock(dfaSet->lock, 600);

  initial_sm = allocSmash(dfaSet->smash);
  if(initial_sm == NULL) {
    return NULL;
  }

  assert(start_state != NULL_STATE);
  
  if(message != NULL) {
    
    memcpy(&(initial_sm->message), message, sizeof(Message));
  }
  
  initial_sm->current_state = start_state;
  initial_sm->message.from_machine_id = initial_sm->machine_id;
  initial_sm->message.to_machine_id = recipient_machine_id;
  initial_sm->message.type = start_state;

  char * initiator;
  {
    int err = asprintf(&initiator, "%s:%d", dfaSet->networkSetup.localhost, dfaSet->networkSetup.localport);
    assert(err != -1);
  }
  strcpy(initial_sm->message.initiator, initiator);
  free(initiator);
  //  printf("Set message initiator to %s\n", initial_sm->message.initiator); fflush(stdout);
  initial_sm->message.initiator_machine_id = initial_sm->machine_id;

  strcpy(initial_sm->message_recipient, recipient_addr);
  machine_id = initial_sm->machine_id;
  writeunlock(dfaSet->lock);

  ret = run_request(dfaSet, machine_id);

  writelock(dfaSet->lock, machine_id); 
  assert(initial_sm == getSmash(dfaSet->smash, machine_id));
  if(message != NULL) {

    memcpy(message, &(initial_sm->message), sizeof(Message));
  }

  freeSmash(dfaSet->smash, initial_sm->machine_id);
  writeunlock(dfaSet->lock);

  return ret;
}

void * run_request(DfaSet * dfaSet, state_machine_id machine_id) {
  void * ret;
  WorkerLoopArgs * worker_loop_args = malloc(sizeof(WorkerLoopArgs));
  StateMachine * machine;
  
  readlock(dfaSet->lock, 600);
  
  machine =  getSmash(dfaSet->smash, machine_id);
 
  worker_loop_args->dfaSet = dfaSet;
  worker_loop_args->machine_id = machine_id;
  
  machine->worker_thread = pthread_self();
  readunlock(dfaSet->lock);

  ret = inner_worker_loop(worker_loop_args);

  return (void*)ret;

}
DfaSet * dfa_malloc_old(int count, short port, 
		    char *** broadcast_lists, 
		    int broadcast_lists_count, 
		    int * broadcast_list_host_count) {
  DfaSet * dfaSet = stasis_calloc(1, DfaSet);
  dfa_initialize_new(dfaSet, port, count);
  
  dfaSet->networkSetup.broadcast_lists = broadcast_lists;
  dfaSet->networkSetup.broadcast_lists_count = broadcast_lists_count;
  dfaSet->networkSetup.broadcast_list_host_count = broadcast_list_host_count;

  return dfaSet;
}

DfaSet * dfa_malloc(int count, NetworkSetup * ns) {
  DfaSet * dfaSet = stasis_calloc(1, DfaSet);
  dfa_initialize_new(dfaSet, ns->localport, count);
  
  memcpy(&dfaSet->networkSetup, ns, sizeof(NetworkSetup));
  return dfaSet;
}
