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
#include <unistd.h>
#include <sys/wait.h>
#include <stdio.h>
#include <malloc.h>
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
  /*init_MonoTree(&(dfaSet->monoTree), count);*/
  Tinit();
  dfaSet->smash = init_Smash(count);
  dfaSet->networkSetup.localport = port;
}

void recover(DfaSet * dfaSet);

/*void nothing(int signo) { 
  /
     This space left intentionally blank. 
  /
 }*/

int dfa_reinitialize(DfaSet *dfaSet, char * localhost,
	      Transition transitions[], int transition_count, 
	      State states[], state_name state_count) {

/*  struct sigaction        actions;
    int rc;*/


  dfaSet->lock = initlock();
  dfaSet->states = states;
  dfaSet->state_count = state_count;
  dfaSet->transitions = transitions;
  dfaSet->transition_count = transition_count;


/*  memset(&actions, 0, sizeof(actions));
  sigemptyset(&actions.sa_mask);
  actions.sa_flags = 0;
  actions.sa_handler = nothing;

  rc = sigaction(SIGALRM,&actions,NULL);
  if(rc < 0) {
    perror("sigaction");
    return -1;
  }
*/

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
  /* int machine_count;
    StateMachine* machines = enumerateMachines(&(dfaSet->monoTree), &machine_count); 
  int i;
  for(i = 0; i < machine_count; i++) {
    machines[i].worker_thread = spawn_worker_thread(dfaSet, machines[i].machine_id);
    } */
  //StateMachine sm_stack; 
  StateMachine * sm;//= &sm_stack;
  StateMachine * this;
  // Need to write iterator...
//  int ret = (jbHtFirst(dfaSet->smash->xid, dfaSet->smash->hash, (byte*)sm) != -1);
  int keySize = sizeof(state_machine_id);
  state_machine_id * sm_id;
  int valueSize = sizeof(StateMachine);
  lladd_hash_iterator * it = ThashIterator(dfaSet->smash->xid, dfaSet->smash->hash, keySize, valueSize);

  //assert(0);  // need to call linear hash iterator here. 
  while(ThashNext(dfaSet->smash->xid, it, (byte**)&sm_id, &keySize, (byte**)&sm, &valueSize)) {
//  while(ret) {
    assert(*sm_id == sm->machine_id);
    this = getSmash(dfaSet->smash, *sm_id);
    DEBUG("StateMachine %ld\n", sm->machine_id);
    this->worker_thread = spawn_worker_thread(dfaSet, sm->machine_id);
    free(sm_id);
    free(sm);
 //   ret = (jbHtNext(dfaSet->smash->xid, dfaSet->smash->hash, (byte*)sm) != -1);
  }

}

void* main_loop(DfaSet *dfaSet) {

  Message * message = malloc(sizeof(Message));
  char * from = malloc(sizeof(char) * MAX_ADDRESS_LENGTH);
  NetworkSetup * networkSetup = malloc(sizeof(NetworkSetup));
  int recieved_message = 0;

  /* StateMachine stateMachine_stack; */
  StateMachine * stateMachine; /* = &stateMachine_stack; */
  
  
  writelock(dfaSet->lock, 300);
  memcpy(networkSetup, &(dfaSet->networkSetup), sizeof(NetworkSetup));
  writeunlock(dfaSet->lock);

  while(1) {
    int i;
    state_name new_state, current_state;
    /*    int ret;*/
    int new_machine = 0;

    /*Listen on socket... */
    if(recieved_message) {
      /*      if(stateMachine != NULL) {
	setSmash(dfaSet->smash, stateMachine->machine_id);
	} */
      writeunlock(dfaSet->lock); 
      recieved_message = 0;
    }

    if(receive_message(networkSetup, message, from) <= 0) {
      continue;
    }

    recieved_message = 1;
    
    /* The commented out, more complex locking scheme does not work,
       because freeMachine (called by worker threads) invalidates
       stateMachine pointers, so this loop really needs a global
       write lock. */
    /*      readlock(dfaSet->lock, 200); */

    writelock(dfaSet->lock, 200);

    stateMachine = getSmash(dfaSet->smash, message->to_machine_id);
    /*    printf("Lookup %ld, ret = %d\n", message->to_machine_id, ret); */
    /*    stateMachine = getMachine(&(dfaSet->monoTree), message->to_machine_id); */
    /*      if(stateMachine != NULL) {
	    / * Grab the lock now. * /
	    pthread_mutex_lock(&(stateMachine->mutex));
	    }  */
    
    /* This is the only thread that can write to the monoTree, so we
       don't need to worry that someone else will create the
       stateMachine, so we can safely release all locks if the
       stateMachine was null, and not worry about race conditions.  */

    /*     readunlock(dfaSet->lock); */
    
    /* TODO:  Check states to make sure they actually exist? */
    
    if(stateMachine == NULL) {
    
      /*writelock(dfaSet->lock, 600);*/
      
      DEBUG("Allocate machine %ld->", message->to_machine_id); fflush(NULL); 
      
      if(message->to_machine_id == NULL_MACHINE) {
	
	stateMachine = allocSmash(dfaSet->smash);

	/*stateMachine = allocMachine(&(dfaSet->monoTree));*/
	
      } else {
	
	/* TODO: Check id. */	  
	/*stateMachine = insertMachine(&(dfaSet->monoTree), message->to_machine_id);	   */
	
	stateMachine = insertSmash(dfaSet->smash, message->to_machine_id);
      }
      
      if(stateMachine == NULL) {

	
	/*writeunlock(dfaSet->lock);*/
	
	fprintf(stderr, "Too many state machines.  Dropping request for new one.\n");
	continue;
	
      } else {
	/*	printf("machine->id:%ld\n", stateMachine->machine_id); */
      }
      
      new_machine = 1;


      /* Done with monotree for now. */
      
      /* Grab a write lock on our machine before releasing the global
	 write lock, so that its worker thread doesn't get a hold of
	 it. */
      
      /*pthread_mutex_lock(&(stateMachine->mutex));*/
      
      /*writeunlock(dfaSet->lock);*/
      
      stateMachine->worker_thread  = (pid_t)NULL;
      
      stateMachine->page      = NULL; 
      stateMachine->page_id.page = 0; /* TODO: Is there such a thing as a null page?? */
      stateMachine->page_id.slot = 0;
      stateMachine->page_id.size = 0;  
      
      current_state = NULL_STATE;
    } else {
      current_state = stateMachine->current_state;
    }

    /* At this point, we hold stateMachine->mutex and no other locks. */
    
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
      
      fprintf(stderr, "%ld received: %ld-%d:%d->? (bad message)\n",  stateMachine->machine_id, message->from_machine_id, 
	     message->type, current_state);
      /*pthread_mutex_unlock(&(stateMachine->mutex));*/
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
      
      if(stateMachine->worker_thread == (pid_t)NULL) {
	
	/* No worker thread, so just deallocate, and move on */
	/* Note, that at this point, we hold the mutex on the state machine, and the global write lock. */
	/*writelock(dfaSet->lock,500);
	  pthread_mutex_unlock(&(stateMachine->mutex));*/
	
	/*freeMachine(&(dfaSet->monoTree),
	  stateMachine->machine_id); */
	freeSmash(dfaSet->smash, stateMachine->machine_id);
	
	/*writeunlock(dfaSet->lock);*/
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
      
      /* TODO: Is this general enough?  The transition function is
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
      
      /* TODO: Force if necessary.*/ 

      if(dfaSet->transitions[i].force) {
	setSmash(dfaSet->smash, stateMachine->machine_id);
	forceSmash(dfaSet->smash); 
      }

      /*stateMachine->pending++;	       */

      /*      setSmash(dfaSet->smash, stateMachine->machine_id); */


      /* Fork or signal the process if there already is one. */
      
      if(stateMachine->worker_thread == (pthread_t)NULL) {
	/*	assert (getMachine(&(dfaSet->monoTree), stateMachine->machine_id) == stateMachine); */
 	assert ((stateMachine->current_state != NULL_STATE) && 
 		(stateMachine->current_state != NULL_STATE_TOMBSTONE));

	stateMachine->worker_thread = spawn_worker_thread(dfaSet, stateMachine->machine_id);

      } else {
	/*	pthread_cond_t * cond;
		pthread_cond_t cond_copy; 
	
		pthread_kill (stateMachine->worker_thread, SIGALRM);
		pthread_cond_signal (&(stateMachine->sleepCond)); */
	/*	printf("Waking worker...\n"); */

	/* memcpy(&cond_copy, &(stateMachine->sleepCond), sizeof(pthread_cond_t)); */

	pthread_cond_broadcast (stateMachine->sleepCond);  /* TODO: Signal should be adequate.. */
      }
    }
    /*    setSmash(dfaSet->smash, stateMachine->machine_id); */
	
  }
}

void * inner_worker_loop(void * arg_void) {

  WorkerLoopArgs * arg = arg_void;
  /*   int ret; */
  DfaSet * dfaSet = arg->dfaSet;
  const state_machine_id machine_id = arg->machine_id;
  /*  pthread_cond_t cond_copy;
      pthread_mutex_t mutex_copy; */

  int timeout = 0; /* Run through the loop immediately the first time around. */
  int state = 0;
  /*  int tries = 0; */
  /*  StateMachine stateMachine_stack; */
  StateMachine* stateMachine; /* = &stateMachine_stack; */

  
  free(arg_void);

  readlock(dfaSet->lock, machine_id); 



  stateMachine = getSmash(dfaSet->smash, machine_id);


  /*  stateMachine = getMachine(&(dfaSet->monoTree), machine_id); */
  /*  assert(stateMachine != NULL); */
  /*  assert(ret); */

  /*  memcpy(&mutex_copy, &(stateMachine->mutex), sizeof(pthread_mutex_t));
      memcpy(&cond_copy, &(stateMachine->sleepCond), sizeof(pthread_cond_t)); */

  pthread_mutex_lock(stateMachine->mutex);
  
  while(1) {
    int rc = 0;

    state_name i, state_idx; 
    
    /** SIGALRM will make sleep return immediately (I hope!)*/

    /*    printf("pending: %ld, %d\n", stateMachine->machine_id, stateMachine->pending); */
    /** @todo inner worker loop doesn't seem to 'do the right thing' with respect to timing */
    if(1|| !stateMachine->pending) {  /* If no pending messages, go to sleep */
      struct timeval now;
      struct timespec timeout_spec;

      pthread_cond_t * cond;
      pthread_mutex_t * mutex;
      
      long usec;

      cond = stateMachine->sleepCond;
      mutex = stateMachine->mutex;
      
      /*      setSmash(dfaSet->smash, stateMachine); */  /* No longer write to stateMacine in this loop..Well
							    , we do, but we can safely lose that change. */
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
      /** @todo ridiculously long timeout in libdfa.c */
      
      timeout_spec.tv_sec = now.tv_sec;/* + timeout; */
      timeout_spec.tv_nsec = 1000 * usec; /*now.tv_usec * 1000; */
      

      rc =  pthread_cond_timedwait (cond, mutex, &timeout_spec );

      /*      rc = sleep(timeout); */

      
      if(rc == EINVAL) {
	perror("pthread");
      } /*else if (rc != ETIMEDOUT) {
	printf("Worker signaled.\n");      
      } else {
	printf("Timed out.\n");
	}*/
      readlock(dfaSet->lock, machine_id);     
      
      /* Some other thread may have invalidated our pointer while we
	 were sleeping witout a lock... no longer true, *but* since
	 our pointer is local to this thread, we still need to re-read
	 from the store.*/

      /*stateMachine = getMachine(&(dfaSet->monoTree), machine_id);*/
      assert(stateMachine == getSmash(dfaSet->smash, machine_id));
    }

    /*    stateMachine = getMachine(&(dfaSet->monoTree), machine_id); */
    /* This can't happen! */
    /*    assert(ret); */
    /*    assert(stateMachine != NULL); */

    /*    pthread_mutex_lock(&(stateMachine->mutex));   */

    /*    if(stateMachine->pending > 0) {

      if(stateMachine->pending > 0) {
	stateMachine->pending--;
      }
      assert (stateMachine->pending >= 0);

      }*/
    /* pthread_mutex_unlock(&(stateMachine->mutex)); */

    /*    printf("Current State: %d, %d\n", stateMachine->current_state, NULL_STATE_TOMBSTONE); */
    if(stateMachine->current_state == NULL_STATE_TOMBSTONE) {
      /*      printf("Breaking\n"); */
      break;
    }
    state = stateMachine->current_state;
    stateMachine->message.type = stateMachine->current_state;
    timeout = 690000 +(int) (300000.0*rand()/(RAND_MAX+1.0)); 
    /*    timeout = 1; */
    for(i = 0; i < dfaSet->state_count; i++) {
      if(dfaSet->states[i].name == stateMachine->current_state) {
	state_idx = i;
      }
    }

    /*    if(dfaSet->states[stateMachine->current_state].abort_fcn && (time(NULL) - stateMachine->last_transition > 100)) { 
      if(dfaSet->states[state_idx].abort_fcn && (time(NULL) - stateMachine->last_transition > 100)) {
	((callback_fcn*)dfaSet->states[state_idx].abort_fcn)(dfaSet, stateMachine, &(stateMachine->message), NULL); 
	freeMachine(&(dfaSet->monoTree), machine_id); 
	break;
      } 
      / *      if((time(NULL) - stateMachine->last_transition > abort_timeout) && abort_fcn!=NULL) {
	      abort_fcn(dfaSet, stateMachine, &(stateMachine->message), NULL); * /
    }*/
      
    /* TODO:  Copy this stuff into buffers w/ memcopy, unlock, then call send_message. */
    
    /*    printf("Worker loop for state machine: %ld still active\n", machine_id); */

    send_message(&(dfaSet->networkSetup), &(stateMachine->message), stateMachine->message_recipient);

  }

  setSmash(dfaSet->smash, stateMachine->machine_id);

  pthread_mutex_unlock(stateMachine->mutex);

  readunlock(dfaSet->lock);
  

  return 0;
}

void * worker_loop(void * arg_void) {

  WorkerLoopArgs * arg = arg_void;
  /*  StateMachine stateMachine_stack; */
  StateMachine * stateMachine; /* = &stateMachine_stack; */
  DfaSet * dfaSet = arg->dfaSet;
  state_machine_id machine_id = arg->machine_id;
  readlock(dfaSet->lock, machine_id);

  /*  printf("Worker loop: %ld\n", machine_id); */
  
  stateMachine = getSmash(dfaSet->smash, machine_id);

  /*   stateMachine = getMachine(&(dfaSet->monoTree), machine_id); */
  /*  assert(stateMachine != NULL); */
  assert(stateMachine->machine_id == machine_id);

  /* pthread_detach gets angry if the current process recieves a
     signal.  How to handle this properly?  Can the main thread
     wait until this thread detaches successfully? (Do we need a lock somewhere?) */
  if(pthread_detach(stateMachine->worker_thread) != 0) {
    perror("pthread_detach");
  }

  readunlock(dfaSet->lock);
  inner_worker_loop(arg_void);

  /*  fflush(NULL); */

  writelock(dfaSet->lock, machine_id); 
  DEBUG("Freeing machine %ld\n", machine_id);

  /*  pthread_mutex_lock(&(stateMachine->mutex));   */
  /*freeMachine(&(dfaSet->monoTree), machine_id); */
  freeSmash(dfaSet->smash, machine_id);
  /* pthread_mutex_unlock(&(stateMachine->mutex));   */
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

  /* Do we need to malloc a new worker_loop_args for each thread? 
    TODO:  Should this be freed? Is it already?
  */
  pthread_t worker_thread;
  int ret;

  WorkerLoopArgs ** worker_loop_args = malloc(sizeof(WorkerLoopArgs*));


  *worker_loop_args = malloc(sizeof(WorkerLoopArgs));


  DEBUG("spawn_worker_thread(state_machine_id=%ld)\n", machine_id);

  (*worker_loop_args)->dfaSet = dfaSet;
  (*worker_loop_args)->machine_id = machine_id; 

  ret = pthread_create(&worker_thread, NULL, &worker_loop, *worker_loop_args);
  
  if(ret != 0) {
    perror("libdfa: pthread_create:");
    worker_thread = (pthread_t)NULL;
    fflush(NULL);
  }

  return worker_thread;
}

void * request(DfaSet * dfaSet, state_name start_state, char * recipient_addr, state_machine_id recipient_machine_id, Message * message) {
  /*  StateMachine initial_sm_stack; */
  StateMachine * initial_sm; /* = &initial_sm_stack; */
  state_machine_id machine_id;
  int ret;
  writelock(dfaSet->lock, 600);

  
  /*  initial_sm = allocMachine(&(dfaSet->monoTree)); */
  initial_sm = allocSmash(dfaSet->smash);
  if(initial_sm == NULL) {
    return NULL;
  }
  /*  if(!ret) {
    return NULL;
    } */
  assert(start_state != NULL_STATE);
  
  if(message != NULL) {
    
    memcpy(&(initial_sm->message), message, sizeof(Message));
  }
  
  initial_sm->current_state = start_state;
  initial_sm->message.from_machine_id = initial_sm->machine_id;
  initial_sm->message.to_machine_id = recipient_machine_id;
  initial_sm->message.type = start_state;

  //strcpy(initial_sm->message.initiator, dfaSet->networkSetup.localhost);
  char * initiator;
  asprintf(&initiator, "%s:%d", dfaSet->networkSetup.localhost, dfaSet->networkSetup.localport);
  strcpy(initial_sm->message.initiator, initiator);
  free(initiator);
  printf("Set message initiator to %s", initial_sm->message.initiator);
  initial_sm->message.initiator_machine_id = initial_sm->machine_id;

  strcpy(initial_sm->message_recipient, recipient_addr);
  machine_id = initial_sm->machine_id;
  /*  setSmash(dfaSet->smash, initial_sm->machine_id); */
  writeunlock(dfaSet->lock);

  ret = (int)run_request(dfaSet, machine_id);


  writelock(dfaSet->lock, machine_id); 
  assert(initial_sm == getSmash(dfaSet->smash, machine_id));
  if(message != NULL) {

    memcpy(message, &(initial_sm->message), sizeof(Message));
  }
  
  /*  freeMachine(&(dfaSet->monoTree), initial_sm->machine_id); */
  freeSmash(dfaSet->smash, initial_sm->machine_id);
  writeunlock(dfaSet->lock);

  return (void*)ret;
}

void * run_request(DfaSet * dfaSet, state_machine_id machine_id) {
  void * ret;
  WorkerLoopArgs * worker_loop_args = malloc(sizeof(WorkerLoopArgs));
  /*  StateMachine machine_stack; */
  StateMachine * machine; /* = &machine_stack; */
  
  readlock(dfaSet->lock, 600);
  
  machine =  getSmash(dfaSet->smash, machine_id);
  /*   machine= getMachine(&(dfaSet->monoTree), machine_id); */

  worker_loop_args->dfaSet = dfaSet;
  worker_loop_args->machine_id = machine_id;
  
  machine->worker_thread = pthread_self();
  /*  setSmash(dfaSet->smash, machine->machine_id); */
  readunlock(dfaSet->lock);


  ret = inner_worker_loop(worker_loop_args);

  return (void*)ret;

}
DfaSet * dfa_malloc_old(int count, short port, 
		    char *** broadcast_lists, 
		    int broadcast_lists_count, 
		    int * broadcast_list_host_count) {
  DfaSet * dfaSet = calloc(1, sizeof(DfaSet));
  /*  dfaSet->monoTree.buffer = calloc(count, sizeof(StateMachine)); */
  dfa_initialize_new(dfaSet, port, count);
  
  dfaSet->networkSetup.broadcast_lists = broadcast_lists;
  dfaSet->networkSetup.broadcast_lists_count = broadcast_lists_count;
  dfaSet->networkSetup.broadcast_list_host_count = broadcast_list_host_count;

  return dfaSet;
}

DfaSet * dfa_malloc(int count, NetworkSetup * ns) {
  DfaSet * dfaSet = calloc(1, sizeof(DfaSet));
  dfa_initialize_new(dfaSet, ns->localport, count);
  
  memcpy(&dfaSet->networkSetup, ns, sizeof(NetworkSetup));
  return dfaSet;
}
