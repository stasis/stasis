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
#include <libdfa/smash.h>
#include <stdlib.h>
#include <assert.h>

smash_t *  init_Smash(int size) {
  int xid = Tbegin();
  recordid store = Talloc(xid, sizeof(smash_t));
  smash_t * ret = stasis_calloc(1, smash_t);
  
  ret->size = size;
  ret->contents = 0;
  ret->next_sm_id = 0;
  ret->store = store;
//  ret->hash = jbHtCreate(xid, 3499);
  ret->hash = ThashCreate(xid, sizeof(state_machine_id), sizeof(StateMachine));//lHtCreate(xid, 7);
  ret->xid = xid;
  ret->lock = malloc(sizeof(pthread_mutex_t));
  ret->memHash = pblHtCreate();

  pthread_mutex_init(ret->lock, NULL);

  Tset(xid, ret->store, ret);

  

  return ret;
}


void * _getSmash (smash_t * smash, state_machine_id id) {
  /*extern void * pblHtLookup  ( pblHashTable_t * h, void * key, size_t keylen );*/
  return pblHtLookup ( smash->memHash, &(id), sizeof(state_machine_id));
  /*  return (-1 != jbHtLookup(smash->xid, smash->hash, &(id), sizeof(state_machine_id), machine)); */
}

StateMachine *  _insertSmash(smash_t * smash, state_machine_id id) {
  StateMachine * new_sm;

  if(smash->contents+1 == smash->size) {
    return NULL;
  }

  smash->contents++;
  new_sm = malloc(sizeof (StateMachine));
  new_sm->machine_id = id;
  new_sm->mutex = malloc(sizeof(pthread_mutex_t));
  new_sm->sleepCond = malloc(sizeof(pthread_cond_t));
  new_sm->pending = 0;
  pthread_mutex_init(new_sm->mutex, NULL);
  pthread_cond_init(new_sm->sleepCond, NULL);
  
  new_sm->current_state = START_STATE;
  /*  printf("Insert %ld\n", id);  */
  ThashInsert(smash->xid, smash->hash, (byte*)&id, sizeof(state_machine_id), (byte*)new_sm, sizeof(StateMachine));
  pblHtInsert(smash->memHash, &id, sizeof(state_machine_id), new_sm);
  /*  Tcommit(smash->xid);
      smash->xid = Tbegin(); */
  
  return new_sm;
}



/** @return -1 on error, 0 if there isn't any more room, and 1 on success. */
StateMachine *  allocSmash (smash_t * smash) {
  void * ret;

  pthread_mutex_lock(smash->lock);

  /* Make sure we don't clobber an existing state machine... */
  /*  while(jbHtLookup(smash->xid, smash->hash, &(smash->next_sm_id), sizeof(state_machine_id), &junk) != -1) {
    smash->next_sm_id++;
    }*/
  
  while(_getSmash(smash, smash->next_sm_id) != NULL) {
    smash->next_sm_id++;
  }

  /*  printf("Alloc %ld\n", smash->next_sm_id);  */
  
  ret = _insertSmash(smash, smash->next_sm_id);
  smash->next_sm_id++;
  pthread_mutex_unlock(smash->lock);

  return ret;
}


/** @return -1 on error, 0 if there isn't any more room, and 1 on success. */
StateMachine *  insertSmash(smash_t * smash, state_machine_id id) {
  void * ret;
  byte * junk; // will point to StateMachine...

  pthread_mutex_lock(smash->lock);

  if(ThashLookup(smash->xid, smash->hash, (byte*)&(smash->next_sm_id), sizeof(state_machine_id), &junk) != -1) {

    free(junk);
    pthread_mutex_unlock(smash->lock);
    return NULL;
  }

  ret= _insertSmash(smash, id);
  pthread_mutex_unlock(smash->lock);
  return ret;
}

/** @return -1 on error, 0 if there isn't any more room, and 1 on success. */
int  freeSmash  (smash_t * smash, state_machine_id id) {
  StateMachine * old = getSmash(smash, id);
  int ret;
  pthread_mutex_lock(smash->lock);
  
  if(old == NULL) {
    /* Bogus state machine id?? */

    assert(0);
    
  }
  

  smash->contents--;
  pthread_mutex_destroy(old->mutex);
  pthread_cond_destroy(old->sleepCond);
  free(old->mutex);
  free(old->sleepCond); 
  
  pblHtRemove(smash->memHash, &(id), sizeof(state_machine_id));
  //ret = TlogicalHashDelete(smash->xid, smash->hash, (byte*)&(id), sizeof(state_machine_id), NULL, sizeof(state_machine_id)) != -1;
  ret = ThashRemove(smash->xid, smash->hash, (byte*)&(id), sizeof(state_machine_id));
  free(old);
  
  /*  Tcommit(smash->xid);
      smash->xid = Tbegin();*/
  pthread_mutex_unlock(smash->lock);
  return ret;
    
}


void *  getSmash   (smash_t * smash, state_machine_id id) {
  void * ret;
  /*  printf("Get smash: %ld\n", id); */
  
  pthread_mutex_lock(smash->lock);
  ret = _getSmash(smash, id);
  pthread_mutex_unlock(smash->lock);
  
  return ret;
}

void _setSmash(smash_t * smash, state_machine_id id) {
  
  StateMachine * machine;
  machine = _getSmash(smash, id);
  //TlogicalHashInsert(smash->xid, smash->hash, (byte*)&id, sizeof(state_machine_id),(byte*) machine, sizeof(StateMachine));
  ThashInsert(smash->xid, smash->hash, 
  	      (byte*)&id, sizeof(state_machine_id),
              (byte*) machine, sizeof(StateMachine));
}


void setSmash (smash_t * smash, state_machine_id id) {
 // int ret;
  /*  printf("Set smash: %ld\n", machine->machine_id); */
  pthread_mutex_lock(smash->lock);

  //ret = 
  _setSmash(smash, id);

  pthread_mutex_unlock(smash->lock); 

 // return ret;
}

int forceSmash (smash_t * smash) {
  int ret;

  pthread_mutex_lock(smash->lock);

  Tcommit(smash->xid);
  ret = (-1 != (smash->xid = Tbegin()));
  
  pthread_mutex_unlock(smash->lock);

  return ret;

}
