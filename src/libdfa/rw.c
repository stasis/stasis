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
#include <libdfa/rw.h>

rwl *initlock (void)
{
	rwl *lock;

	lock = (rwl *)malloc (sizeof (rwl));
	if (lock == NULL) return (NULL);
	lock->mut = (pthread_mutex_t *) malloc (sizeof (pthread_mutex_t));
	if (lock->mut == NULL) { free (lock); return (NULL); }
	lock->writeOK = 
		(pthread_cond_t *) malloc (sizeof (pthread_cond_t));
	if (lock->writeOK == NULL) { free (lock->mut); free (lock); 
		return (NULL); }
	lock->readOK = 
		(pthread_cond_t *) malloc (sizeof (pthread_cond_t));
	if (lock->writeOK == NULL) { free (lock->mut); free (lock->writeOK); 
		free (lock); return (NULL); }
	
	pthread_mutex_init (lock->mut, NULL);
	pthread_cond_init (lock->writeOK, NULL);
	pthread_cond_init (lock->readOK, NULL);
	lock->readers = 0;
	lock->writers = 0;
	lock->waiting = 0;

	return (lock);
}

/*void readlock(rwl *lock, int d) {
  writelock(lock, d);
  }*/

void readlock (rwl *lock, int d)
{
  /*  printf("reader %d\n", d); */
  fflush(NULL);
  
  pthread_mutex_lock (lock->mut);
  if (lock->writers || lock->waiting) {
    do {
      /*      printf ("reader %d blocked. %d readers, %d writers, %d waiting\n", d, lock->readers,  lock->writers, lock->waiting);  */
      pthread_cond_wait (lock->readOK, lock->mut);
      /*      printf ("reader %d unblocked.\n", d);  */
    } while (lock->writers);
  }
  lock->readers++;
  pthread_mutex_unlock (lock->mut);
  /*  printf("reader %d done\n", d); */
  fflush(NULL);
  
  return;
}

void writelock (rwl *lock, int d)
{
  /*  printf("\nwritelock %d\n", d); */
  fflush(NULL);
  pthread_mutex_lock (lock->mut);
  lock->waiting++;
  while (lock->readers || lock->writers) {
    /* printf ("writer %d blocked. %d readers, %d writers, %d waiting\n", d, lock->readers,  lock->writers, lock->waiting);  */
    pthread_cond_wait (lock->writeOK, lock->mut);
    /*    printf ("writer %d unblocked.\n", d);  */
	}
  lock->waiting--;
  lock->writers++;
  pthread_mutex_unlock (lock->mut);

  /* printf("\nwritelock %d done\n", d); */
  fflush(NULL);
  
  return;
}

/*void readunlock(rwl *lock) {
  writeunlock(lock);
  }*/

void readunlock (rwl *lock)
{
  pthread_mutex_lock (lock->mut);
  lock->readers--;
  pthread_cond_signal (lock->writeOK); 

  /* Don't need to broadcast, since only one writer can run at
     once. */

  /*  pthread_cond_broadcast (lock->writeOK);  */ 
  
  pthread_mutex_unlock (lock->mut);
  /*  printf("readunlock done\n"); */
}

void writeunlock (rwl *lock)
{
  /* printf("writeunlock done\n"); */
  fflush(NULL);

  pthread_mutex_lock (lock->mut);
  lock->writers--;
  /* Need this as well (in case there's another writer, which is blocking the all of the readers. */
  pthread_cond_signal (lock->writeOK); 
  pthread_cond_broadcast (lock->readOK);
  pthread_mutex_unlock (lock->mut);
}

void deletelock (rwl *lock)
{
	pthread_mutex_destroy (lock->mut);
	pthread_cond_destroy (lock->readOK);
	pthread_cond_destroy (lock->writeOK);
	free (lock);

	return;
}
