#include <stasis/rw.h>
#include <assert.h>

#undef pthread_cond_wait
#undef pthread_cond_timedwait


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

void assertlocked(rwl * lock) { 
  assert(lock->writers || lock->readers);
}

void readlock (rwl *lock, int d)
{
  /*  printf("reader %d\n", d);  
      fflush(NULL); */
  
  pthread_mutex_lock (lock->mut);
  if (lock->writers) { // XXX avoids deadlock; lets writers starve... || lock->waiting) {
    do {
      /*      printf ("reader %d blocked. %d readers, %d writers, %d waiting\n", d, lock->readers,  lock->writers, lock->waiting);  */
      pthread_cond_wait (lock->readOK, lock->mut);
      /*      printf ("reader %d unblocked.\n", d);  */
    } while (lock->writers);
  }
  lock->readers++;
  pthread_mutex_unlock (lock->mut);
  /*  printf("reader %d done\n", d); 
      fflush(NULL); */
  
  return;
}
int tryreadlock (rwl *lock, int d)
{
  pthread_mutex_lock (lock->mut);
  if (lock->writers || lock->waiting) {
    pthread_mutex_unlock (lock->mut);
    return 0;
  }
  lock->readers++;
  pthread_mutex_unlock (lock->mut);
  return 1;
}


void writelock (rwl *lock, int d)
{
  /*  printf("\nwritelock %d\n", d);
      fflush(NULL); */
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

  /* printf("\nwritelock %d done\n", d); 
     fflush(NULL); */
  
  return;
}

int trywritelock(rwl *lock, int d) {
  /*  printf("\nwritelock %d\n", d);
      fflush(NULL); */
  pthread_mutex_lock (lock->mut);
  if (lock->readers || lock->writers) {
    pthread_mutex_unlock(lock->mut);
    return 0;
  }
  lock->writers++;
  pthread_mutex_unlock (lock->mut);
  return 1;
} 

void downgradelock(rwl * lock) {
  pthread_mutex_lock(lock->mut);
  assert(lock->writers);
  lock->writers--;
  lock->readers++;
  if(lock->waiting) {
    pthread_cond_signal (lock->writeOK); 
  } else {
    pthread_cond_broadcast(lock->readOK);
  }
  pthread_mutex_unlock(lock->mut);
}

void unlock(rwl * lock) {
  pthread_mutex_lock (lock->mut);
  if(lock->readers) {
    lock->readers--;
    if(lock->waiting) {
      pthread_cond_signal (lock->writeOK);
    }
  } else {
    assert (lock->writers);
    lock->writers--;
    /* Need this as well (in case there's another writer, which is blocking the all of the readers. */
    if(lock->waiting) {
      pthread_cond_signal (lock->writeOK); 
    } else {
      pthread_cond_broadcast (lock->readOK);
    }
  }
  pthread_mutex_unlock (lock->mut);
}

void readunlock(rwl * lock) {
  unlock(lock);
}
void writeunlock(rwl * lock) {
  unlock(lock);
}

void deletelock (rwl *lock)
{
	pthread_mutex_destroy (lock->mut);
	pthread_cond_destroy (lock->readOK);
	pthread_cond_destroy (lock->writeOK);
	free(lock->mut);
	free(lock->writeOK);
	free(lock->readOK);
	free (lock);

	return;
}
