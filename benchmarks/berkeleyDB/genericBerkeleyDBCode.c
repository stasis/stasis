#include "genericBerkeleyDBCode.h"
DB_ENV *dbenv;
DB *db_cats;

int commitCount = 0;
int putCount = 0;
void
env_dir_create()
{
	struct stat sb;

	/*
	 * If the directory exists, we're done.  We do not further check
	 * the type of the file, DB will fail appropriately if it's the
	 * wrong type.
	 */
	if (stat(ENV_DIRECTORY, &sb) == 0) {
	  printf("Error!  Directory already exists!\n");
	  fflush(stdout);
	  abort();  // This is a benchmark!!!
	  return;
	}

	/* Create the directory, read/write/access owner only. */
	if (mkdir(ENV_DIRECTORY, S_IRWXU) != 0) {
		fprintf(stderr,
		    "txnapp: mkdir: %s: %s\n", ENV_DIRECTORY, strerror(errno));
		exit (1);
	}
}

void
env_open(DB_ENV **dbenvp)
{
	DB_ENV *dbenv;
	int ret;

	/* Create the environment handle. */
	if ((ret = db_env_create(&dbenv, 0)) != 0) {
		fprintf(stderr,
		    "txnapp: db_env_create: %s\n", db_strerror(ret));
		exit (1);
	}

	/* Set up error handling. */
	dbenv->set_errpfx(dbenv, "txnapp");
	dbenv->set_errfile(dbenv, stderr);

	/* Do deadlock detection internally. */
	/*	if ((ret = dbenv->set_lk_detect(dbenv, DB_LOCK_DEFAULT)) != 0) {
		dbenv->err(dbenv, ret, "set_lk_detect: DB_LOCK_DEFAULT");
		exit (1);
		}*/

	dbenv->set_tx_max(dbenv, 32000);
	int max;
	dbenv->get_tx_max(dbenv, &max);
	printf("Max xact count: %d\n", max);


	/*
	 * Open a transactional environment:
	 *	create if it doesn't exist
	 *	free-threaded handle
	 *	run recovery
	 *	read/write owner only
	 */
	if ((ret = dbenv->open(dbenv, ENV_DIRECTORY,
			       DB_CREATE |/* DB_INIT_LOCK |*/ DB_INIT_LOG | /*| DB_PRIVATE*/ 
			       DB_INIT_MPOOL | DB_INIT_TXN | DB_RECOVER | DB_THREAD,
			       S_IRUSR | S_IWUSR)) != 0) {
		dbenv->err(dbenv, ret, "dbenv->open: %s", ENV_DIRECTORY);
		exit (1);
	}

	*dbenvp = dbenv;
}

void *
checkpoint_thread(void *arg)
{
	DB_ENV *dbenv;
	int ret;

	dbenv = arg;
	dbenv->errx(dbenv, "Checkpoint thread: %lu", (u_long)pthread_self());

	/* Checkpoint once a minute. */
	for (;; sleep(60))
		if ((ret = dbenv->txn_checkpoint(dbenv, 0, 0, 0)) != 0) {
			dbenv->err(dbenv, ret, "checkpoint thread");
			exit (1);
		}

	/* NOTREACHED */
}

void *
logfile_thread(void *arg)
{
	DB_ENV *dbenv;
	int ret;
	char **begin, **list;

	dbenv = arg;
	dbenv->errx(dbenv,
	    "Log file removal thread: %lu", (u_long)pthread_self());

	/* Check once every 5 minutes. */
	for (;; sleep(300)) {
		/* Get the list of log files. */
		if ((ret =
		    dbenv->log_archive(dbenv, &list, DB_ARCH_ABS)) != 0) {
			dbenv->err(dbenv, ret, "DB_ENV->log_archive");
			exit (1);
		}

		/* Remove the log files. */
		if (list != NULL) {
			for (begin = list; *list != NULL; ++list)
				if ((ret = remove(*list)) != 0) {
					dbenv->err(dbenv,
					    ret, "remove %s", *list);
					exit (1);
				}
			free (begin);
		}
	}
	/* NOTREACHED */
}

void
log_archlist(DB_ENV *dbenv)
{
	int ret;
	char **begin, **list;

	/* Get the list of database files. */
	if ((ret = dbenv->log_archive(dbenv,
	    &list, DB_ARCH_ABS | DB_ARCH_DATA)) != 0) {
		dbenv->err(dbenv, ret, "DB_ENV->log_archive: DB_ARCH_DATA");
		exit (1);
	}
	if (list != NULL) {
		for (begin = list; *list != NULL; ++list)
			printf("database file: %s\n", *list);
		free (begin);
	}

	/* Get the list of log files. */
	if ((ret = dbenv->log_archive(dbenv,
	    &list, DB_ARCH_ABS | DB_ARCH_LOG)) != 0) {
		dbenv->err(dbenv, ret, "DB_ENV->log_archive: DB_ARCH_LOG");
		exit (1);
	}
	if (list != NULL) {
		for (begin = list; *list != NULL; ++list)
			printf("log file: %s\n", *list);
		free (begin);
	}
}
DB *db;

void
db_open(DB_ENV *dbenv, DB **dbp, char *name, int type)
{
	int ret;

	/* Create the database handle. */
	if ((ret = db_create(&db, dbenv, 0)) != 0) {
		dbenv->err(dbenv, ret, "db_create");
		exit (1);
	}

	/* Optionally, turn on duplicate data items. */
	/*	if (dups && (ret = db->set_flags(db, DB_DUP)) != 0) {
		dbenv->err(dbenv, ret, "db->set_flags: DB_DUP");
		exit (1);
		} */

	/*
	 * Open a database in the environment:
	 *	create if it doesn't exist
	 *	free-threaded handle
	 *	read/write owner only
	 */
	if ((ret = db->open(db, NULL, name, NULL, type, /*DB_DIRECT_LOG | DB_DIRECT_DB | */
			    DB_AUTO_COMMIT | DB_DIRTY_READ | DB_TXN_SYNC | DB_CREATE | DB_THREAD, S_IRUSR | S_IWUSR)) != 0) {
		(void)db->close(db, 0);
		dbenv->err(dbenv, ret, "db->open: %s", name);
		exit (1);
	}

	*dbp = db;
}

void
run_xact(DB_ENV *dbenv, DB *db, int offset, int count)
{
  //	va_list ap;
	DBC *dbc;
	DBT key, data;
	DB_TXN *tid;
	int ret;
	char *s;

	/* Initialization. */
	memset(&key, 0, sizeof(key));
	memset(&data, 0, sizeof(data));
	int keyPtr;
	int valPtr;
	key.data = &keyPtr;
	key.size = sizeof(int);/*strlen(name);*/
	data.data = &valPtr;
	data.size = sizeof(int);

retry:	/* Begin the transaction. */
	if ((ret = dbenv->txn_begin(dbenv, NULL, &tid, 0)) != 0) {
		dbenv->err(dbenv, ret, "DB_ENV->txn_begin");
		exit (1);
	}

	/* Delete any previously existing item. */
	/*	switch (ret = db->del(db, tid, &key, 0)) {
	case 0:
	case DB_NOTFOUND:
	  break;
	case DB_LOCK_DEADLOCK:
	  abort();
	  / * Deadlock: retry the operation. * /
			  if ((ret = tid->abort(tid)) != 0) {
			    dbenv->err(dbenv, ret, "DB_TXN->abort");
			    exit (1);
			  }
			  goto retry;
	default:
	  //	  dbenv->err(dbenv, ret, "db->del: %s", name);
	  abort();
	  exit (1);
	} */
 

	/* Create a cursor. */
	/*	if ((ret = db->cursor(db, tid, &dbc, 0)) != 0) {
		dbenv->err(dbenv, ret, "db->cursor");
		exit (1);
		} */

	int q;
	/* Count is one for this test */
	//	assert(count == 1);

	for(q = offset; q < offset + count; q++) {
	  keyPtr = q+1;
	  valPtr = q;
	  /*	  switch (ret = db->del(db, tid, &key, 0)) {
	  case 0:
	  case DB_NOTFOUND:
	    break;
	  case DB_LOCK_DEADLOCK:
	    abort();
	    // Deadlock: retry the operation. 
	    if ((ret = tid->abort(tid)) != 0) {
	      dbenv->err(dbenv, ret, "DB_TXN->abort");
	      exit (1);
	    }
	    goto retry;
	  default:
	    //	  dbenv->err(dbenv, ret, "db->del: %s", name);
	    abort();
	    exit (1);
	    } */
	  		
	  //	  pthread_mutex_lock(&hack);
	  switch (ret = db->put(db, tid, &key, &data, 0)) {
	  case 0:
	    break;
	  case DB_LOCK_DEADLOCK:
	    //		  va_end(ap);
	    abort();  // Locking should be disabled!
	    /* Deadlock: retry the operation. */
	    if ((ret = dbc->c_close(dbc)) != 0) {
	      dbenv->err(
			 dbenv, ret, "dbc->c_close");
	      exit (1);
	    }
	    if ((ret = tid->abort(tid)) != 0) {
	      dbenv->err(dbenv, ret, "DB_TXN->abort");
	      exit (1);
		  }
	    goto retry;
	  default:
	    /* Error: run recovery. */
	    dbenv->err(dbenv, ret, "dbc->put: %d/%d", q, q);
	    abort();  // Error invalidates benchmark!
	    exit (1);
	  }
	  //	  pthread_mutex_unlock(&hack);
	  putCount++;
	}
	

	if ((ret = tid->commit(tid, 0)) != 0) {
		dbenv->err(dbenv, ret, "DB_TXN->commit");
		exit (1);
	}
	commitCount++;
#ifdef DEBUG_BDB
	printf("Called commit\n");
#endif
}

void
usage()
{
	(void)fprintf(stderr, "usage: txnapp\n");
	exit(1);
}

void 
initDB(/*DB_ENV ** dbenv, */pthread_attr_t * attr, int type)  
{

	pthread_t ptid;

	env_dir_create();
	env_open(&dbenv);
	int ret;
	/*
	///Start a checkpoint thread.
	if ((ret = pthread_create(
	    &ptid, attr, checkpoint_thread, (void *)dbenv)) != 0) {
		fprintf(stderr,
		    "txnapp: failed spawning checkpoint thread: %s\n",
		    strerror(ret));
		exit (1);
	}

	/// Start a logfile removal thread. 
	if ((ret = pthread_create(
	    &ptid, attr, logfile_thread, (void *)dbenv)) != 0) {
		fprintf(stderr,
		    "txnapp: failed spawning log file removal thread: %s\n",
		    strerror(ret));
		exit (1);
	}
	*/
	db_open(dbenv, &db_cats, "cats", type);



}
