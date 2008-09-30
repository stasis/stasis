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
#include <config.h>
#include <syslog.h>
#include <unistd.h>
#include <assert.h>
#include "cyrusdb.h"
#include "exitcodes.h"
#include "string.h"
#include "xmalloc.h"

#include <lladd/transactional.h>
#include "../../pbl/jbhash.h"


struct db {
	jbHashTable_t *nameSpace;
};

struct txn {
	int tid;
};

static int dirExists(char *dir)
{
	char *curDir = getcwd(NULL, 0);
	int status = chdir(dir);
	chdir(curDir);
	free(curDir);
	return (status == 0);
}

static char *dbdirName = 0;
static int dbinit = 0;
static void openNameSpace(const char *filename, jbHashTable_t ** nameHT);
static void writeNameSpace(const char *filename, jbHashTable_t * nameHT);

/*
 * init() should be called once per process; no calls are legal until init() returns
 */
static int init(const char *dbdir, int myflags)
{
	dbdirName = malloc(strlen(dbdir));
	strcpy(dbdirName, dbdir);
	if (!dirExists(dbdirName))
		mkdir(dbdirName);
	dbinit = 1;
	return 0;
}

/*
 * done() should be called once per process; no calls are legal once done() starts.  it is legal to call
 * init() after done() returns to reset state
 */
static int done(void)
{
	free(dbdirName);
	dbdirName = 0;
	dbinit = 0;
	return 0;
}

/* checkpoints this database environment */
static int mysync(void)
{
	return 0;		/* no need to checkpoint */
}

/*
 * archives this database environment, and specified databases into the specified directory
 */
static int archive(const char **fnames, const char *dirname)
{
	/* looks like this copies the db file and the log to a new directory */
	jbHashTable_t *oldHT, *newHT;
	const char **list = fnames;
	recordid loc;
	static char archiveName[200];
        static char data[600];	/* max is 600 now! should be a better system for this... */
	static char curKey[80];
	int xid, status, datalen, keylen;
	struct txn **trans;
	if (list != NULL) {
		while (*list != NULL) {
			openNameSpace(*list, &oldHT);
			if (oldHT != 0) {
				syslog(LOG_ERR, "DBERROR: error archiving database file: %s", *list);
				return CYRUSDB_IOERROR;
			}
			/* theoretically, can use foreach here */
			xid = Tbegin();
			newHT = jbHtCreate(xid);
			strcpy(archiveName, "archive/");
			strcat(archiveName, *list);
			writeNameSpace(archiveName,newHT);
			for (datalen = jbHtFirst(xid, oldHT, data); datalen > 0; datalen = jbHtNext(xid, oldHT, data)) {
				loc = Talloc(xid, datalen);
				Tset(xid, loc, data);
				keylen = jbHtCurrentKey(xid, oldHT, curKey);
				jbHtInsert(xid, newHT, curKey, keylen, data, datalen);
			}
			Tcommit(xid);
			free(newHT);
			list++;
		}
	}
	return 0;
}

/*
 * Pass in a filename and an unallocated hashtable, will return either a valid malloc'ed hashtable or null
 * into nameHT
 */
static void openNameSpace(const char *filename, jbHashTable_t ** nameHT)
{
	int count;
	FILE *fp = fopen(filename, "r");
	recordid rid;
	if (fp != 0) {
		*nameHT = xmalloc(sizeof(jbHashTable_t));
		for (count = 0; count < sizeof(recordid); count++)
			*(char *)(&rid + count) = fgetc(fp);
		fclose(fp);
		Tread(69, rid, *nameHT);
		if (!jbHtValid(69, *nameHT)) {
			free(*nameHT);
			*nameHT = NULL;	/* not a valid hashtable, so return null */
		}
	} else {		/* file does not exist */
		fclose(fp);
		*nameHT = NULL;
	}
}

static void writeNameSpace(const char *filename, jbHashTable_t * nameHT)
{
	int count;
	FILE *fp = fopen(filename, "w");
	if (fp != 0) {
		for (count = 0; count < sizeof(recordid); count++)
			fputc(*(char *)(&nameHT->store+count), fp);
	}
}

/* open the specified database in the global environment */
static int myopen(const char *fname, int flags, struct db ** ret)
{
	jbHashTable_t *nameHT;
	static char filename[40];
	struct db *newdb = xmalloc(sizeof(struct db));
	int xid, i;
	FILE *fp;
	recordid rid;
	assert(dbinit && fname && ret);
	/* prepare the filename by concatenating the dbdir with the filename */
	strcpy(filename, dbdirName);
	strcat(filename, "/");
	strncat(filename, fname, strlen(fname));

	/* this is where you load the namespace for this db into memory */
	openNameSpace(filename, &nameHT);
	if (nameHT != NULL) {
		/* that means we found a valid hashtable in the filename */
		newdb->nameSpace = nameHT;
		*ret = newdb;
		return 0;
	}
	/* by this point */


	/* if we've reached here, that means the HT didn't exist */
	free(nameHT);		/* get rid of the attempt to find the HT */
	/* make a xid just for the creation of the namespace hashtable */
	xid = Tbegin();
	newdb->nameSpace = jbHtCreate(xid);	/* make a new nameSpace just for this database */
	rid = newdb->nameSpace->store;	/* get the rid of this namespace to save to a file */
	fp = fopen(filename, "w");
	if (fp == 0) {
		syslog(LOG_ERR, "DBERROR: opening %s for writing", filename);
		return CYRUSDB_IOERROR;
	}
	for (i = 0; i < sizeof(recordid); i++)
		fputc(*(char *)(&rid + i), fp);
	fclose(fp);		/* this flushes the location to disk */
	Tcommit(xid);		/* commit the transaction only when the intermediate step of recording the
				 * location of it has been flushed to disk */
	/*
	 * note: this means it's possible to record an rid on disk without it actually being written there,
	 * so therefore check validity of HT next time
	 */
	*ret = newdb;
	return 0;
}

/* close the specified database */
static int myclose(struct db * db)
{
	/*
	 * since there are transactions, no need to do anything to them now should be all committed by this
	 * point just deallocate the hashtable
	 */
	assert(dbinit && db);
	free(db->nameSpace);
	free(db);
	return 0;
}

/* what are the overall specifications? */
/*
 * 'mydb': the database to act on 'key': the key to fetch.  cyrusdb currently requires this to not have any
 * of [\t\n\0] in keys 'keylen': length of the key 'data': where to put the data (generally won't have
 * [\n\0]) 'datalen': how big is the data? 'mytid': may be NULL, in which case the fetch is not txn
 * protected. if mytid != NULL && *mytid == NULL, begins a new txn if mytid != NULL && *mytid != NULL,
 * continues an old txn
 * 
 * transactions may lock the entire database on some backends. beware
 * 
 * fetchlock() is identical to fetch() except gives a hint to the underlying database that the key/data being
 * fetched will be modified soon. it is useless to use fetchlock() without a non-NULL mytid
 */
static int fetch(struct db * mydb,
		  const char *key, int keylen,
		  const char **data, int *datalen,
		  struct txn ** mytid)
{
	recordid rid;
	int xid, status;
	assert(dbinit && mydb);
	if (mytid != NULL && *mytid == NULL) {	/* make a new transaction */
		/* begin a new transaction */
		*mytid = (struct txn *) xmalloc(sizeof(struct txn));
		(*mytid)->tid = Tbegin();
		xid = (*mytid)->tid;
	} else if (mytid != NULL) {	/* therefore *mytid!=NULL, continue an old txn */
		xid = (*mytid)->tid;
	} else if (mytid == NULL) {
		/* not transaction protected */
		xid = 69;
		/*
		 * note, Tread shouldn't have xid needed, but it's there so put in junk xid
		 */
	}
	/* get the record given the key */
	status = jbHtLookup(xid, mydb->nameSpace, key, keylen, &rid);
	if (status == -1) {
		/* invalid key! return error */
		Tabort(xid);
		if (mytid && *mytid)
			free(*mytid);
		return -1;	/* what is the return for an error? */
	}
	Tread(xid, rid, *data);
}

static int fetchlock(struct db * mydb,
		      const char *key, int keylen,
		      const char **data, int *datalen,
		      struct txn ** mytid)
{
	/* for now, the same as fetch */
	return fetch(mydb, key, keylen, data, datalen, mytid);
}

/*
 * foreach: iterate through entries that start with 'prefix' if 'p' returns true, call 'cb'
 * 
 * if 'cb' changes the database, these changes will only be visible if they are after the current database
 * cursor.  If other processes change the database (i.e. outside of a transaction) these changes may or may
 * not be visible to the foreach()
 * 
 * 'p' should be fast and should avoid blocking it should be safe to call other db routines inside of 'cb'.
 * however, the "flat" backend is currently are not reentrant in this way unless you're using transactions
 * and pass the same transaction to all db calls during the life of foreach()
 */
static int foreach(struct db * mydb,
		    char *prefix, int prefixlen,
		    foreach_p * p,
		    foreach_cb * cb, void *rock,
		    struct txn ** tid)
{
	char data[600];
	char curKey[80];
	int datalen, keylen, xid, r;
	if (tid && *tid)
		xid = (*tid)->tid;
	else
		xid = 69;
	assert(dbinit && mydb && cb);
	for (datalen = jbHtFirst(xid, mydb->nameSpace, data); datalen > 0; datalen = jbHtNext(xid, mydb->nameSpace, data)) {
		/* does this match our prefix? */
		keylen = jbHtCurrentKey(xid, mydb->nameSpace, curKey);
		if (prefixlen && memcmp(curKey, prefix, prefixlen))
			continue;
		/* else we have a match! */
		if (p(rock, curKey, keylen, data, datalen)) {
			/* we have a winner! */
			r = cb(rock, curKey, keylen, data, datalen);
			if (r != 0) {
				if (r < 0) {
					syslog(LOG_ERR, "DBERROR: foreach cb() failed");
				}
				/* don't mistake this for a db error  -- WHY?? */
				r = 0;

				break;
			}
		}
	}
}

/*
 * mystore combines create and store and delete, flags: 0 = storing and nooverwrite 1 = storing and overwrite
 * 2 = deleting
 * 
 */
static int mystore(struct db * db,
		    const char *key, int keylen,
		    const char *data, int datalen,
		    struct txn ** tid, int flags)
{
	int xid, status;
	struct txn *newtxn;
	recordid rid;
	assert(dbinit && db && key && keylen);
	/* check tid first!! if *mytid null, start a new transaction */
	if (!tid) {
		/* create a new transaction with no intention of committing it (ever) */
		xid = Tbegin();
	} else if (!*tid) {
		/* make a new transaction */
		newtxn = xmalloc(sizeof(struct txn));
		newtxn->tid = Tbegin();
		xid = newtxn->tid;
		*tid = newtxn;
	} else
		xid = (*tid)->tid;	/* transaction already exists */
	/* first see if a variable of this key exists */
	status = jbHtLookup(xid, db->nameSpace, key, keylen, &rid);
	if (status == -1) {	/* this variable doesn't exist yet */
		if (flags != 2) {	/* only add variable if you aren't deleting it, duh */
			/* key doesn't exist, so make it and allocate space */
			rid = Talloc(xid, datalen);
			jbHtInsert(xid, db->nameSpace, key, keylen, &rid, sizeof(recordid));
		}		/* else if you are deleting this, nothing to do since variable never existed!
				 * yay! */
	} else {
		/* key exists */
		switch (flags) {
		case 0:
			/* -- if no overwrite, return error */
			Tabort(xid);
			if (tid && *tid)
				free(*tid);
			return -1;
			break;
		case 2:
			/* delete: so remove it from namespace */
			jbHtRemove(xid, db->nameSpace, key, keylen, NULL);
			break;
		case 1:
			break;
		}
	}
	if (flags != 2)
		Tset(xid, rid, data);
	/* else Tdealloc(xid, rid); ** IS THERE A DEALLOC YET? ** */
	if (!tid)		/* was a one time transaction, commit it */
		Tcommit(xid);
}

/*
 * Place entries in database create will not overwrite existing entries
 */
static int create(struct db * db,
		   const char *key, int keylen,
		   const char *data, int datalen,
		   struct txn ** tid)
{
	return mystore(db, key, keylen, data, datalen, tid, 0);	/* no overwrite, not deleting */

}

static int store(struct db * db,
		  const char *key, int keylen,
		  const char *data, int datalen,
		  struct txn ** tid)
{
	return mystore(db, key, keylen, data, datalen, tid, 1);	/* overwrite, not deleting */
}

/* Remove entrys from the database */
static int delete(struct db * db,
		   const char *key, int keylen,
		   struct txn ** tid,
		   int force)
{				/* 1 = ignore not found errors */
	return mystore(db, key, keylen, NULL, 0, tid, 2);	/* overwrite (meaningless), deleting */
}

/*
 * Commit the transaction.  When commit() returns, the tid will no longer be valid, regardless of if the
 * commit succeeded or failed
 */
static int mycommit(struct txn * tid)
{
	Tcommit(tid->tid);
	assert(dbinit && tid);
	free(tid);
}

static int mycommit_db(struct db * db, struct txn * tid)
{
	return mycommit(tid);
}

/* Abort the transaction and invalidate the tid */
static int myabort(struct txn * tid)
{
	assert(dbinit && tid);
	Tabort(tid->tid);
	free(tid);
}

static int myabort_db(struct db * db, struct txn * tid)
{
	return myabort(tid);
}

struct cyrusdb_backend cyrusdb_lladd =
{
	"lladd",		/* name */

	&init,
	&done,
	&mysync,
	&archive,

	&myopen,
	&myclose,

	&fetch,
	&fetchlock,
	&foreach,
	&create,
	&store,
	&delete,

	&mycommit_db,
	&myabort_db,

	NULL,
	NULL
};
