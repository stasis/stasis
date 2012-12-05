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

#include "../../pbl/jbhash.h"
#include <db4/db.h>
#include <errno.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define DATABASE "access.db"
#define ENV_DIRECTORY   "TXNAPP"


void  env_dir_create(void);
void  env_open(DB_ENV **);




int main(void) {
	DB *dbp;
	DB_ENV *dbenv;
	DB_TXN *xid;
	DBT key, data;
	const unsigned int INSERT_NUM = 100;
	char value[22]; /* should be log INSERT_NUM */
	int ret, i, t_ret;

        env_dir_create();
        env_open(&dbenv);

	if ((ret = db_create(&dbp, dbenv, 0)) != 0) {
		fprintf(stderr, "db_create: %s\n", db_strerror(ret));
		exit (1);
	}

	dbenv->txn_begin(dbenv, NULL, &xid, 0);
	if ((ret = dbp->open(dbp,
					xid, DATABASE, NULL, DB_BTREE, DB_CREATE, 0664)) != 0) {
		dbp->err(dbp, ret, "%s", DATABASE);
		goto err;
	}

	memset(&key, 0, sizeof(key));
	memset(&data, 0, sizeof(data));

	key.size = sizeof(int);
	key.data = malloc(sizeof(int));
	data.data = value;

	for( i = 0; i < INSERT_NUM; i++ ) {
		*((int*)key.data) = i;
		data.size = sizeof(char)*strlen(data.data);
		sprintf(value, "value: %u\n", i);
		dbp->put(dbp, xid, &key, &data, 0);
	}

	xid->commit(xid, 0);
	dbenv->txn_begin(dbenv, NULL, &xid, 0);
	
	for( i = 0; i < INSERT_NUM; i++ ) {
		*((int*)key.data) = i;
		dbp->get(dbp, xid, &key, &data, 0);
		printf("db: %u: key retrieved: data was %s.\n", *((int*)key.data), (char *)data.data);
	}

	xid->abort(xid);

err:    if ((t_ret = dbp->close(dbp, 0)) != 0 && ret == 0)
			ret = t_ret; 


	return 0;

}

void
env_dir_create(void) {
        struct stat sb;




        /*
         * If the directory exists, we're done.  We do not further check
         * the type of the file, DB will fail appropriately if it's the
         * wrong type.
         */
        if (stat(ENV_DIRECTORY, &sb) == 0)
                return;




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




        /*
         * Open a transactional environment:
         *      create if it doesn't exist
         *      free-threaded handle
         *      run recovery
         *      read/write owner only
         */
        if ((ret = dbenv->open(dbenv, ENV_DIRECTORY,
            DB_CREATE | DB_INIT_LOCK | DB_INIT_LOG |
            DB_INIT_MPOOL | DB_INIT_TXN | DB_RECOVER ,
            S_IRUSR | S_IWUSR)) != 0) {
                (void)dbenv->close(dbenv, 0);
                fprintf(stderr, "dbenv->open: %s: %s\n",
                    ENV_DIRECTORY, db_strerror(ret));
                exit (1);
        }




        *dbenvp = dbenv;
}
