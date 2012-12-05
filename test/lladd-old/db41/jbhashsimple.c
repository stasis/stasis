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
#include <sys/types.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <db4/db.h>



#define DATABASE "access.db"




int main(void) {
	DB *dbp;
	DBT key1, dat1, key2, dat2, key3, dat3, key4, dat4;
	int ret, i, t_ret;



	if ((ret = db_create(&dbp, NULL, 0)) != 0) {
		fprintf(stderr, "db_create: %s\n", db_strerror(ret));
		exit (1);
	}
	if ((ret = dbp->open(dbp,
					NULL, DATABASE, NULL, DB_BTREE, DB_CREATE, 0664)) != 0) {
		dbp->err(dbp, ret, "%s", DATABASE);
		goto err;
	}




	memset(&key1, 0, sizeof(key1));
	memset(&dat1, 0, sizeof(dat1));
	memset(&key2, 0, sizeof(key1));
	memset(&dat2, 0, sizeof(dat1));
	memset(&key3, 0, sizeof(key1));
	memset(&dat3, 0, sizeof(dat1));
	memset(&key4, 0, sizeof(key1));
	memset(&dat4, 0, sizeof(dat1));

	key1.size = key2.size = key3.size = key4.size = sizeof(int);
	key1.data = malloc(sizeof(int));
	key2.data = malloc(sizeof(int));
	key3.data = malloc(sizeof(int));
	key4.data = malloc(sizeof(int));

	*((int*)key1.data) = 1; dat1.data = "one"; dat1.size = 4;
	*((int*)key2.data) = 2; dat2.data = "two"; dat2.size = 4;
	*((int*)key3.data) = 3; dat3.data = "three"; dat3.size = 6;
	*((int*)key4.data) = 4; dat4.data = "four"; dat4.size = 5;
	
	if( ret = (
				dbp->put(dbp, NULL, &key1, &dat1, 0) ||
				dbp->put(dbp, NULL, &key2, &dat2, 0) ||
				dbp->put(dbp, NULL, &key3, &dat3, 0) ||
				dbp->put(dbp, NULL, &key4, &dat4, 0) ))
	{
		dbp->err(dbp, ret, "DB->put");
		goto err;
	} else {
		printf("db: keys stored.\n");
	}


	for( i = 1; i <= 4; i++ ) {
		*((int*)key1.data) = i;

		if ((ret = dbp->get(dbp, NULL, &key1, &dat1, 0)) == 0)
			printf("db: %u: key retrieved: data was %s.\n", *((int*)key1.data), (char *)dat1.data);
		else {
			dbp->err(dbp, ret, "DB->get");
			goto err;
		}
	}

err:    if ((t_ret = dbp->close(dbp, 0)) != 0 && ret == 0)
			ret = t_ret; 




	return 0;

}
