/*---
This software is copyrighted by the Regents of the University of
California and Ashok Sudarsanam. The following terms apply to all files
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

#include "fitest.h"

int
runChecker() {
  int hashVal, fd, i, xid, k;
  int *commitTable = NULL, *insertTable = NULL, tableLength;
  int *key, **bkey, *value, **bvalue;
  int keySize, valueSize;
  lladd_hash_iterator* hashIter;
  FILE *commitFile, *insertFile;

  printf("\tRunning Checker Process\n"); fflush(stdout);

  readGlobalsFromSharedBuffer();

  /* Read in the hash table recordid from disk. */
  fd = open("recordid.txt", O_RDONLY, 0777);
  read(fd, (char*) &hashTable, sizeof(recordid));
  close(fd);

  /* Open the insert and commit log files. */
  insertFile = fopen("inserts.txt", "r");
  commitFile = fopen("commits.txt", "r");

  /* Allocate two tables to keep track of all hash table inserts that were
   * possibly committed (insert table) and definitely committed (commit table).
   */
  tableLength = baseKey + (numThreads * opsPerThread);
  insertTable = (int*) calloc(tableLength, sizeof(int));
  commitTable = (int*) calloc(tableLength, sizeof(int));

  /* Read all the entries from the insert log (commit log) and insert into the 
   * insert table (commit table).
   */
  while (fscanf(insertFile, "%d\n", &hashVal) != EOF) {
    assert(hashVal < tableLength && insertTable[hashVal] == 0);
    insertTable[hashVal] = 1;
  }

  while (fscanf(commitFile, "%d\n", &hashVal) != EOF) {
    assert(hashVal < tableLength && commitTable[hashVal] == 0);
    commitTable[hashVal] = 1;
  }

  /* Iterate over all hash table entries.  Report an error if any entry exists in the hash 
   * table that was not committed, i.e.:
   *   (1) entry >= tableLength, or 
   *   (2) insertTable[entry] == 0 && commitTable[entry] == 0 
   */
  bkey = &key;
  bvalue = &value;
  Tinit();
  xid = Tbegin();

  hashIter = ThashIterator(xid, hashTable, sizeof(int), sizeof(int));
  while (ThashNext(xid, hashIter, (byte**) bkey, &keySize, (byte**) bvalue, &valueSize)) {
    k = *key;

    if (k >= tableLength || (insertTable[k] == 0 && commitTable[k] == 0)) {
      printf("\t\tERROR: <%d> inserted but not committed\n", k); fflush(stdout);
      return 1;
    }
    else {
      commitTable[k] = 0;
    }
  }

  Tabort(xid);
  Tdeinit();

  /* Now iterate over all commit table entries.  If any entry was committed but not 
   * inserted into the hash table (i.e., commitTable[entry] == 1), then report an error.
   */
  for (i = 0; i < tableLength; i++) {
    if (commitTable[i] == 1) {
      printf("\t\tERROR: <%d> committed but not inserted\n", i); fflush(stdout);
      return 1;
    }
  }

  /* Close the insert and commit log files. */
  fclose(insertFile);
  fclose(commitFile);

  printf("\t\tChecker Process Exiting Normally\n"); fflush(stdout);

  return 0;
}


