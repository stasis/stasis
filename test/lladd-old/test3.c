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
/********************************
 * $Id: test3.c 2 2004-06-24 21:10:31Z sears $
 *
 * ******************************/

/**
 * This test creates one transaction that does one int write
 * and one write per record for NUM_TRIALS records
 */


#include "test.h"

#define NUM_TRIALS 10
#define VERBOSE 1


int test(int argc, char **argv) {
  int xids[NUM_TRIALS];
  int initVals[NUM_TRIALS];
  int writeVals[NUM_TRIALS];
  int readVals[NUM_TRIALS];
  int commits[NUM_TRIALS];
  int i;
  recordid recs[NUM_TRIALS];
  int failed = 0;

  printf("\nRunning %s\n", __FILE__);

  for (i = 0; i < NUM_TRIALS; i++) {
    xids[i] = Tbegin();
    initVals[i] = rand();
    recs[i] = Talloc(xids[i], sizeof(int));
    Tset(xids[i], recs[i], &initVals[i]);
    Tcommit(xids[i]);
  }

  

  for (i = 0; i < NUM_TRIALS; i++) {
    xids[i] = Tbegin();
    commits[i] = 0;
    writeVals[i] = rand();
  }

  for (i = 0; i < NUM_TRIALS; i++) {
    Tset(xids[i], recs[i], &writeVals[i]);
  }
  
  for (i = 0; i < NUM_TRIALS; i++) {
    if (rand() % 2) { 
      Tcommit(xids[i]);
      commits[i] = 1;
    } else {
      Tabort(xids[i]);
   }
  }

  for (i = 0; i < NUM_TRIALS; i++) {
      Tread(xids[i], recs[i], &readVals[i]);
  }


  for (i = 0; i < NUM_TRIALS; i++) {
    if (VERBOSE) {
      if (commits[i])
         printf("xid %d commited value %d, and read %d\n", xids[i], writeVals[i], readVals[i]);
      else
         printf("xid %d aborted while setting value %d, and read %d and should've read %d\n", xids[i], writeVals[i], readVals[i], initVals[i]);
          
    }
    
  if (commits[i]) {
     if (writeVals[i] != readVals[i])
        failed = 1;
  } else {
     if (initVals[i] != readVals[i])
        failed = 1;    
  }  
}  
  
  printf("%s\n\n", failed ? "****FAILED****" : "PASSED");

  return failed;
}
