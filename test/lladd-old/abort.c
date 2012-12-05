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
/*********************************
 * $Id: abort.c 2 2004-06-24 21:10:31Z sears $
 *
 * aborting and committing transactions
 * *******************************/

#include "test.h"

int test(void) {
  int xid_0;
  int writeVal_0;
  int readVal_0;
  recordid rec_0;

  int xid_1;
  int writeVal_1;
  int readVal_1;
  recordid rec_1;

  printf("\nRunning test4\n");

  

  writeVal_0 = 314159265;
  writeVal_1 = 271828183;

  xid_0 = Tbegin();
  xid_1 = Tbegin();
    
  rec_0 = Talloc(xid_0, sizeof(int));
  rec_1 = Talloc(xid_1, sizeof(int));
  
  Tset(xid_0, rec_0, &writeVal_0);
  printf("xid %d set rid (%d, %d) to value [%d]\n", xid_0, rec_0.page, rec_0.slot, writeVal_0);

  Tset(xid_1, rec_1, &writeVal_1);
  printf("xid %d set rid (%d, %d) to value [%d]\n", xid_1, rec_1.page, rec_1.slot, writeVal_1);

  Tread(xid_0, rec_0, &readVal_0);
  printf("xid %d read from rid (%d, %d) the value [%d]\n", xid_0, rec_0.page, rec_0.slot, readVal_0);

  Tread(xid_1, rec_1, &readVal_1);
  printf("xid %d read from rid (%d, %d) the value [%d]\n", xid_1, rec_1.page, rec_1.slot, readVal_1);
  
  if(1) {
    Tabort(xid_0);
    printf("xid %d aborted\n", xid_0);
    
    Tcommit(xid_1);
    printf("xid %d committed\n", xid_1);
  } else {

    Tcommit(xid_0);
    printf("xid %d committed\n", xid_0);
    
    Tabort(xid_1);
    printf("xid %d aborted\n", xid_1);
  }
  
  xid_0 = Tbegin();
  xid_1 = Tbegin();
  Tread(xid_0, rec_0, &readVal_0);
  printf("xid %d read from rid (%d, %d) the value [%d]\n", xid_0, rec_0.page, rec_0.slot, readVal_0);

  Tread(xid_1, rec_1, &readVal_1);
  printf("xid %d read from rid (%d, %d) the value [%d]\n", xid_1, rec_1.page, rec_1.slot, readVal_1);
  
  Tcommit(xid_0);
  Tcommit(xid_1);
  
  

  printf("\n");
  return 0;
}
