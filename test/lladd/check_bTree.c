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
#include <check.h>
#include "../check_includes.h"

#include <lladd/transactional.h>

#include <assert.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>
#define LOG_NAME   "check_bTree.log"
#define NUM_ENTRIES 100000
/** @test
*/

START_TEST(bTreeFixedPage)
{
 Tinit();
  SimpleExampleFixedPage();
   Tdeinit();
} END_TEST

int SimpleExampleFixedPage(){


  int DEBUGT = 1;
  int xid = Tbegin();
  int pageid1;
  Page * p1;
  
  recordid rid = TfixedPageAlloc(xid, sizeof(int)); // this does the initialize
  
  pageid1 = rid.page;
  
  p1 = loadPage(xid, pageid1);
  
  int num_slots = rid.slot;
  int size_of_slots = rid.size;
  
  if(DEBUGT) { printf("\n rid:size_of_slots  = %d" , size_of_slots); }
  if(DEBUGT) { printf("\n rid:num_slots = %d" , num_slots);}
  
  
  if(DEBUGT) {printf("\n record size (p1) = %d" , fixedPageRecordSize(p1));}
  if(DEBUGT) {printf("\n fixedPageCount (p1) = %d" , fixedPageCount(p1));}
  
  releasePage(p1);
  Tcommit(xid);
  
  return 0;
}
START_TEST(bTreeTest)
{
  Tinit();
  // HelloWorld();
  SimpleExample();
  // printf("\n end of practice run\n");
   Tdeinit();
} END_TEST

int SimpleExample(){

  int DEBUGP = 0;
  int DEBUGT = 0;
  int xid = Tbegin();
  /* Where to find stuff
   * ****************************
   *  TpageAlloc ->         lladd/src/lladd/operations/pageOperations.c
   *  TfixedPageAlloc ->    lladd/src/lladd/operations/pageOperations.c
   *  fixedPageInitailze -> lladd/src/lladd/page/fixed.c
   */
  int pageid1;// = TpageAlloc(xid);

  int pageid2 =  TpageAlloc(xid);

  int pageid3;// =  TpageAlloc(xid);

  Page * p1;// = (Page *) malloc (sizeof(300));
  Page * p2;

 

  //p2 =  loadPage(xid, pageid2);

  //  fixedPageInitialize(p1, sizeof(int) , 4); 


  recordid rid = TfixedPageAlloc(xid, sizeof(int)); // this does the initialize

  pageid1 = rid.page;

  p1 = loadPage(xid, pageid1);

  int num_slots = rid.slot;
  int size_of_slots = rid.size;

  if(DEBUGP) { printf("\n size of int = %d ", sizeof(int));}
     
  if(DEBUGT) { printf("\n rid:size_of_slots  = %d" , size_of_slots); }
  if(DEBUGT) { printf("\n rid:num_slots = %d" , num_slots);}
	   
  if(DEBUGP) {  printf("\n rid:pageid num = %d" , pageid1);}
  
  
  
  
  // int page_type = *page_type_ptr(p1);
  
  //  if(DEBUGP) { printf ("\n\n *page_type_ptr (p1) = %d \n" ,( *page_type_ptr(p1) == FIXED_PAGE)); }
  
  // if(DEBUGT) { printf ("\n\n *page_type_ptr (p1) = %d \n" , page_type_ptr(p1));}
  
  // if(DEBUGT) { printf ( page_type_ptr(p1));}
  
  //  * page_type_ptr(p1);

  // int  i = page_type_ptr(p1);
  
  
  if(DEBUGT) {printf("\n record size (p1) = %d" , fixedPageRecordSize(p1));}
  if(DEBUGT) {printf("\n fixedPageCount (p1) = %d" , fixedPageCount(p1));}

 
  
  if(DEBUGT) {  printf("\n%x\n", p1);}

  //  recordid r =  fixedRawRallocMany(p1, 4);
  //fixedRawRallocMany(p1, 4);
  if(DEBUGP) {printf("\n");}
  if(DEBUGT) {printf("\n Just for FUN ....  \n  .... ");}

  Page * p = & p1;
  // int page_type = *page_type_ptr(p); // copied from page.c

  //  if(DEBUGT) {printf("\n page_type = %d", page_type);}

  releasePage(p1);
  Tcommit(xid);


  return 0;
}

/** @test
*/

#define NUM_ENTRIES_XACT 10000

Suite * check_suite(void) {
  Suite *s = suite_create("bTree");
  /* Begin a new test */
  TCase *tc = tcase_create("simple");

  tcase_set_timeout(tc, 0); // disable timeouts if test takes more than 2 sec - it would get killed 

  /* Sub tests are added, one per line, here */
   tcase_add_test(tc, bTreeTest);
   tcase_add_test(tc, bTreeFixedPage);
   //  tcase_add_test(tc, simpleLinearHashTest); // put back in if playing with hashtable

  
  /* --------------------------------------------- */
  
   tcase_add_checked_fixture(tc, setup, teardown);// leave stuff below here. 

  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
