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

#include "../check_includes.h"

#include <stasis/transactional.h>
#include <stasis/page.h>

#include <assert.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>
#define LOG_NAME   "check_bTree.log"
#define NUM_ENTRIES 100000
/** @test
*/

void testFunctions();
int SimpleExample();

START_TEST(bTreeTest)
{
  Tinit();
  testFunctions();
  SimpleExample();
  // printf("\n end of practice run\n");
   Tdeinit();
} END_TEST

/* This only takes in a page that is already initialized as a fixed page.
   and has been initialized as a BTree Node, which involves placing an
   index to tell how many entries are currently valid.
   For now it will just return false if you try to add something to it
   when it is already full.
*/
int insert(int xid, Page* p, int valueIn){
  printf ("\nbegin insert\n");
  int DEBUG = 0;
  int DEBUGERROR = 1;
  int DEBUGERROR2 = 0;
  int DEBUGSTATEMENTS = 1;
  int DEBUGMALLOC = 1; // 1 makes it malloc the space

  //printf("\npage->id  = %d\n", p->id);

  // make a copy of the rid - so we don't effect the caller's copy
  recordid rid = { p->id, 0, sizeof(int)};


  // if DEBUGERROR ==1 this causes a seg fault below!
  if (DEBUGERROR) {printf("\n***page->id  = %lld\n", p->id);}
  printf("\n***rid.page  = %lld\n\n", (long long)rid.page);


  if(DEBUG) {printf("\nrid.page  = %lld\n", (long long)rid.page);}
  if(DEBUG) {printf("\nrid.slot  = %lld\n", (long long)rid.slot);}


  // Figure out how many entries are in the node
  rid.slot = 0; // need to get the variable in slot 0

  byte * countBuff;/// AHHH - i wasn't mallocing space for this!

  if(DEBUGMALLOC ) {countBuff = (byte *) malloc (sizeof(int));}

  if (DEBUGSTATEMENTS) {printf("\nDebug1\n");}

  stasis_record_read(xid, p, rid, countBuff); // read the value of count from slot 0

  if (DEBUGSTATEMENTS) {printf("\nDebug2\n");}

  int * countInt = (int *) countBuff; // cast it to an int *

  // set rid.slot to be the max slot entry used.
  rid.slot = *countInt;

  printf("\nrid2slot  = %d\n", rid.slot);

  // *recordcount_ptr(p) = last accessible index on the page.
  int max_index = stasis_fixed_records_per_page(rid.size); // rcs made this change.
  //  int max_index = *recordcount_ptr(p);     // recordcount_ptr is the number of slots currently allocated on the page.
                                               // but this code seems to do it's own allocation(?)


  //THESE LINES WERE CAUSING A SEGFAULT! *******************************************
  if (DEBUGERROR2) {printf("\nx  = %d\n", max_index);} // This causes a segfault after Debug2

  // HACK! TO FIX THE ABOVE PROBLEM!
  //  max_index = 1021;

  // assert that max_index is greater than our record of how many
  // entries we currently have on the page.
  assert(max_index>rid.slot);


  // check to see if we have room to add the entry.
  if (rid.slot == max_index){
    // we can't add any more entries to this node

    return -1;
  }



  // the default location to put the new value is location 1.
  int insertLocation = 1;

  // Iterating DOWN through the slots. Stop when we reach 0.
  // Will also break out of the while loop if we've found where
  // to insert the new value.
  while ( (rid.slot >= 1) && (insertLocation == 1)){

    // TODO: JuSt haven't filled in the code here yet...
    insertLocation =2;
  }

  // convert the input valueIn into a byte array
  //  byte * valueInBuff = (byte *) & valueIn;

  // get the rid ready to write to the insertLocation (determined above)
  rid.slot = insertLocation;

  // fixedWrite(p, rid, valueInBuff); // page/fixed.c:58: checkRid: Assertion `page->id == rid.page' failed.
  printf("\n***page->id  = %lld\n", p->id);
  printf("\n***rid.page  = %lld\n", (long long)rid.page);


  return 0;

}

/* This takes a page that is already initialized and a
   corresponding rid and initalizes the count value for
   it to be a BTreeNode. Just puts the value 0 in the
   first index of the page.
*/
void initializeNewBTreeNode(int xid, Page* p){

  // need access to the first slot
  recordid rid = { p->id, 0, sizeof(int)};

  // prepare the value to go into the first slot
  int countInt = 0;
  byte * countBuff = (byte *) & countInt;

  // write the count out
  stasis_record_write(xid, p, rid, countBuff);
  stasis_page_lsn_write(xid, p, 1);
}
void testFunctions(){
  printf("testing functions");

  // getting things ready
  int xid = Tbegin();
  pageid_t pageid1 = TfixedPageAlloc(xid, sizeof(int)); // this does the initialize
  Page *  p1 = loadPage(xid, pageid1);

  // calling functions
  writelock(p1->rwlatch,0);
  initializeNewBTreeNode(xid, p1);
  insert(xid, p1, 3);
  unlock(p1->rwlatch);

  // cleaning up

  releasePage(p1);
  Tcommit(xid);
}


int SimpleExample(){

  int DEBUGP = 0;
  //  int DEBUGT = 0;
  //  int DEBUGA = 0;
  int xid = Tbegin();

  /* Where to find stuff
   * ****************************
   *  TpageAlloc ->         stasis/src/stasis/operations/pageOperations.c
   *  TfixedPageAlloc ->    stasis/src/stasis/operations/pageOperations.c
   *  fixedPageInitailze -> stasis/src/stasis/page/fixed.c
   */



  pageid_t pageid1 = TfixedPageAlloc(xid, sizeof(int)); // this does the initialize

  Page *  p1 = loadPage(xid, pageid1);
  writelock(p1->rwlatch, 0);
  if(DEBUGP) {  printf("\n**** page->id  = %lld\n", p1->id);}

  /* check consistency between rid & page's values
   * for number of slots and record size */
  assert (p1->id == pageid1);


  /* check to make sure page is recorded as a FIXED_PAGE */
  assert( p1->pageType == FIXED_PAGE);

  if (DEBUGP) { printf("\n%lld\n", (long long)pageid1); }
  byte * b1 = (byte *) malloc (sizeof (int));
  byte * b2 = (byte *) malloc (sizeof (int));
  byte * b3 = (byte *) malloc (sizeof (int));
  //  int x = *recordcount_ptr(p1);
  int x = 42; //  rcs - recordcount_ptr is no longer exposed here...
  int y = 0; //rid1.slot;
  int z = 256;

  b1 = (byte *) & x;
  b2 = (byte *) & y;
  b3 = (byte *) & z;

  int * x1 = (int *) b1;


  if (DEBUGP) { printf("\nx  = %d\n", x);}
  if (DEBUGP) { printf("\nb1 = %d\n", *b1);}
  if (DEBUGP) { printf("\nx1  = %d\n", *x1);}
  if (DEBUGP) { printf("\ny  = %d\n", y);}
  if (DEBUGP) { printf("\nb2  = %d\n", *b2);}
  if (DEBUGP) { printf("\nz = %d\n", z);}
  if (DEBUGP) { printf("\nb3 = %d\n", *b3);}

  recordid rid1 = { pageid1, 0,sizeof(int)};

  // @todo This is a messy way to do this...

  stasis_record_write(xid, p1, rid1, b1);
  stasis_page_lsn_write(xid, p1, 1);
  stasis_record_read(xid, p1, rid1, b2);
  if (DEBUGP) { printf("\nb2** = %d\n",*((int *) b2));}

  //  initializeNewBTreeNode(p1, rid1);

  unlock(p1->rwlatch);

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

   //  tcase_add_test(tc, simpleLinearHashTest); // put back in if playing with hashtable


  /* --------------------------------------------- */

   tcase_add_checked_fixture(tc, setup, teardown);// leave stuff below here.

  suite_add_tcase(s, tc);
  return s;
}

#include "../check_setup.h"
