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
/*#include <assert.h> */

//#include <lladd/transactional.h>
//#include "../../src/lladd/logger/logWriter.h"
#include "../check_includes.h"
#include <assert.h>
#include <libdfa/networksetup.h>
#include <libdfa/messages.h>
#include <string.h>
#define LOG_NAME   "check_networksetup.log"

/** 
    @test
*/
START_TEST (networksetup_check) {
  NetworkSetup * ns = readNetworkConfig("../../libdfa/networksetup.sample", COORDINATOR);
  assert(ns);
  
  assert(ns->localport == 20000);
  assert(!strcmp(ns->localhost, "127.0.0.1"));
  assert(ns->broadcast_lists);
  assert(ns->broadcast_lists_count == 2);
  int i;
  for(i = 0; i < ns->broadcast_lists_count; i++) {
    assert(ns->broadcast_list_host_count[i] == 2);
    int j;
    for(j = 0; j < ns->broadcast_list_host_count[i]; j++) {
	assert(!strcmp("127.0.0.1", parse_addr(ns->broadcast_lists[i][j])));
	assert(20000 + i + 2*j + 1 == parse_port(ns->broadcast_lists[i][j]));
    }
  }
  
  // check contents of broadcast_lists 
  
}
END_TEST
/** 
    @test
*/
START_TEST (networksetup_check_error) {
  assert(!readNetworkConfig("networksetup.bad", COORDINATOR));
  assert(!readNetworkConfig("networksetup.bad2", COORDINATOR));
  assert(!readNetworkConfig("networksetup.bad3", COORDINATOR));
  assert(!readNetworkConfig("networksetup.bad4", COORDINATOR));
}
END_TEST
/** 
  Add suite declarations here
*/
Suite * check_suite(void) {
  Suite *s = suite_create("networksetup_suite");
  /* Begin a new test */
  TCase *tc = tcase_create("networksetup");
  /* void * foobar; */  /* used to supress warnings. */
  /* Sub tests are added, one per line, here */
  
  tcase_add_test(tc, networksetup_check);
  tcase_add_test(tc, networksetup_check_error);
  /* --------------------------------------------- */
//  tcase_add_checked_fixture(tc, setup, teardown);
  suite_add_tcase(s, tc);

  return s;
}

#include "../check_setup.h"
