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
#include "../../src/apps/cht/cht.h"
#include <assert.h>

/** Thanks, jbhtsimple.c!! */

#define C    "140.254.26.214:10010"
#define CLIENT "140.254.26.223:12345"
#define S0   "140.254.26.249:10010"
#define S1   "140.254.26.250:10010"
#define S2   "140.254.26.247:10010"
#define S3   "140.254.26.226:10010"
/*
#define C    "127.0.0.1:10010"
#define CLIENT "127.0.0.1:12345"
#define S0   "127.0.0.1:10011"
#define S1   "127.0.0.1:10012"
#define S2   "127.0.0.1:10013"
#define S3   "127.0.0.1:10014"
*/

char** broadcast_lists[100];
char* star_nodes[] = { C };
char* point_nodes[] = { S0, S1, S2, S3, };
char* ointp_nodes[] = { S2, S0, S1, S1, };
char* intpo_nodes[] = { S1, S2, S0, S0, };
char* ntpoi_nodes[] = { S3, S0, S1, S2, };
char* tpoin_nodes[] = { S0, S1, S2, S3, };


int broadcast_list_host_count [] = { 1, 3, 1, 1, 1, 1};

int broadcast_lists_count = 6;


int main (int argc, char**argv) {
  
  DfaSet * dfaSet; 

  int server_type;
  short port;

  char * localhost;

  assert(argc == 8);

  Tinit();


  broadcast_lists[0] = star_nodes;
  broadcast_lists[1] = point_nodes;
  broadcast_lists[2] = point_nodes;
  broadcast_lists[3] = ointp_nodes;
  broadcast_lists[4] = intpo_nodes;
  broadcast_lists[5] = ntpoi_nodes;
  broadcast_lists[6] = tpoin_nodes;

  broadcast_lists_count = atoi(argv[3]) + 2;

  broadcast_list_host_count[2] = atoi(argv[4]);
  broadcast_list_host_count[3] = atoi(argv[5]);
  broadcast_list_host_count[4] = atoi(argv[6]);
  broadcast_list_host_count[5] = atoi(argv[7]);


  if(argv[1][0] == 'c') {
    server_type = CHT_COORDINATOR;
  } else {
    server_type = CHT_SERVER;
  }

  if(server_type == CHT_COORDINATOR) {
    localhost = broadcast_lists[0][0];

  } else {
    localhost = broadcast_lists[atoi(argv[1])][atoi(argv[2])];
  }
  port = parse_port(localhost);
  Tinit();
  dfaSet = cHtInit(server_type, localhost,  NULL, port, broadcast_lists, broadcast_lists_count, broadcast_list_host_count);

  main_loop(dfaSet);

  return -1;

}
