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
#include "../../libdfa/messages.h"
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

int main (int argc, char ** argv) {
  short portnum;
  /*  short target_portnum;
      char * target_ip; */
  NetworkSetup ns;
  Message m;
  /*  struct sockaddr_in to, from;  */
  char * to;
  char from[MAX_ADDRESS_LENGTH+1];
  
  printf("This test code is out of date.  Exiting.\n");

  if(argc != 3) {
    printf("Usage: %s port_num ip_address\n", argv[0]);
    return -1;
	  
  }

  return 0;

  portnum = (unsigned short) atoi(argv[1]);
  to = argv[2];
  /*  target_ip = argv[2];
      target_portnum = (unsigned short) atoi(argv[3]); */

  init_network(&ns, portnum);

  /*  to.sin_family = AF_INET;
  err=inet_aton(target_ip, &(to.sin_addr));
  if(err == 0) {
    perror("inet_aton");
  }
  to.sin_port = htons(target_portnum);
  */
  send_message(&ns, &m, to);

  printf("Ping");
  fflush(NULL);

  receive_message(&ns, &m, from);


  printf("Hello %s\n", from);
  return 0;
}
