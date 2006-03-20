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
/*#include <sys/types.h> */

#define _GNU_SOURCE

#include <lladd/common.h>
#include <sys/socket.h>
#include <sys/poll.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include <libdfa/messages.h>

char * parse_addr(const char * address) {
  char * strtok_buf = NULL;
  char * addr_copy = strdup(address);
  char * addr_s;

  if(addr_copy == NULL) {
    perror("Out of memory in send_message");
  }

  addr_s = strtok_r(addr_copy, ":", &strtok_buf);

  if(addr_s == NULL) {
    fprintf(stderr, "Invalid address (%s) passed into parse_addr", address);
    return NULL;
  }
  
  /* The caller needs to call free on the pointer we return.  That's the same pointer as addr_copy. */

  /*   addr_copy = strdup(addr_copy); */
  /*   free(addr_copy);               */

  assert(addr_copy == addr_s);

  return addr_s;
}

short parse_port(const char * address) {
  char * strtok_buf = NULL;
  char * addr_copy = strdup(address);
  short port;
  char * port_s;

  if(addr_copy == NULL) {
    perror("Out of memory in send_message");
  }


  /* This sets port_s to addr.  The next copy returns the port */
  port_s = strtok_r(addr_copy, ":", &strtok_buf);

  if(port_s == NULL) {
//    printf("Invalid address (%s) passed into parse_port", address);
//   assert(0);
    return 0;
  }

  port_s = strtok_r(NULL, ":", &strtok_buf);

  if(port_s == NULL) {
//    printf("Invalid address (%s) passed into parse_port", address);
//    assert(0);
    return 0;
  }
  
  port = atoi(port_s);
  free(addr_copy);

  return port;
}

/* code to establish a socket; originally from bzs@bu-cs.bu.edu */ 

int establish(unsigned short portnum) { 
  /* char myname[HOST_NAME_MAX+1];  */
  int s;

  struct sockaddr_in sa; 
  sa.sin_family = AF_INET;
  sa.sin_addr.s_addr = INADDR_ANY; 

  sa.sin_port= htons(portnum);                  /* this is our port number */ 

  if ((s= socket(AF_INET, SOCK_DGRAM, 0)) < 0) { /* create socket */ 
    perror("establish:socket()");
    return(-1); 
  }

  /* bind address to socket */ 

  if (bind(s,(struct sockaddr *)&sa,sizeof(struct sockaddr_in)) < 0) { 
    perror("establish:bind()");
    close(s); 
    return(-1); 
  } 

  listen(s, 5); /* max # of queued connects */ 
  return(s); 
}


int init_network(NetworkSetup * ns, unsigned short portnum) {
  return init_network_broadcast(ns, portnum, NULL, NULL, 0, NULL);
}
int init_network_broadcast(NetworkSetup * ns, unsigned short portnum, char * localhost, char *** broadcast_lists, 
			   int broadcast_lists_count, int * broadcast_list_host_count) {

  ns->localport = portnum;
  ns->localhost = localhost;
  ns->broadcast_lists = broadcast_lists;
  ns->broadcast_lists_count = broadcast_lists_count;
  ns->broadcast_list_host_count = broadcast_list_host_count;

  return (ns->socket = establish(portnum));

}
int __send_message(const NetworkSetup *ns, Message *message, const char *to) ;

int send_message(const NetworkSetup *ns, Message *message, const char *to) {

  return __send_message(ns, message, to);
}
int _send_message(const NetworkSetup *ns, Message *message, const char *to);


int __send_message(const NetworkSetup *ns, Message *message, const char *to) {



  if(strncmp(to, "bc:", 3)==0) {

    int i;
    int list_number = parse_port(to);
    if(list_number == ALL_BUT_GROUP_ZERO) {
  //    fprintf(stderr, "Broadcasting to all groups (except group 0).\n");
      for(int i = 1; i < ns->broadcast_lists_count+1; i++) {
	char * new_to;
	asprintf(&new_to, "bc:%d", i);
	int ret = __send_message(ns, message, new_to);
        free(new_to);	
	if(ret < 0) {
	  return ret;
	}
      }
      return 0;
    } else if(list_number < 0 || list_number >= ns->broadcast_lists_count+1) {
      fprintf(stderr, "Invalid list number %d passed into send_message: %s\n", list_number, to);
      return -1;
    }
    if(list_number == 0) {
      // send to coordinator 
 //     fprintf(stderr, "Sending message to coordinator: %s", ns->coordinator);
      return __send_message(ns, message, ns->coordinator);
    }
    if(ns->broadcast_list_host_count[list_number-1] == 0) {
      fprintf(stderr, "Sending to empty broadcast list! Address was %s\n", to);
    }
    
    for(i =0; i < ns->broadcast_list_host_count[list_number-1]; i++) {
 //     fprintf(stderr, "sending to member %d of list %d\n", i, list_number);
      int ret;
      if((ret = __send_message(ns, message, ns->broadcast_lists[list_number-1][i])) < 0) {
	return ret;
      }
    }
  } else {
    DEBUG("Sending %ld-%d: to %s:%ld\n", message->from_machine_id, message->type ,to, message->to_machine_id);

    return _send_message(ns, message, to);
  }
  return 0;
}

int _send_message(const NetworkSetup *ns, Message *message, const char *to) {
  int ret;
  char *addr;
  short port;
  int err;

  /* TODO: Right size? */
  struct sockaddr_in * to_sa = malloc(sizeof(struct sockaddr_in));


  to_sa->sin_family = AF_INET;

  addr = parse_addr(to);
  port = parse_port(to);

  if(addr == NULL) {
    fprintf(stderr, "Send failed.  Could not parse addr.\n");
    free(to_sa);
    return -1;
  }

  if(port == -1) {
    fprintf(stderr, "Send failed.  Could not parse port.\n");
    free(to_sa);
    free(addr);
    return -1;
  }
  to_sa->sin_port = htons(port);
  err=inet_aton(addr, &(to_sa->sin_addr));

  if(err == 0) {
    perror("inet_aton");
    free(to_sa);
    free(addr);
    return -1;
  }

  free(addr);
  message->to_machine_id = htonl(message->to_machine_id);
  message->from_machine_id = htonl(message->from_machine_id);

  ret = sendto(ns->socket, message, sizeof(Message), 0, (struct sockaddr*)to_sa, sizeof(struct sockaddr_in));

  message->to_machine_id = ntohl(message->to_machine_id);
  message->from_machine_id = ntohl(message->from_machine_id);

  if(ret < 0) {
    perror("send_message");
  }
  free(to_sa);
  if(ret != sizeof(Message)) {
    fprintf(stderr, "send_message sent partial message!\n");
    
    return -1;
  }
  return ret;
}

/** Synchronously get a UDP packet.  Blocks.  @return 1 on success, 0
 *  on corrupted packet / timeout, < 0 on network erro.
 *
 *  @param from A char array of length MAX_ADDRESS_LENGTH+1 that will
 *              be clobbered and set to a null terminated string.
 */
/*int recieve_message(NetworkSetup *ns, Message *m, struct sockaddr_in *from, socklen_t fromlen) { */
int receive_message(NetworkSetup *ns, Message *message, char *from) {
  size_t max_len = sizeof(Message);
  /*  int flags = MSG_TRUNC ; */
  int flags = 0 ; 
  ssize_t message_size ;
  struct sockaddr_in from_sockaddr;
  socklen_t from_len = sizeof(struct sockaddr_in);
  char portstr[6];
  bzero(&from_sockaddr, sizeof(struct sockaddr_in));
  
  /*  printf("recv'ing\n"); 
      fflush(NULL); */
  /* FIXME! */
  /*  message_size = sizeof(Message);  */
  message_size = recvfrom(ns->socket, message, max_len, flags, 
			  (struct sockaddr*)&from_sockaddr, &from_len);
  if(message_size < 0) {
    perror("recvfrom");
    fflush(NULL);
    assert(0);
  }
  /*  printf("recv'ed\n");
      fflush(NULL); */
  
  /* TODO:  Check error fields from recvfrom */
  
  /* Fix the state machine number... */
  message->to_machine_id = ntohl(message->to_machine_id);
  message->from_machine_id = ntohl(message->from_machine_id);
    
  /* Fill out the address string. */

  strcpy(from, inet_ntoa((from_sockaddr).sin_addr));
  strcat(from, ":");
  sprintf(portstr, "%d", ntohs((from_sockaddr).sin_port));
  strcat(from, portstr);

  if(message_size != sizeof(Message)) { 
    /* drop packet */
    fprintf(stderr, "Size mismatch: %ld, %ld\n", message_size, sizeof(Message)); 
    return 0;
  } else {
    /* TODO: Callback to security stuff / crypto here? */
    return 1;
  }

}

int get_index(char ** table, int table_length, const char *address) {
  int i;

  for(i = 0; i < table_length; i++) {
    if(0 == strcmp(table[i], address)) {
      return i;
    }
  }
  return -1;
}
