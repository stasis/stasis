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

#ifndef _MESSAGES_H
#define _MESSAGES_H

#include <limits.h>

typedef unsigned long state_machine_id;

/* Maximum size of a message payload, in bytes. Should be a multiple of the system word length.*/
#define MAX_PAYLOAD 800
/**
    Note that message_type UCHAR_MAX is reserved for the NULL (stop) state, and 0 is reserved for the 
    start state.
*/
#define MAX_MESSAGE_TYPE (UCHAR_MAX)
#define MAX_MESSAGE_COUNT (UCHAR_MAX+1)
typedef unsigned char message_name;

/**
"xxx.xxx.xxx.xxx:yyyyy" is 21 characters, and is as long as the
longest valid ip:port string we will ever encounter or produce
*/

#define MAX_ADDRESS_LENGTH 21


/**
   Message structs are the in-memory representation of network
   messages. Everything except for the payload is translated to and
   from network byte order automatically, and the whole struct is sent
   (as is) over the network.

   This is why the payload field can be of arbitrary length and is
   defined as a char.  Allocating a message with a payload larger than
   one byte can be done with sizeof() arithmetic, and by setting size
   accordingly.
*/
typedef struct message { 
  state_machine_id to_machine_id;
  state_machine_id from_machine_id;
  state_machine_id initiator_machine_id;
  char initiator[MAX_ADDRESS_LENGTH];
  message_name type;
  /** Payload is a byte array of arbitrary length. **/ 
  char payload[MAX_PAYLOAD];
} Message;

/**
   This struct contains the state for the messages (networking) layer.
   Currently, everything here can be derived at startup, so this won't
   need to be in transactional storage, with some luck. */
typedef struct networkSetup {
  unsigned short localport;
  char * localhost;
  int socket;
  /** 
      Same format as argv for main().  If a message is addressed to
      "broadcast", then the message will be sent to each
      "address:port" pair in this string.  If you want to use proper
      IP broadcast, then this list can simply contain one entry that
      contains a subnet broadcast address like "1.2.3.0:1234".

      It would be best to set this value to NULL and
      broadcast_list_count to zero if you don't plan to use broadcast.
  */
  char *** broadcast_lists;
  int broadcast_lists_count;
  int *broadcast_list_host_count;
} NetworkSetup;

int init_network(NetworkSetup *ns, unsigned short portnum);

int init_network_broadcast(NetworkSetup * ns, unsigned short portnum, char * localhost, char *** broadcast_lists, 
			   int broadcast_lists_count, int * broadcast_list_host_count);
  /*int init_network_broadcast(NetworkSetup * ns, unsigned short portnum, char *** broadcast_lists, 
    int broadcast_lists_count, int * broadcast_list_host_count); */

int send_message(const NetworkSetup *ns, Message *m, const char *to);
int receive_message(NetworkSetup *ns, Message *m, char *from);

/**
   Remember to call free() on the pointer that this returns!
*/
char * parse_addr(const char *message);
short  parse_port(const char *message);
int get_index(char ** table, int table_length, const char *message);


#endif
