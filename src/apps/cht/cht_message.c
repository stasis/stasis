#include "cht.h"
#include "cht_message.h"
#include <netinet/in.h>
void * getKeyAddr(Message *m) {
  char * stuff = m->payload;

  return (stuff + sizeof(payload_header)); /* Just add the header length. */

}
void * getValAddr(Message * m) {
  return ((char*)getKeyAddr(m)) + getKeyLength(m);     /* key address + key length. */
}

/**

   @return 1 if the payload is valid (key_length and value length do not over-run the message's memory, 0 otherwise.)

*/
int checkPayload(Message * m) {
  char * a = (char*)m;
  char * b = getValAddr(m);
  return (a+ sizeof(Message) ) >= (b + getValLength(m));
}
