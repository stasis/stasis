#include <libdfa/libdfa.h>

#define CREATE 1
#define INSERT 2
#define LOOKUP 3
#define REMOVE 4
#define DELETE 5
/** Unimplemented: Evaluate a function call from a table provided by the library user. */
#define TSTSET 6
#define GETXID  7
/* #define COMMIT 8
   #define ABORT  9 */

typedef struct {
  unsigned short key_length;
  unsigned short value_length;
  unsigned char  request_type;
 // unsigned char  response_type;
  int hashTable;
} payload_header;

#define __header_ptr(m) ((payload_header*)(&((m)->payload)))

#define __key_length(m) (&(__header_ptr(m)->key_length))
#define __value_length(m) (&(__header_ptr(m)->value_length))

#define getKeyLength(m)    (ntohs(*__key_length(m)))
#define setKeyLength(m, x) (*__key_length(m)=htons(x))

#define getValLength(m)    (ntohs(*__value_length(m)))
#define setValLength(m, x) (*__value_length(m)=htons(x))

#define requestType(m) (&(__header_ptr(m)->request_type))
#define responseType(m) (&(__header_ptr(m)->response_type))

void * getKeyAddr(Message *m);
void * getValAddr(Message * m);
int checkPayload(Message * m);


/*
static unsigned short* _key_length(Message * m) {
  return &(__header_ptr(m)->key_length);
}

static unsigned short* _value_length(Message *m) {
  return &(__header_ptr(m)->value_length);
}
static unsigned char * requestType(Message *m) {
  return &(__header_ptr(m)->request_type);
}

static unsigned char * responseType(Message *m) {
  return &(__header_ptr(m)->response_type);
}*/
