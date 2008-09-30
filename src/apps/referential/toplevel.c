#define _GNU_SOURCE

#include <sys/types.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/socket.h>
#include <netdb.h>

#include <stasis/transactional.h>

#include "algebra.h"
#include "tuple.h"
#include "ddl.h"
#include "dml.h"


#include "lang/ast.h"
//XXX#include "lang/context.h"

#define MAX_CONN_QUEUE 10
#define THREAD_POOL 20
#define SHUTDOWN_SERVER 1

typedef struct thread_arg {
  int id;
  recordid hash;
} thread_arg;

pthread_mutex_t interpreter_mut = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t  interpreter_cond = PTHREAD_COND_INITIALIZER;
pthread_cond_t  nextSocket_cond = PTHREAD_COND_INITIALIZER;
FILE * nextSocket = 0;

int shuttingdown = 0;
#define INTERPRETER_BUSY 1
#define INTERPRETER_IDLE 0

char interpreterStates[THREAD_POOL];
pthread_t interpreters[THREAD_POOL];
FILE * interpreterConnections[THREAD_POOL];

ReferentialAlgebra_context_t * context;

int openInterpreter(FILE * in, FILE * out, recordid hash);

void * interpreterThread(void * arg) {
  thread_arg * a = arg;
  int i = a->id;
  //  printf("I am thread %d\n",i);
  pthread_mutex_lock(&interpreter_mut);
  while(!shuttingdown) {
    while(!nextSocket) {
      if(shuttingdown) { break; }
      pthread_cond_wait(&interpreter_cond, &interpreter_mut);
    }
    if(shuttingdown) { break; }
    interpreterStates[i] = INTERPRETER_BUSY;
    interpreterConnections[i] = nextSocket;
    nextSocket = 0;
    pthread_cond_signal(&nextSocket_cond);
    pthread_mutex_unlock(&interpreter_mut);
    //    printf("Opening connection for FILE %x\n",interpreterConnections[i]);
    int ret =  openInterpreter(interpreterConnections[i],
		   interpreterConnections[i],a->hash);
    pthread_mutex_lock(&interpreter_mut);
    if(ret == SHUTDOWN_SERVER) {
      shuttingdown = 1;
    }
    interpreterStates[i] = INTERPRETER_IDLE;
    fclose(interpreterConnections[i]);
    interpreterConnections[i] = 0;
  }
  pthread_mutex_unlock(&interpreter_mut);
  free(a);
  return 0;
}

int openInterpreter(FILE * in, FILE * out, recordid hash) {
  char * line = NULL;
  size_t len = 0;
  int AUTOCOMMIT = 1;
  int debug = 0;
  int xid = -1;
  size_t read;

  fprintf(out, "> ");
  int ret = 0;
  if(!AUTOCOMMIT) { xid = Tbegin(); }
  while((read = getline(&line, &len, in)) != -1) {
    if(line[0] == '!') {
      if(!strncmp(line+1,"debug",strlen("debug"))) {
	debug = !debug;
	if(debug)
	  fprintf(out, "Enabling debugging\n");
	else
	  fprintf(out, "Disabling debugging\n");
      } else if(!strncmp(line+1,"regions",strlen("regions"))) {
	fprintf(out, "Boundary tag pages:\n");
	pageid_t pid = REGION_FIRST_TAG;
	boundary_tag tag;
	TregionReadBoundaryTag(-1,pid,&tag);
	int done = 0;
	while(!done) {
	  fprintf(out, "\tpageid=%lld\ttype=%d\tsize=%d\n", pid, tag.allocation_manager, tag.size);
	  if(tag.size == UINT32_MAX) { fprintf(out, "\t[EOF]\n"); }
	  int err = TregionNextBoundaryTag(-1,&pid,&tag,0);
	  if(!err) { done = 1; }
	}
      } else if(!strncmp(line+1,"autocommit",strlen("autocommit"))) {
	if(AUTOCOMMIT) {
	  // we're not in a transaction
	  fprintf(out, "Disabling autocommit\n");
	  AUTOCOMMIT = 0;
	  xid = Tbegin();
	} else {
	  fprintf(out, "Enabling autocommit\n");
	  AUTOCOMMIT = 1;
	  Tcommit(xid);
	}
   /* } else if(!strncmp(line+1,"parseTuple",strlen("parseToken"))) {
	char * c = line + 1 + strlen("parseToken");
	char ** toks = parseTuple(&c);
	for(int i = 0; toks[i]; i++) {
	  fprintf(out, "col %d = ->%s<-\n", i, toks[i]);
	}
	fprintf(out, "trailing stuff: %s", c);
      } else if(!strncmp(line+1,"parseExpression",
			 strlen("parseExpression"))) {
	char * c = line + 1 + strlen("parseExpression");
	lladdIterator_t * it = parseExpression(xid, hash, &c);
	it = 0; */
      } else if(!strncmp(line+1,"exit",strlen("exit"))) {
	break;
      } else if(!strncmp(line+1,"shutdown",strlen("shutdown"))) {
	ret = SHUTDOWN_SERVER;
	break;
      }
    } else {
      expr_list * results = 0;
      parse(line, &results);
      for(int i = 0; results && i < results->count; i++) {
	expr * e = results->ents[i];
	switch(e->typ) {
	case query_typ: {
	  lladdIterator_t * it = ReferentialAlgebra_ExecuteQuery(xid, context, e->u.q);
	  if(it) {
	    while(Titerator_next(xid,it)) {
	      byte * tup;
	      Titerator_value(xid,it, &tup);
	      char * tupleString = stringTuple(*(tuple_t*)tup);
	      fprintf(out, "%s\n", tupleString);
	      free(tupleString);
	    }
	    Titerator_close(xid,it);
	  }
	} break;
	case insert_typ: {
	  if(AUTOCOMMIT) { xid = Tbegin(); }
	  ReferentialDML_ExecuteInsert(xid, context, e->u.i);
	  if(AUTOCOMMIT) { Tcommit(xid); xid = -1;}
	} break;
	case delete_typ: {
	  if(AUTOCOMMIT) { xid = Tbegin(); }
	  ReferentialDML_ExecuteDelete(xid, context, e->u.d);
	  if(AUTOCOMMIT) { Tcommit(xid); xid = -1;}
	} break;
	case create_typ: {
	  if(AUTOCOMMIT) { xid = Tbegin(); }
	  ReferentialDDL_CreateTable(xid, context, e->u.c);
	  if(AUTOCOMMIT) { xid = Tcommit(xid); xid = -1;}
	} break;
	default:
	  abort();
	}
      }

      //XXX typecheck(context, results);
      if(results) {
	char * str = pp_expr_list(results);
	printf("%s", str);
	free(str);
      } else {
	printf("No results\n");
      }
    }
    fprintf(out, "> ");
  }
  if(!AUTOCOMMIT) { Tcommit(xid); }

  free(line);
  fprintf(out, "\n");
  return ret;
}

void startServer(char * addr, recordid hash) {
  struct addrinfo hints;
  struct addrinfo *result, *rp;
  int sfd, s;
  struct sockaddr peer_addr;
  socklen_t peer_addr_len;
  //  ssize_t nread;
  //char buf[BUF_SIZE];

  memset(&hints, 0, sizeof(struct addrinfo));
  hints.ai_family = AF_INET; //AF_UNSPEC;    /* Allow IPv4 or IPv6 */
  hints.ai_socktype = SOCK_STREAM; /* Datagram socket */
  hints.ai_flags = AI_PASSIVE;    /* For wildcard IP address */
  hints.ai_protocol = 0;          /* Any protocol */

    printf("Listening on socket\n");

  s = getaddrinfo("127.0.0.1", addr, &hints, &result);
  if (s != 0) {
    fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(s));
    exit(EXIT_FAILURE);
  }

  /* getaddrinfo() returns a list of address structures.
     Try each address until we successfully bind().
     If socket(2) (or bind(2)) fails, we (close the socket
     and) try the next address. */

  for (rp = result; rp != NULL; rp = rp->ai_next) {
    sfd = socket(rp->ai_family, rp->ai_socktype,
		 rp->ai_protocol);
    if (sfd == -1)
      continue;

    if (bind(sfd, rp->ai_addr, rp->ai_addrlen) == 0)
      break;                  /* Success */

    close(sfd);
  }

  if (rp == NULL) {               /* No address succeeded */
    perror("Could not bind\n");
    exit(EXIT_FAILURE);
  }

  freeaddrinfo(result);           /* No longer needed */

  int err = listen(sfd, MAX_CONN_QUEUE);
  if(err == -1) {
    perror("Couldn't listen()");
    return;
  }

  printf("Spawning servers.\n"); fflush(NULL);

  for(int i = 0; i < THREAD_POOL; i++) {
    thread_arg * arg = malloc(sizeof(thread_arg));
    arg->id = i;
    arg->hash = hash;
    interpreterStates[i] = INTERPRETER_IDLE;
    interpreterConnections[i] = 0;
    pthread_create(&interpreters[i], 0, interpreterThread, arg);
  }

  printf("Ready for connections.\n"); fflush(NULL);

  /* Read datagrams and echo them back to sender */

  for (;;) {
    int fd = accept(sfd, &peer_addr, &peer_addr_len);
    if(fd == -1) {
      perror("Error accepting connection");
    } else {
      FILE * sock = fdopen(fd, "w+");

      pthread_mutex_lock(&interpreter_mut);

      //      int ret = openInterpreter(sock, sock, hash);
      //      fclose(sock);
      //      if(ret) {
      while(nextSocket) {
	pthread_cond_wait(&nextSocket_cond, &interpreter_mut);
      }

      nextSocket = sock;
      pthread_cond_signal(&interpreter_cond);

      pthread_mutex_unlock(&interpreter_mut);
      if(shuttingdown) {
	break;
      }

    }
  }

  pthread_cond_broadcast(&interpreter_cond);
  for(int i = 0; i < THREAD_POOL; i++) {
    pthread_join(interpreters[i],0);
  }
  close(sfd);

}


int main(int argc, char * argv[]) {

  Tinit();
  ReferentialAlgebra_init();

  recordid rootEntry;
  recordid hash;
  int xid = Tbegin();
  if(TrecordType(xid, ROOT_RECORD) == INVALID_SLOT) {
    printf("Creating new store\n");

    rootEntry = Talloc(xid, sizeof(recordid));
    assert(rootEntry.page == ROOT_RECORD.page);
    assert(rootEntry.slot == ROOT_RECORD.slot);

    hash = ReferentialAlgebra_allocContext(xid);

    Tset(xid, rootEntry, &hash);

  } else {
    printf("Opened existing store\n");
    rootEntry.page = ROOT_RECORD.page;
    rootEntry.slot = ROOT_RECORD.slot;
    rootEntry.size = sizeof(recordid);

    Tread(xid, rootEntry, &hash);

  }

  context = ReferentialAlgebra_openContext(xid,hash);

  Tcommit(xid);

  FILE * in;
  if(argc == 3) { // listen on socket
    if(strcmp("--socket", argv[1])) {
      printf("usage:\n\n%s\n%s <filename>\n%s --socket addr:port\n",
	     argv[0],argv[0],argv[0]);
    } else {
      startServer(argv[2], hash);
    }

    printf("Shutting down...\n");
  } else {
    if(argc == 2) {
      in = fopen(argv[1], "r");
      if(!in) {
	printf("Couldn't open input file.\n");
	return 1;
      }
    } else {
      in = stdin;
    }
    openInterpreter(in, stdout, hash);
  }
  Tdeinit();
}
