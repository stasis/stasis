/** 
    @file Test cases for blast.
*/

#include "lladd-delta.h"
#include <assert.h>

void __initialize__();

Page * ptr = 0;

Page * loadPage(int xid, int page) { 
  /* Trick it into assuming the user is not a total idiot. */
  if(page == -1) { 
    page = 1;
  }

  ptr ++;
  ptr->id = page;
  return ptr;
} 

void releasePage(Page * p) { 
  p->id = -1;
}

void assertIsCorrectPage(int pid, int id) { 
 assert(pid == id);
}

/** To run tests:

   You'll also need to uncomment main() below.  Don't ask. ;)

   cpp lladd-blast.c > lladd-blast.i && cilly.asm.exe --out blast-test.c --dooptimizeLLADD lladd-blast.i
   runblast blast-test.c failCil        

   (where failCil is one of failCil, passCil, failAssert, passAssert)                         
*/
int passAssert(Page * p, int pageid) {
  __initialize__();
  pageid = 1;
  Page * q = loadPage(1,pageid);
  if(!p) {
     q->id = pageid;// = loadPage(1, pageid);
     q = loadPage(1, pageid);
  } else if(p->id == pageid) {
     q->id == p->id; //releasePage(q);
     q = loadPage(1, pageid);
  } else {
     q = p;
     q->id = pageid;
     q = loadPage(1, pageid);
  }
  assertIsCorrectPage(q->id, pageid);
  return 0;
}

int failAssert(Page * p, int pageid) {
  __initialize__();
  pageid = 1;
  Page * q = loadPage(1,pageid);
  if(!p) {
     q->id = pageid;// = loadPage(1, pageid);
     q = loadPage(1, pageid);
  } else if(p->id == pageid) {
     q->id == p->id; //releasePage(q);
     q = loadPage(1, pageid);
  } else {
     q = p;
     q->id = pageid;
     q = loadPage(1, pageid);
  }
  assertIsCorrectPage(p->id, pageid);
  return 0;
}


void passCil(Page * q, int pageid) { 
  Page * p = loadPage(1, pageid);
}

void failCil(Page * p, int pageid) {
  passCil_q(p, pageid);
}
/*
int main(int argc, char argv[][]) {
  pass1(0, 0);
  pass2(0, 0);
  pass3(0, 0);
  pass4(0, 0);
}

int pass1(int argc, char argv[][]) { 
  int xid = 2; 
  long page = 1;
  Page * p = 0;
  int i;

  for(i = 0; i < argc; i++) {
    p = loadPage(xid, page+i);
    releasePage(p);
  }
}
// @test see if it can handle multiple pages... It doesn't do arrays, unfortunately. :( 
int pass2(int argc, char argv[][]) {
  int xid = 2;
  long page = 1;
  int i;
  Page * p, * q, * r, * s, * t, * u, * v, * w;

  p = 0 ;
  p = loadPage(xid, 1);
  q = 0 ;
  q = loadPage(xid, 2);
  r = 0 ;
  r = loadPage(xid, 3);
  s = 0 ;
  s = loadPage(xid, 4);
  t = 0 ;
  t = loadPage(xid, 5);
  u = 0 ;
  u = loadPage(xid, 6);
  v = 0 ;
  v = loadPage(xid, 7);
  w = 0 ;
  w = loadPage(xid, 8);

}
int pass3(int argc, char argv[][]) {
  int xid = 2;
  Page * p, * q, * r, * s, * t, * u, * v, * w;

  p = 0 ;
  p = loadPage(xid, 1);
  releasePage(p);
  q = 0 ;
  r = 0 ;
  s = 0 ; 
  t = 0 ;
  u = 0 ;
  v = 0 ;
  w = 0 ;

  q = loadPage(xid, 2);
  r = loadPage(xid, 3);
  s = loadPage(xid, 4);
  releasePage(s);
  releasePage(q);
  t = loadPage(xid, 5);
  u = loadPage(xid, 6);
  releasePage(t);
  releasePage(r);
  v = loadPage(xid, 7);
  releasePage(u);
  w = loadPage(xid, 8);
  releasePage(w);
  releasePage(v);

}

int pass4(int argc, char argv[][]) { 
  int xid = 2; 
  Page * p = 0;

  p = loadPage(xid, 1);
  releasePage(p);
  p = loadPage(xid, 1); 
  releasePage(p);
  p = loadPage(xid, 1); 
  releasePage(p);
  p = loadPage(xid, 1); 
  releasePage(p);
  p = loadPage(xid, 1); 
  releasePage(p);
  p = loadPage(xid, 1); 
  releasePage(p);
  p = loadPage(xid, 1); 
  releasePage(p);
  p = loadPage(xid, 1); 
  releasePage(p);

}


int fail1(int argc, char argv[][]) { 
  int xid = 2; 
  Page * p = 0;

  releasePage(p);
  p = loadPage(xid, 1); 
  releasePage(p);
  p = loadPage(xid, 1); 
  releasePage(p);
  p = loadPage(xid, 1); 
  releasePage(p);
  p = loadPage(xid, 1); 
  releasePage(p);
  p = loadPage(xid, 1); 
  releasePage(p);
  p = loadPage(xid, 1); 
  releasePage(p);
  p = loadPage(xid, 1); 
  releasePage(p);
}

int fail2(int argc, char argv[][]) { 
  int xid = 2; 
  Page * p = 0, *q = 0;
  int i;
  p = loadPage(xid, 1); 
  for(i = 0; i < argc; i++) {

    q = loadPage(xid, 2);

    if(!(argc + i % 3426739)) { 
      continue;
    }

    releasePage(q);
  }
  releasePage(p);
}
*/
