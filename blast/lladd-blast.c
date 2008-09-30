/** 
    @file Test cases for blast.
*/

#include "lladd-blast.h"

Page * ptr = 0;

Page * loadPage(int xid, long page) { 
  ptr ++;
  ptr->id = page;
  return ptr;
} 

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
/** @test see if it can handle multiple pages... It doesn't do arrays, unfortunately. :( */
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
