typedef unsigned char byte;
typedef struct rwl_s rwl;
/* This is from page.h */
struct Page_s {
  int id;
  long LSN;
  byte *memAddr;
  byte dirty;
  struct Page_s *next;
  struct Page_s *prev; 
  int queue; 
  int inCache;
  rwl * rwlatch;
  rwl * loadlatch;

};

typedef struct Page_s Page;


Page * loadPage(int xid, long page);

void releasePage(Page * p);
