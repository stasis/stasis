#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include "stasis/operations/lsmTable.h"

#include "stasis/transactional.h"

#include "stasis/page/compression/nop.h"
#include "stasis/page/compression/multicolumn-impl.h"
#include "stasis/page/compression/staticMulticolumn.h"
#include "stasis/page/compression/for-impl.h"
#include "stasis/page/compression/rle-impl.h"
#include "stasis/page/compression/staticTuple.h"
#include "stasis/page/compression/pageLayout.h"

namespace rose {
  template<class PAGELAYOUT> 
  void getTuple(long int i, typename PAGELAYOUT::FMT::TUP & t) {
    typename PAGELAYOUT::FMT::TUP::TYP0 m = i;
    typename PAGELAYOUT::FMT::TUP::TYP1 j = i / 65536;
    typename PAGELAYOUT::FMT::TUP::TYP2 k = i / 12514500;
    typename PAGELAYOUT::FMT::TUP::TYP3 l = i / 10000000;
    typename PAGELAYOUT::FMT::TUP::TYP4 n = i / 65536;
    typename PAGELAYOUT::FMT::TUP::TYP5 o = i / 12514500;
    typename PAGELAYOUT::FMT::TUP::TYP6 p = i / 10000000;
    typename PAGELAYOUT::FMT::TUP::TYP7 q = i / 65536;
    typename PAGELAYOUT::FMT::TUP::TYP8 r = i / 12514500;
    typename PAGELAYOUT::FMT::TUP::TYP9 s = i / 10000000;

    t.set0(&m);
    t.set1(&j);
    t.set2(&k);
    t.set3(&l);
    t.set4(&n);
    t.set5(&o);
    t.set6(&p);
    t.set7(&q);
    t.set8(&r);
    t.set9(&s);
  }

  template<class PAGELAYOUT>
  int main(int argc, char **argv) {
    unlink("storefile.txt");
    unlink("logfile.txt");

    sync();

    //    PAGELAYOUT::initPageLayout();

    bufferManagerNonBlockingSlowHandleType = IO_HANDLE_PFILE;

    Tinit();

    int xid = Tbegin();

    recordid lsmTable = TlsmTableAlloc<PAGELAYOUT>(xid);

    Tcommit(xid);

    lsmTableHandle<PAGELAYOUT>* h = TlsmTableStart<PAGELAYOUT>(lsmTable, XID_COL);

    typename PAGELAYOUT::FMT::TUP t;
    typename PAGELAYOUT::FMT::TUP s;

    long INSERTS = 0;
    int file_mode = 0;
    char * file = 0;
    if(argc == 2) {
      INSERTS = atoll(argv[1]);
    } else if (argc == 3) {
      file_mode = 1;
      assert(!strcmp("-f", argv[1]));
      file = argv[2];
    } else {
      INSERTS = 10 * 1000 * 1000;
    }

    //    int column[] = { 0 , 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    //               0   1  2  3   4  5  6  7  8   9 
//    int column[] = { 3 , 4, 1, 11, 0, 5, 6, 9, 10, 14 };

//    const int column[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    const int column[] = {2, 7, 5, 6, 0, 3, 10};

    static long COUNT = INSERTS / 100;
    long int count = COUNT;

    struct timeval start_tv, now_tv;
    double start, now, last_start;

    gettimeofday(&start_tv,0);
    start = rose::tv_to_double(start_tv);
    last_start = start;

    epoch_t this_xid = 0;
    epoch_t last_ts_col = 0;

    printf("tuple 'size'%d ; %ld\n", PAGELAYOUT::FMT::TUP::sizeofBytes(), sizeof(typename PAGELAYOUT::FMT::TUP));

    if(file_mode) {
      typename PAGELAYOUT::FMT::TUP scratch;

      int max_col_number = 0;
      for(int col =0; col < PAGELAYOUT::FMT::TUP::NN; col++) {
	max_col_number = max_col_number < column[col]
	  ? column[col] : max_col_number;
      }
      char ** toks = (char**)malloc(sizeof(char*)*(max_col_number+1));
      char * mode;
      printf("Reading from file %s\n", file);
      int inserts = 0;
      size_t line_len = 100;
      // getline wants malloced memmory (it probably calls realloc...)
      char * line = (char*) malloc(sizeof(char) * line_len);

      FILE * input = fopen(file, "r");
      if(!input) {
	perror("Couldn't open input");
	return 1;
      }
      size_t read_len;
      COUNT = 100000;
      count = 100000;

      struct timeval cannonical_start_tv;
      gettimeofday(&cannonical_start_tv,0);
      double cannonical_start = tv_to_double(cannonical_start_tv);

      while(-1 != (int)(read_len = getline(&line, &line_len, input))) {
	int line_tok_count;
	{
	  char * saveptr;
	  int i;
	  mode = strtok_r(line, ",\n", &saveptr);
	  toks[0] = strtok_r(0, ",\n", &saveptr);
	  for(i = 1; i < (max_col_number+1); i++) {
	    toks[i] = strtok_r(0, ",\n", &saveptr);
	    if(!toks[i]) {
	      break;
	    }
	  }
	  line_tok_count = i;
	}
	if(line_tok_count < (max_col_number+1)) {
	  //	  printf("!");
	  if(-1 == getline(&line,&line_len,input)) {
	    // hit eof.
	  } else {
	    printf("Not enough tokesn on line %d (found: %d expected: %d\n",
		   inserts+1, line_tok_count, max_col_number+1);
	    return 1;
	  }
	} else {
	  //	  printf(".");
	  inserts ++;

	  if(0 < PAGELAYOUT::FMT::TUP::NN) {
	    char * endptr;
	    errno = 0;
	    typename PAGELAYOUT::FMT::TUP::TYP0 t = strtoll(toks[column[0]], &endptr, 0);
	    if(strlen(toks[column[0]]) - (size_t)(endptr-toks[column[0]]) > 1) {
	      printf("couldnt parse token #%d: ->%s<-\n", column[0], toks[column[0]]);
	      return 1;
	    }
	    if(errno) {
	      printf("Couldn't parse token #%d: ->%s<-", column[0],toks[column[0]]);
	      perror("strtoll error is");
	      return 1;
	    }
	    scratch.set0(&t);
	  }
	  if(1 < PAGELAYOUT::FMT::TUP::NN) {
	    char * endptr;
	    errno = 0;
	    typename PAGELAYOUT::FMT::TUP::TYP1 t = strtoll(toks[column[1]], &endptr, 0);
	    if(strlen(toks[column[1]]) - (size_t)(endptr-toks[column[1]]) > 1) {
	      printf("couldnt parse token #%d: ->%s<-\n", column[1], toks[column[1]]);
	      return 1;
	    }
	    if(errno) {
	      printf("Couldn't parse token #%d: ->%s<-", column[1],toks[column[1]]);
	      perror("strtoll error is");
	      return 1;
	    }
	    scratch.set1(&t);
	  }
	  if(2 < PAGELAYOUT::FMT::TUP::NN) {
	    char * endptr;
	    errno = 0;
	    typename PAGELAYOUT::FMT::TUP::TYP2 t = strtoll(toks[column[2]], &endptr, 0);
	    if(strlen(toks[column[2]]) - (size_t)(endptr-toks[column[2]]) > 1) {
	      printf("couldnt parse token #%d: ->%s<-\n", column[2], toks[column[2]]);
	      return 1;
	    }
	    if(errno) {
	      printf("Couldn't parse token #%d: ->%s<-", column[2],toks[column[2]]);
	      perror("strtoll error is");
	      return 1;
	    }
	    scratch.set2(&t);
	  }
	  if(3 < PAGELAYOUT::FMT::TUP::NN) {
	    char * endptr;
	    errno = 0;
	    typename PAGELAYOUT::FMT::TUP::TYP3 t = strtoll(toks[column[3]], &endptr, 0);
	    if(strlen(toks[column[3]]) - (size_t)(endptr-toks[column[3]]) > 1) {
	      printf("couldnt parse token #%d: ->%s<-\n", column[3], toks[column[3]]);
	      return 1;
	    }
	    if(errno) {
	      printf("Couldn't parse token #%d: ->%s<-", column[3],toks[column[3]]);
	      perror("strtoll error is");
	      return 1;
	    }
	    scratch.set3(&t);
	  }
	  if(4 < PAGELAYOUT::FMT::TUP::NN) {
	    char * endptr;
	    errno = 0;
	    typename PAGELAYOUT::FMT::TUP::TYP4 t = strtoll(toks[column[4]], &endptr, 0);
	    if(strlen(toks[column[4]]) - (size_t)(endptr-toks[column[4]]) > 1) {
	      printf("couldnt parse token #%d: ->%s<-\n", column[4], toks[column[4]]);
	      return 1;
	    }
	    if(errno) {
	      printf("Couldn't parse token #%d: ->%s<-", column[4],toks[column[4]]);
	      perror("strtoll error is");
	      return 1;
	    }
	    scratch.set4(&t);
	  }
	  if(5 < PAGELAYOUT::FMT::TUP::NN) {
	    char * endptr;
	    errno = 0;
	    typename PAGELAYOUT::FMT::TUP::TYP5 t = strtoll(toks[column[5]], &endptr, 0);
	    if(strlen(toks[column[5]]) - (size_t)(endptr-toks[column[5]]) > 1) {
	      printf("couldnt parse token #%d: ->%s<-\n", column[5], toks[column[5]]);
	      return 1;
	    }
	    if(errno) {
	      printf("Couldn't parse token #%d: ->%s<-", column[5],toks[column[5]]);
	      perror("strtoll error is");
	      return 1;
	    }
	    scratch.set5(&t);
	  }
	  if(6 < PAGELAYOUT::FMT::TUP::NN) {
	    char * endptr;
	    errno = 0;
	    typename PAGELAYOUT::FMT::TUP::TYP6 t = strtoll(toks[column[6]], &endptr, 0);
	    if(strlen(toks[column[6]]) - (size_t)(endptr-toks[column[6]]) > 1) {
	      printf("couldnt parse token #%d: ->%s<-\n", column[6], toks[column[6]]);
	      return 1;
	    }
	    if(errno) {
	      printf("Couldn't parse token #%d: ->%s<-", column[6],toks[column[6]]);
	      perror("strtoll error is");
	      return 1;
	    }
	    scratch.set6(&t);
	  }
	  if(7 < PAGELAYOUT::FMT::TUP::NN) {
	    char * endptr;
	    errno = 0;
	    typename PAGELAYOUT::FMT::TUP::TYP7 t = strtoll(toks[column[7]], &endptr, 0);
	    if(strlen(toks[column[7]]) - (size_t)(endptr-toks[column[7]]) > 1) {
	      printf("couldnt parse token #%d: ->%s<-\n", column[7], toks[column[7]]);
	      return 1;
	    }
	    if(errno) {
	      printf("Couldn't parse token #%d: ->%s<-", column[7],toks[column[7]]);
	      perror("strtoll error is");
	      return 1;
	    }
	    scratch.set7(&t);
	  }
	  if(8 < PAGELAYOUT::FMT::TUP::NN) {
	    char * endptr;
	    errno = 0;
	    typename PAGELAYOUT::FMT::TUP::TYP8 t = strtoll(toks[column[8]], &endptr, 0);
	    if(strlen(toks[column[8]]) - (size_t)(endptr-toks[column[8]]) > 1) {
	      printf("couldnt parse token #%d: ->%s<-\n", column[8], toks[column[8]]);
	      return 1;
	    }
	    if(errno) {
	      printf("Couldn't parse token #%d: ->%s<-", column[8],toks[column[8]]);
	      perror("strtoll error is");
	      return 1;
	    }
	    scratch.set8(&t);
	  }
	  if(9 < PAGELAYOUT::FMT::TUP::NN) {
	    char * endptr;
	    errno = 0;
	    typename PAGELAYOUT::FMT::TUP::TYP9 t = strtoll(toks[column[9]], &endptr, 0);
	    if(strlen(toks[column[9]]) - (size_t)(endptr-toks[column[9]]) > 1) {
	      printf("couldnt parse token #%d: ->%s<-\n", column[9], toks[column[9]]);
	      return 1;
	    }
	    if(errno) {
	      printf("Couldn't parse token #%d: ->%s<-", column[9],toks[column[9]]);
	      perror("strtoll error is");
	      return 1;
	    }
	    scratch.set9(&t);
	  }

	  if(10 < PAGELAYOUT::FMT::TUP::NN) {
	    char * endptr;
	    errno = 0;
	    typename PAGELAYOUT::FMT::TUP::TYP10 t = strtoll(toks[column[10]], &endptr, 0);
	    if(strlen(toks[column[10]]) - (size_t)(endptr-toks[column[10]]) > 1) {
	      printf("couldnt parse token #%d: ->%s<-\n", column[10], toks[column[10]]);
	      return 1;
	    }
	    if(errno) {
	      printf("Couldn't parse token #%d: ->%s<-", column[10],toks[column[10]]);
	      perror("strtoll error is");
	      return 1;
	    }
	    scratch.set10(&t);
	  }
	  if(11 < PAGELAYOUT::FMT::TUP::NN) {
	    char * endptr;
	    errno = 0;
	    typename PAGELAYOUT::FMT::TUP::TYP11 t = strtoll(toks[column[11]], &endptr, 0);
	    if(strlen(toks[column[11]]) - (size_t)(endptr-toks[column[11]]) > 1) {
	      printf("couldnt parse token #%d: ->%s<-\n", column[11], toks[column[11]]);
	      return 1;
	    }
	    if(errno) {
	      printf("Couldn't parse token #%d: ->%s<-", column[11],toks[column[11]]);
	      perror("strtoll error is");
	      return 1;
	    }
	    scratch.set11(&t);
	  }
	  if(12 < PAGELAYOUT::FMT::TUP::NN) {
	    char * endptr;
	    errno = 0;
	    typename PAGELAYOUT::FMT::TUP::TYP12 t = strtoll(toks[column[12]], &endptr, 0);
	    if(strlen(toks[column[12]]) - (size_t)(endptr-toks[column[12]]) > 1) {
	      printf("couldnt parse token #%d: ->%s<-\n", column[12], toks[column[12]]);
	      return 1;
	    }
	    if(errno) {
	      printf("Couldn't parse token #%d: ->%s<-", column[12],toks[column[12]]);
	      perror("strtoll error is");
	      return 1;
	    }
	    scratch.set12(&t);
	  }
	  if(13 < PAGELAYOUT::FMT::TUP::NN) {
	    char * endptr;
	    errno = 0;
	    typename PAGELAYOUT::FMT::TUP::TYP13 t = strtoll(toks[column[13]], &endptr, 0);
	    if(strlen(toks[column[13]]) - (size_t)(endptr-toks[column[13]]) > 1) {
	      printf("couldnt parse token #%d: ->%s<-\n", column[13], toks[column[13]]);
	      return 1;
	    }
	    if(errno) {
	      printf("Couldn't parse token #%d: ->%s<-", column[13],toks[column[13]]);
	      perror("strtoll error is");
	      return 1;
	    }
	    scratch.set13(&t);
	  }
	  if(14 < PAGELAYOUT::FMT::TUP::NN) {
	    char * endptr;
	    errno = 0;
	    typename PAGELAYOUT::FMT::TUP::TYP14 t = strtoll(toks[column[14]], &endptr, 0);
	    if(strlen(toks[column[14]]) - (size_t)(endptr-toks[column[14]]) > 1) {
	      printf("couldnt parse token #%d: ->%s<-\n", column[14], toks[column[14]]);
	      return 1;
	    }
	    if(errno) {
	      printf("Couldn't parse token #%d: ->%s<-", column[14],toks[column[14]]);
	      perror("strtoll error is");
	      return 1;
	    }
	    scratch.set14(&t);
	  }
	  if(15 < PAGELAYOUT::FMT::TUP::NN) {
	    char * endptr;
	    errno = 0;
	    typename PAGELAYOUT::FMT::TUP::TYP15 t = strtoll(toks[column[15]], &endptr, 0);
	    if(strlen(toks[column[15]]) - (size_t)(endptr-toks[column[15]]) > 1) {
	      printf("couldnt parse token #%d: ->%s<-\n", column[15], toks[column[15]]);
	      return 1;
	    }
	    if(errno) {
	      printf("Couldn't parse token #%d: ->%s<-", column[15],toks[column[15]]);
	      perror("strtoll error is");
	      return 1;
	    }
	    scratch.set15(&t);
	  }
	  if(16 < PAGELAYOUT::FMT::TUP::NN) {
	    char * endptr;
	    errno = 0;
	    typename PAGELAYOUT::FMT::TUP::TYP16 t = strtoll(toks[column[16]], &endptr, 0);
	    if(strlen(toks[column[16]]) - (size_t)(endptr-toks[column[16]]) > 1) {
	      printf("couldnt parse token #%d: ->%s<-\n", column[16], toks[column[16]]);
	      return 1;
	    }
	    if(errno) {
	      printf("Couldn't parse token #%d: ->%s<-", column[16],toks[column[16]]);
	      perror("strtoll error is");
	      return 1;
	    }
	    scratch.set16(&t);
	  }
	  if(17 < PAGELAYOUT::FMT::TUP::NN) {
	    char * endptr;
	    errno = 0;
	    typename PAGELAYOUT::FMT::TUP::TYP17 t = strtoll(toks[column[17]], &endptr, 0);
	    if(strlen(toks[column[17]]) - (size_t)(endptr-toks[column[17]]) > 1) {
	      printf("couldnt parse token #%d: ->%s<-\n", column[17], toks[column[17]]);
	      return 1;
	    }
	    if(errno) {
	      printf("Couldn't parse token #%d: ->%s<-", column[17],toks[column[17]]);
	      perror("strtoll error is");
	      return 1;
	    }
	    scratch.set17(&t);
	  }
	  if(18 < PAGELAYOUT::FMT::TUP::NN) {
	    char * endptr;
	    errno = 0;
	    typename PAGELAYOUT::FMT::TUP::TYP18 t = strtoll(toks[column[18]], &endptr, 0);
	    if(strlen(toks[column[18]]) - (size_t)(endptr-toks[column[18]]) > 1) {
	      printf("couldnt parse token #%d: ->%s<-\n", column[18], toks[column[18]]);
	      return 1;
	    }
	    if(errno) {
	      printf("Couldn't parse token #%d: ->%s<-", column[18],toks[column[18]]);
	      perror("strtoll error is");
	      return 1;
	    }
	    scratch.set18(&t);
	  }
	  if(19 < PAGELAYOUT::FMT::TUP::NN) {
	    char * endptr;
	    errno = 0;
	    typename PAGELAYOUT::FMT::TUP::TYP19 t = strtoll(toks[column[19]], &endptr, 0);
	    if(strlen(toks[column[19]]) - (size_t)(endptr-toks[column[19]]) > 1) {
	      printf("couldnt parse token #%d: ->%s<-\n", column[19], toks[column[19]]);
	      return 1;
	    }
	    if(errno) {
	      printf("Couldn't parse token #%d: ->%s<-", column[19],toks[column[19]]);
	      perror("strtoll error is");
	      return 1;
	    }
	    scratch.set19(&t);
	  }

	  int needupdate = 0;
	  if(last_ts_col != *(epoch_t*)scratch.get(XID_COL)) {
	    this_xid++;
	    last_ts_col = *(epoch_t*)scratch.get(XID_COL);
	    needupdate = 1;
	  }

	  if(!strcmp(mode, "add")) {
	    (*(epoch_t*)scratch.get(XID_COL)) = this_xid * 2;  // will be *2 + 1 for deletes
	    //	  abort();
	    TlsmTableInsert(h,scratch);
	    if(needupdate) {
	      TlsmTableUpdateTimestamp(h,(this_xid-1) * 2);
	      count--;
	    }

	  } else if(!strcmp(mode, "delete")) {
	    (*(epoch_t*)scratch.get(XID_COL)) = this_xid * 2 + 1;  // + 1 => delete
	    TlsmTableInsert(h,scratch);
	    if(needupdate) { TlsmTableUpdateTimestamp(h,(this_xid-1) * 2 + 1); }
	  } else if(!strcmp(mode, "deliver")) {
	    TlsmTableInsert(h,scratch);
	    (*(epoch_t*)scratch.get(XID_COL)) = this_xid * 2 + 1;  // + 1 => delete
	    (*(epoch_t*)scratch.get(PAGELAYOUT::FMT::TUP::NN-1)) = 0;   // undelivered tuple
	    TlsmTableInsert(h,scratch);
	    if(needupdate) { TlsmTableUpdateTimestamp(h,(this_xid-1) * 2 + 1); }
	  } else if(!strcmp(mode, "status")) {
	    typename PAGELAYOUT::FMT::TUP scratch2;
	    // XXX never finds tuples; gets to correct page, then
	    // fails because it doesn't know the xid, so no tuples match.
	    //	    TlsmTableFindC0(xid,h,scratch, scratch2);
	  }
	  //count --;
	  if(!count) {
	    count = COUNT;
	    gettimeofday(&now_tv,0);
	    now = tv_to_double(now_tv);
	    printf("After %6.1f seconds, wrote %d tuples "
		   "%9.3f Mtup/sec (avg) %9.3f Mtup/sec (cur) "
		   "%9.3f Mbyte/sec (avg) %9.3f Mbyte/sec (cur)\n",
		   now - cannonical_start,
		   inserts, //((inserts+1) * 100) / INSERTS,
		   ((double)inserts/1000000.0)/(now-start),
		   ((double)count/1000000.0)/(now-last_start),
		   (((double)PAGELAYOUT::FMT::TUP::sizeofBytes())*(double)inserts/1000000.0)/(now-start),
		   (((double)PAGELAYOUT::FMT::TUP::sizeofBytes())*(double)count/1000000.0)/(now-last_start)
		   );
	    last_start = now;
	    //int count = TlsmTableCount(xid,h);
	    //printf("counted %d tuples\n", count);
	  }
	}
      }
      printf("insertions done.\n");
    } else {
      for(long int i = 0; i < INSERTS; i++) {
	getTuple<PAGELAYOUT>(i,t);
	TlsmTableInsert(h,t);
	//      getTuple<PAGELAYOUT>(i,t);
	//      assert(TlsmTableFind(xid,h,t,s));
	count --;
	if(!count) {
	  count = COUNT;
	  gettimeofday(&now_tv,0);
	  now = tv_to_double(now_tv);
	  printf("%3ld%% write "
		 "%9.3f Mtup/sec (avg) %9.3f Mtup/sec (cur) "
		 "%9.3f Mbyte/sec (avg) %9.3f Mbyte/sec (cur)\n",
		 ((i+1) * 100) / INSERTS,
		 ((double)i/1000000.0)/(now-start),
		 ((double)count/1000000.0)/(now-last_start),
		 (((double)PAGELAYOUT::FMT::TUP::sizeofBytes())*(double)i/1000000.0)/(now-start),
		 (((double)PAGELAYOUT::FMT::TUP::sizeofBytes())*(double)count/1000000.0)/(now-last_start)
		 );
	  last_start = now;
	}
#ifdef LEAK_TEST
	if(i == INSERTS-1) {
	  printf("Running leak test; restarting from zero.\n");
	  i = 0;
	}
#endif
      }
      printf("insertions done.\n"); fflush(stdout);

      count = COUNT;

      gettimeofday(&start_tv,0);
      start = rose::tv_to_double(start_tv);
      last_start = start;

      for(long int i = 0; i < INSERTS; i++) {

	getTuple<PAGELAYOUT>(i,t);

	typename PAGELAYOUT::FMT::TUP const * const sp = TlsmTableFind(xid,h,t,s);
	assert(sp);
	assert(*sp == s);
	count--;
	if(!count) {
	  count = COUNT;
	  gettimeofday(&now_tv,0);
	  now = tv_to_double(now_tv);
	  printf("%3ld%% read "
		 "%9.3f Mtup/sec (avg) %9.3f Mtup/sec (cur) "
		 "%9.3f Mbyte/sec (avg) %9.3f Mbyte/sec (cur)\n",
		 ((i+1) * 100) / INSERTS,
		 ((double)i/1000000.0)/(now-start),
		 ((double)count/1000000.0)/(now-last_start),
		 (((double)PAGELAYOUT::FMT::TUP::sizeofBytes())*(double)i/1000000.0)/(now-start),
		 (((double)PAGELAYOUT::FMT::TUP::sizeofBytes())*(double)count/1000000.0)/(now-last_start)
		 );
	  last_start = now;
	}
      }
    }

    TlsmTableStop<PAGELAYOUT>(h);

    Tdeinit();

    printf("test\n");
    return 0;
  }
}
