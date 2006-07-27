/*--- This software is copyrighted by the Regents of the University of
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
#include <config.h>
#include <lladd/common.h>
#include <../../src/lladd/latches.h>
#include <check.h>
#include <assert.h>

#include <lladd/transactional.h>
#include "../check_includes.h"
#define LOG_NAME   "check_regions.log"

long myrandom(long x) {
  double xx = x;
  double r = random();
  double max = ((long)RAND_MAX)+1;
  max /= xx;
  return (long)((r/max));
}


/**
   @test
*/
START_TEST(regions_smokeTest) {
  Tinit();
  int xid = Tbegin();

  int max_page = 0;
  int page = TregionAlloc(xid, 100, 1);
  int new_page = page;
  if(new_page + 1 + 100 > max_page) { 
    max_page = new_page + 1 + 100;
  }

  new_page = TregionAlloc(xid, 1, 1);
  if(new_page + 2 > max_page) { 
    max_page = new_page + 2;
  }
  TregionDealloc(xid, page);

  unsigned int pages[50];

  for(int i = 0; i < 50; i++) { 
    new_page = TregionAlloc(xid, 1, 1);
    pages[i] = new_page;
    if(new_page + 2 > max_page) { 
      max_page = new_page + 2;
    }
  }

  for(int i = 0; i < 50; i+=2) { 
    TregionDealloc(xid, pages[i]);
  }

  Tcommit(xid);

  xid = Tbegin();
  for(int i = 0; i < 50; i+=2) { 
    new_page = TregionAlloc(xid, 1, 1);
    if(new_page + 2 > max_page) { 
      max_page = new_page + 2;
    }
  }
  Tabort(xid);
  xid = Tbegin();
  for(int i = 0; i < 50; i+=2) { 
    new_page = TregionAlloc(xid, 1, 1);
    if(new_page + 2 > max_page) { 
      max_page = new_page + 2;
    }
  }

  fsckRegions(xid);

  Tcommit(xid);

  printf("\nMaximum space usage = %d, best possible = %d\n", max_page, 104); // peak storage usage = 100 pages + 1 page + 3 boundary pages.

  Tdeinit();
}
END_TEST

START_TEST(regions_randomizedTest) { 
  Tinit();
  time_t seed = time(0);
  printf("\nSeed = %ld\n", seed);
  srandom(seed);
  int xid = Tbegin();
  unsigned int pagesAlloced = 0;
  unsigned int regionsAlloced = 0;
  double max_blowup = 0;
  unsigned int max_region_count = 0;
  unsigned int max_waste = 0;
  unsigned int max_size = 0;
  unsigned int max_ideal_size = 0;
  for(int i = 0; i < 10000; i++) { 
    if(!(i % 100)) { 
      Tcommit(xid);
      xid = Tbegin();
    }
    if(!(i % 100)) { 
      fsckRegions(xid);
    }

    if(myrandom(2)) { 
      unsigned int size = myrandom(100);
      TregionAlloc(xid, size, 1);
      pagesAlloced += size;
      regionsAlloced ++;
    } else {
      if(regionsAlloced) { 
	unsigned int victim = myrandom(regionsAlloced);
	unsigned int victimSize;
	unsigned int victimPage;
	TregionFindNthActive(xid, victim, &victimPage, &victimSize);
	TregionDealloc(xid, victimPage);
	pagesAlloced -= victimSize;
	regionsAlloced --;
      } else { 
	i--;
      }
    }

    if(regionsAlloced) { 
      unsigned int lastRegionStart;
      unsigned int lastRegionSize;
      
      TregionFindNthActive(xid, regionsAlloced-1, &lastRegionStart, &lastRegionSize);
      unsigned int length = lastRegionStart + lastRegionSize+1;
      unsigned int ideal  = pagesAlloced + regionsAlloced + 1;
      double blowup = (double)length/(double)ideal;
      unsigned int bytes_wasted = length - ideal;
      // printf("Region count = %d, blowup = %d / %d = %5.2f\n", regionsAlloced, length, ideal, blowup);
      if(max_blowup < blowup) { 
	max_blowup = blowup;
      }
      if(max_waste < bytes_wasted) { 
	max_waste = bytes_wasted;
      }
      if(max_size < length) { 
	max_size = length;
      }
      if(max_ideal_size < ideal) { 
	max_ideal_size = ideal;
      }
      if(max_region_count < regionsAlloced) { 
	max_region_count = regionsAlloced;
      }
    }
  }
  fsckRegions(xid);
  Tcommit(xid);

  Tdeinit();
  if((double)max_size/(double)max_ideal_size > 5) {  
    // max_blowup isn't what we want here; it measures the peak
    // percentage of the file that is unused.  Instead, we want to
    // measure the actual and ideal page file sizes for this run.
    printf("WARNING: Excessive blowup ");
  } 
  printf("Max # of regions = %d, page file size = %5.2fM, ideal page file size = %5.2fM, (blowup = %5.2f)\n",
	 //peak bytes wasted = %5.2fM, blowup = %3.2f\n", 
	 max_region_count, 
	 ((double)max_size * PAGE_SIZE)/(1024.0*1024.0), 
	 ((double)max_ideal_size * PAGE_SIZE)/(1024.0*1024.0), 
	 (double)max_size/(double)max_ideal_size);
  //	 ((double)max_waste * PAGE_SIZE)/(1024.0*1024.0), 
  //	 max_blowup);

} END_TEST

START_TEST(regions_lockSmokeTest) {
  Tinit();
  int xid = Tbegin();
  int pageid = TregionAlloc(xid, 100,1);
  fsckRegions(xid);
  Tcommit(xid);
  

  xid = Tbegin();
  int xid2 = Tbegin();

  TregionDealloc(xid, pageid);
  
  for(int i = 0; i < 50; i++) {
    TregionAlloc(xid2, 1, 1);
  }

  fsckRegions(xid);
  Tabort(xid);
  fsckRegions(xid2);
  Tcommit(xid2);
  Tdeinit();
} END_TEST

START_TEST(regions_lockRandomizedTest) { 
  Tinit();
  
  const int NUM_XACTS = 100;
  const int NUM_OPS   = 10000;
  const int FUDGE = 10;
  int xids[NUM_XACTS];

  int * xidRegions[NUM_XACTS + FUDGE];
  int xidRegionCounts[NUM_XACTS + FUDGE];
  
  int longXid = Tbegin();

  time_t seed = time(0);
  printf("\nSeed = %ld\n", seed);
  srandom(seed);

  for(int i = 0; i < NUM_XACTS; i++) { 
    xids[i] = Tbegin();
    assert(xids[i] < NUM_XACTS + FUDGE);
    xidRegions[xids[i]] = malloc(sizeof(int) * NUM_OPS);
    xidRegionCounts[xids[i]] = 0;
  }
  int activeXacts = NUM_XACTS;

  for(int i = 0; i < NUM_OPS; i++) { 
    int j;
    if(!(i % (NUM_OPS/NUM_XACTS))) { 
      // abort or commit one transaction randomly.
      activeXacts --;
      j = myrandom(activeXacts);

      if(myrandom(2)) {
	Tcommit(xids[j]);
      } else { 
	Tabort(xids[j]);
      }

      if(activeXacts == 0) { 
	break;
      }
      for(; j < activeXacts; j++) { 
	xids[j] = xids[j+1];
      }
      fsckRegions(longXid);
    }

    j = myrandom(activeXacts);

    if(myrandom(2)) {
      // alloc
      xidRegions[xids[j]][xidRegionCounts[xids[j]]] = TregionAlloc(xids[j], myrandom(100), 1);
	xidRegionCounts[xids[j]]++;
    } else {
      // free
      if(xidRegionCounts[xids[j]]) {
	int k = myrandom(xidRegionCounts[xids[j]]); 
	
	TregionDealloc(xids[j], xidRegions[xids[j]][k]);
	
	xidRegionCounts[xids[j]]--;
      
	for(; k < xidRegionCounts[xids[j]]; k++) { 
	  xidRegions[xids[j]][k] = xidRegions[xids[j]][k+1];
	}
      }
    }
  }

  for(int i = 0; i < activeXacts; i++) { 
    Tabort(i);
    fsckRegions(longXid);
  }

  Tcommit(longXid);

  Tdeinit();
} END_TEST

/** 
  Add suite declarations here
*/
Suite * check_suite(void) {
  Suite *s = suite_create("regions");
  /* Begin a new test */
  TCase *tc = tcase_create("regions_test");
  tcase_set_timeout(tc, 0); // disable timeouts

  /* Sub tests are added, one per line, here */  
  tcase_add_test(tc, regions_smokeTest);
  tcase_add_test(tc, regions_randomizedTest);
  tcase_add_test(tc, regions_lockSmokeTest);
  tcase_add_test(tc, regions_lockRandomizedTest);
  /* --------------------------------------------- */
  tcase_add_checked_fixture(tc, setup, teardown);
  suite_add_tcase(s, tc);


  return s;
}

#include "../check_setup.h"
