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

#include <stdio.h>
#include <assert.h>
#include "test.h"
#include <lladd/transactional.h>

int test(int argc, char **argv) {
	int j;
	int after_39 = 0;
	int xid = Tbegin();
	
	lladdHash_t * ht = lHtCreate(xid, 700);
	
	for(j = 0; j < 1000; j++) {
		lHtInsert(xid, ht, &j, sizeof(int), &j, sizeof(int));
	}

	Tcommit(xid);
	
	// Test successful lookup + committed inserts.

	xid = Tbegin();
	
	for(j = 0; j < 1000; j++) {
		int k = - 100;
		assert(!lHtLookup(xid, ht, &j, sizeof(int), &k));
		assert(j==k);
	}

	Tcommit(xid);
	
	// Test failed lookups. 

	xid = Tbegin();
	
	for(j = 1000; j < 2000; j++) {
		int k = - 100;
		assert(lHtLookup(xid, ht, &j, sizeof(int), &k) == -1);
	}

	Tcommit(xid);

	// Test aborted inserts
	
	xid = Tbegin();
	
	for(j = 10000; j < 11000; j++) {
		lHtInsert(xid, ht, &j, sizeof(int), &j, sizeof(int));
	}

	for(j = 10000; j < 11000; j++) {
		int k = - 100;
		assert(!lHtLookup(xid, ht, &j, sizeof(int), &k));
		assert(k==j);
	}
	
	Tabort(xid);
	
	xid = Tbegin();
	
	for(j = 10000; j < 11000; j++) {
		int k = - 100;
		assert(-1 == lHtLookup(xid, ht, &j, sizeof(int), &k));
	}
	
	Tcommit(xid);

	// Test aborted removes.

	xid = Tbegin();
	for(j = 0; j < 1000; j+=10) {
		int k = -100;
		assert(!lHtRemove(xid, ht, &j, sizeof(int), &k, sizeof(int)));
		
		assert(j==k);
		
	}

	Tabort(xid);
	
	xid = Tbegin();
	
	for(j = 0; j < 1000; j+=10) {
		int k = -100;
		assert(!lHtRemove(xid, ht, &j, sizeof(int), &k, sizeof(int)));
		
		assert(j==k);
		
	}

	Tcommit(xid);

	
	for(j = 0; j < 1000; j+=10) {
		int k = -100;
		xid = Tbegin();

		assert(-1 == lHtRemove(xid, ht, &j, sizeof(int), &k, sizeof(int)));

		Tcommit(xid);

	}

	
	
	/* ------------- Now, test the iterator.  ------------------- */
	
	xid = Tbegin();
	
	lHtFirst(xid, ht, &j);

	for(j = 0; j < 2000; j++) {
		int k;
		if(-1 == lHtNext(xid, ht, &k)) { after_39++; break; }
		assert((k >= 10000 && k < 11000) || 
					 (k % 10 && k > 0 && k < 1000));

		if(after_39 || k == 39) after_39++;
	}
	
	assert(j == 899);
	Tcommit(xid);

	j = 39;
	
	assert(-1 != lHtPosition(xid, ht, &j, sizeof(int)));
	
	j =0;
	
	while(lHtNext(xid, ht, NULL) != -1) {
		j++;
	}
	
	assert(j == after_39);
	
	return 0;
	
}
