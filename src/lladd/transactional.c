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


/**
 * @file
 * $Id$
 *
 * implements main interface to tranactional pages
 *
 * @deprecated 
 * @see transactional2.c
 * ************************************************/
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>

#include <lladd/transactional.h>
#include <lladd/constants.h>
#include <lladd/logger.h>
#include <lladd/bufferManager.h>
#include "recovery.h"

Transaction XactionTable[MAX_TRANSACTIONS];
int numActiveXactions = 0;
int xidCount = 0;
#define INVALID_XTABLE_XID -1

int Tinit() {

	memset(XactionTable, INVALID_XTABLE_XID, sizeof(Transaction)*MAX_TRANSACTIONS);
	operationsTable[OPERATION_SET]       = getSet();
	operationsTable[OPERATION_INCREMENT] = getIncrement();
	operationsTable[OPERATION_DECREMENT] = getDecrement();
	operationsTable[OPERATION_PREPARE]   = getPrepare();
	operationsTable[OPERATION_LHINSERT]  = getLHInsert();
	operationsTable[OPERATION_LHREMOVE]  = getLHRemove();

	
	pageInit();
	bufInit();
	logInit();
	
	return 0;
}

int Tbegin() {

	int i, index = 0;

	if( numActiveXactions == MAX_TRANSACTIONS )
		return EXCEED_MAX_TRANSACTIONS;
	else
		numActiveXactions++;

	for( i = 0; i < MAX_TRANSACTIONS; i++ ) {
		xidCount++;
		if( XactionTable[xidCount%MAX_TRANSACTIONS].xid == INVALID_XTABLE_XID ) {
			index = xidCount%MAX_TRANSACTIONS;
			break;
		}
	}

	assert( i < MAX_TRANSACTIONS );

	XactionTable[index].xid = xidCount;
	XactionTable[index].LSN = LogTransBegin(XactionTable[index]);

	return XactionTable[index].xid;
}

void Tupdate(int xid, recordid rid, const void *dat, int op) {
	assert(numActiveXactions <= MAX_TRANSACTIONS);
	writeLSN(XactionTable[xid%MAX_TRANSACTIONS].LSN = LogUpdate(XactionTable[xid%MAX_TRANSACTIONS].LSN, xid, rid, operationsTable[op], dat), rid.page);
	operationsTable[op].run(xid, rid, dat);
}

recordid Talloc(int xid, size_t size) {
	recordid ret;

	ret = ralloc(xid, size);
	
	writeLSN(XactionTable[xid%MAX_TRANSACTIONS].LSN = LogTransAlloc(XactionTable[xid%MAX_TRANSACTIONS].LSN, xid, ret), ret.page);
	return ret;
}
void Tread(int xid, recordid rid, void *dat) {

        readRecord(xid, rid, dat);
}

int Tcommit(int xid) {

	LogTransCommit(XactionTable[xid%MAX_TRANSACTIONS].LSN, xid);
	bufTransCommit(xid); /* unlocks pages */
	XactionTable[xid%MAX_TRANSACTIONS].xid = INVALID_XTABLE_XID;
	numActiveXactions--;
	assert( numActiveXactions >= 0 );
	return 0;
}

int Tabort(int xid) {
  /* should call undoTrans after log trans abort.  undoTrans will cause pages to contain CLR values corresponding to  */
	undoTrans(XactionTable[xid%MAX_TRANSACTIONS]);
	LogTransAbort(XactionTable[xid%MAX_TRANSACTIONS].LSN, xid);
	bufTransAbort(xid);
	XactionTable[xid%MAX_TRANSACTIONS].xid = INVALID_XTABLE_XID;
	numActiveXactions--;
	assert( numActiveXactions >= 0 );
	return 0;
}

int Tdeinit() {
	int i;

	for( i = 0; i < MAX_TRANSACTIONS; i++ ) {
		if( XactionTable[i].xid != INVALID_XTABLE_XID ) {
			Tabort(XactionTable[i].xid);
		}
	}
	assert( numActiveXactions == 0 );

	bufDeinit();
	logDeinit();

	return 0;
}

void Trevive(int xid, long lsn) {
  int index = xid % MAX_TRANSACTIONS;
  if(XactionTable[index].xid != INVALID_XTABLE_XID) {
    printf("Clashing Tprepare()'ed XID's encountered on recovery!!\n");
    exit(-1);
  }
  XactionTable[index].xid = xid;
  XactionTable[index].LSN = lsn;
  numActiveXactions++;
}

void TsetXIDCount(xid) {
  xidCount = xid;
}

