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

/*****************
 * @file
 * $Id$
 * 
 * C implementation of logger.h
 *
 * @deprecated
 * @see logger2.c
 ******************/
#include <stdlib.h>

#include <lladd/transactional.h>
#include <lladd/logger.h>
#include "logparser.h"
#include "logstreamer.h"
#include <lladd/bufferManager.h>

static long LogCommonWriterNoExtra(long prevLSN, int xid, int type); /*defined later*/

long LogTransBegin(Transaction t) {
  return -1;
}

/* jason please review and delete comment when done
 * to recover: pageRalloc(loadPage(rid.page), rid.size)
 */
long LogTransAlloc(long prevLSN, int xid, recordid rid) {
	char *extra;
	int sizeOfExtra = allocPartsToString(&extra, rid);
	return writeNewLog(prevLSN, xid, XALLOC, extra, sizeOfExtra);
}


long LogTransCommit(long prevLSN, int xid) {
	long newLSN = LogCommonWriterNoExtra(prevLSN, xid, XCOMMIT);
	flushLog();
	return newLSN;
}

long LogTransAbort(long prevLSN, int xid) {
	return LogCommonWriterNoExtra(prevLSN, xid, XABORT);
}

long LogUpdate (long prevLSN, int xid, recordid rid, Operation op, const void *args) {
	char *extra;
	/*	size_t sizeOfData, sizeOfExtra; */
	int sizeOfData, sizeOfExtra;
	void *preImage = 0;
	int invertible;
	if (op.sizeofData==SIZEOF_RECORD) {
	  /*then get the size of this record */
		sizeOfData = rid.size;
	}
	else sizeOfData = op.sizeofData;
	/*heck to see if op is invertible */
	if (op.undo==NO_INVERSE) {
		invertible = 0;
		preImage = (void *)malloc(rid.size);
		readRecord(xid, rid, preImage);
	}
	else invertible = 1;
	/*will put all of the parts special to an update log record into extra */
	sizeOfExtra = updatePartsToString(&extra, op.id, rid, args, sizeOfData, invertible, preImage);
	/*extra has been malloced, but writeNewLog will free it*/
	if(preImage) {
	  free(preImage);
	}
	return writeNewLog(prevLSN, xid, UPDATELOG, extra, sizeOfExtra);
}

long LogCLR (long prevLSN, int xid, long ulLSN, recordid ulRID, long ulPrevLSN) {
#define CLRSIZEOFEXTRA 20
	char *extra;
	CLRPartsToString(&extra, ulLSN, ulRID, ulPrevLSN);
	/*extra has been malloced, but writeNewLog will free it*/
	return writeNewLog(prevLSN, xid, CLRLOG, extra, CLRSIZEOFEXTRA);
}

long LogEnd (long prevLSN, int xid) {
	return LogCommonWriterNoExtra(prevLSN, xid, XEND);
}
/*
   LogCommonWriterNoExtra writes the log record in the log tail when there is no extra data
   being put in the log record -- a special case, and therefore just calls the more generalized LogCommongWriter
   */
static long LogCommonWriterNoExtra(long prevLSN, int xid, int type) {
	return writeNewLog(prevLSN, xid, type, 0, 0);
}

void logInit() {
	startLogStream();
}

void logDeinit() {
	closeLogStream();
	/** FOR NOW, don't delete the log stream at the end, even if all transactions
	 * are committed because we want to debug
	 */
	/*deleteLogStream();*/
}
