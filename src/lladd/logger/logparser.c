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
 * corresponding C file of the log parser
 *
 * @deprecated 
 * @see logEntry.c
 * *************/
#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "logparser.h"
#include "logstreamer.h"

static CommonLog assembleLog (long prevLSN, int xid, int type, void *extra, int extraLength);
static void writeCommonLog (CommonLog comLog);
static UpdateLog assembleUpdateLog (int funcID, recordid rid, const void *args, int arglen, int invertible, const void *preimage);
static CLRLog assembleCLRLog (long thisUpdateLSN, recordid rid, long undoNextLSN);
static int updateLogToString (char **s, UpdateLog upLog);
static int CLRLogToString (char **s, CLRLog clrlog);
long writeNewLog (long prevLSN, int xid, int type, char *extra, int extraLength) {
  long thisLSN = writeStreamPos();
  CommonLog tmpLog;
  /*  printf("LogType:%d\n", type); */
  tmpLog = assembleLog(prevLSN, xid, type, extra, extraLength);
  writeCommonLog (tmpLog);
  if (tmpLog.extraData !=0)
    free(tmpLog.extraData );  /*free up any malloc'ed string if it is there*/
  if (extra != 0)
    free(extra); /*free up extra space that might have been malloced previously*/
  return thisLSN;
}

int updatePartsToString (char **buf, int funcID, recordid rid, const char *args, int arglen, int invertible, const void *preImage) {
  UpdateLog ul;
  int sizeOfString;
  ul = assembleUpdateLog(funcID, rid, args, arglen, invertible, preImage); 
  sizeOfString = updateLogToString(buf, ul);
  /* if (ul.args!=0)
	  free(ul.args); //deallocate the update log's arg string that was malloc'ed in assembleUpdateLog (assembleupdate log no longer mallocs. )
    if (!invertible)
      free(ul.preImage); */
  return sizeOfString;
}

int CLRPartsToString (char **txt, long thisUpdateLSN, recordid rid, long undoNextLSN) {
  	return CLRLogToString(txt, assembleCLRLog(thisUpdateLSN, rid, undoNextLSN));
}

CommonLog readNextCommonLog() {
  return readCommonLogFromLSN(readPos());
}

CommonLog readCommonLogFromLSN(long LSN) {
  byte * buf;/*  char *buf;*/
  long junk;
  int bufPos, numObjs, extraSize;
  CommonLog commonStuff;
  commonStuff.LSN = LSN;
  commonStuff.type = 0; 
  /*seekandreadlog will allocate space for buffer */
  junk = seekAndReadLog(LSN, &buf);
  if (junk==0) {
	  commonStuff.valid=0;
	  free(buf);
	  return commonStuff;
  }
  numObjs = sscanf ((char*)buf, "%ld %d %d", &(commonStuff.prevLSN), &(commonStuff.xid), &(commonStuff.type));

  if (numObjs!=3) {
    commonStuff.valid = 0;
    free(buf);
    return commonStuff; /*if we don't have the 3 basic ingredients, invalid log entry or at end of file, etc*/
  }
  commonStuff.valid = 1; 
  /*try to read extra data, if possible (will first have a size in bytes of the future data)
    check if there is data beyond the 3 fields for optional data, and if so note where to put the data in buf*/
  if (sscanf((char*)buf, "%ld %*d %*d %d %n", &junk, &extraSize, &bufPos)==2) { /*note: pos does not increase the object count of sscanf*/
    /*then there is an extra string*/
	commonStuff.extraData = (void*)malloc(extraSize);
    commonStuff.extraDataSize = extraSize;
    memcpy(commonStuff.extraData, buf+bufPos, extraSize);
    /*printf ("reading extradata=%d %d; extrasize=%d, bufpos=%d\n", *(int *)commonStuff.extraData, *(int *)(commonStuff.extraData+4), extraSize, bufPos);*/
  }
  else {
	  
    commonStuff.extraData = NULL;
    commonStuff.extraDataSize = 0;
  }
  free(buf);
  return commonStuff;
}

UpdateLog updateStringToParts(byte *txt) {
  UpdateLog thisUpdate;
  int txtPos;
  long int tmp_size;
  /*read in the fixed size parts and get the position of where the data begins*/
  sscanf ((char*)txt, "%d %d %d %ld %d %d%n", &(thisUpdate.funcID), &(thisUpdate.rid.page), 
	  &(thisUpdate.rid.slot), &tmp_size,/*(thisUpdate.rid.size),*/ &(thisUpdate.invertible), &thisUpdate.argSize, &txtPos);
  thisUpdate.rid.size = (size_t) tmp_size;
  txtPos++; /*there is an additional space between the argSize and the arg data*/
  thisUpdate.args = (void*)malloc(thisUpdate.argSize);
  memcpy((byte*)thisUpdate.args, txt+txtPos, thisUpdate.argSize);
  txtPos += 1+thisUpdate.argSize;  
  if (!thisUpdate.invertible) {
  	thisUpdate.preImage = (void*)malloc(thisUpdate.rid.size);
	/* Remove the const qualifier from preImage so that we can initialize the malloced memory. */
	memcpy((void*)thisUpdate.preImage, txt+txtPos, thisUpdate.rid.size);
  }
  return thisUpdate;
}

CLRLog CLRStringToParts (void *txt) {
  CLRLog clrlog;
  long int size_tmp;
  sscanf(txt, "%ld %d %d %ld %ld", &(clrlog.thisUpdateLSN), &(clrlog.rid.page), &(clrlog.rid.slot), &(size_tmp), &(clrlog.undoNextLSN));
  clrlog.rid.size = (size_t) size_tmp;
  return clrlog;

}

/* TODO:  Fix string buffer size!!! */
int allocPartsToString (char **txt, recordid rid) {

  return asprintf(txt, "%d %d %ld", rid.page, rid.slot, (long int)rid.size);
  /* *txt = malloc(2*sizeof(int)+sizeof(long)+200);
     sprintf (*txt, "%d %d %ld", rid.page, rid.slot, rid.size); */
  /*memcpy(txt, &rid, sizeof(recordid));
    printf ("writing rid=%d,%d,%ld\n", rid.page, rid.slot, rid.size);*/
  /*return sizeof(recordid);*/
}

recordid allocStringToRID (void *txt) {
  recordid tmp;
  long int tmp2;
  sscanf(txt, "%d %d %ld", &tmp.page, &tmp.slot, &tmp2);
  tmp.size = (size_t)tmp2;
  /*memcpy(&tmp, txt, sizeof(recordid));
    printf ("reading rid=%d,%d,%ld\n", tmp.page, tmp.slot, tmp.size);*/
  return tmp;
}

/************ PRIVATE WRITING-RELATED FUNCTIONS ********************/
/*
 put the pieces of a complete (common) log into the structure
*/
static CommonLog assembleLog (long prevLSN, int xid, int type, void *extra, int extraLength) {
  CommonLog tmp;
  tmp.prevLSN = prevLSN;
  tmp.xid = xid;
  tmp.type = type;
  tmp.valid = 1;
  tmp.extraDataSize = extraLength;
  if (extraLength==0)
    tmp.extraData = NULL;
  else {
    tmp.extraData = (void*)malloc(extraLength);
    memcpy(tmp.extraData, extra, extraLength);
  }
  return tmp;
}

/*
  marshal an common log into the log file
*/
static void writeCommonLog (CommonLog comLog) {
  char *buf;
  int bytesWritten;
  if (comLog.extraDataSize==0) {
    /*TODO: Fix buffer size! (Throughout file) */
    /*	buf = malloc(2*sizeof(int)+sizeof(long)+200); */
    bytesWritten = asprintf (&buf, "%ld %d %d", comLog.prevLSN, comLog.xid, comLog.type);
  } 
  else {
    char * buf2;
    /*	buf = malloc(3*sizeof(int)+sizeof(long)+200+4+comLog.extraDataSize); */
    /*first print all the ordinary components plus a space, then size of extra; note the last byte put into sprintf*/
    bytesWritten = asprintf (&buf, "%ld %d %d %d ", comLog.prevLSN, comLog.xid, comLog.type, comLog.extraDataSize);
    buf2 = malloc(bytesWritten + comLog.extraDataSize);
    memcpy (buf2, buf, bytesWritten);
    free(buf);
    buf = buf2; 
    /*directly write the data into the buffer after the rest of the stuff*/
    memcpy (buf+bytesWritten, comLog.extraData, comLog.extraDataSize);
    /*printf("writing extra=%d %d\n", *(int *)comLog.extraData, *(int *)(comLog.extraData+4));*/
  }
  writeLog(buf, bytesWritten+comLog.extraDataSize);
  free(buf);
}

/*
  Given all the components of an update log, will produce an update log

  Returns an UpdateLog
  NOTE: mallocates the output log's args string, so must be freed after use (Not true any more.)
*/
static UpdateLog assembleUpdateLog (int funcID, recordid rid, const void *args, int arglen, int invertible, const void *preimage) {
  UpdateLog tmpLog;
  tmpLog.funcID = funcID;
  tmpLog.rid = rid;
  /*  tmpLog.args = (void*)malloc(arglen); */
  tmpLog.argSize = arglen;
  tmpLog.invertible = invertible;
  /*  memcpy (tmpLog.args, args, arglen); */

  tmpLog.args = args;

  if (!invertible) {
    /*Don't need this?  Just set the pointer... */
    /*	  tmpLog.preImage = (void*)malloc(rid.size);
	  memcpy(tmpLog.preImage, preimage, rid.size); */
    tmpLog.preImage = preimage;
  } else {
    tmpLog.preImage = 0;
  }
  return tmpLog;
}

static CLRLog assembleCLRLog (long thisUpdateLSN, recordid rid, long undoNextLSN) {
  CLRLog tmpLog;
  tmpLog.thisUpdateLSN = thisUpdateLSN;
  tmpLog.rid = rid;
  tmpLog.undoNextLSN = undoNextLSN;
  return tmpLog;
}
/*
  marshal an update log into a string (assume the string space has been allocated)
  returns number of bytes written to the string
*/
static int updateLogToString (char **s, UpdateLog upLog) {
  int bytesWritten;
  /*  int maxDigitsForInt = 25; //32767 is 5 digits, plus a negative
  int maxDigitsForLong = 25;
  int estSize = 5*maxDigitsForInt+maxDigitsForLong+6+upLog.argSize;*/

  char *tmp;

  /*  if (!upLog.invertible)
      estSize+=1+upLog.rid.size; */

  
  bytesWritten = asprintf (&tmp, "%d %d %d %ld %d %d ", upLog.funcID, upLog.rid.page, upLog.rid.slot, (long int)upLog.rid.size, upLog.invertible, upLog.argSize);

  *s = malloc(bytesWritten + upLog.argSize);
  memcpy(*s, tmp, bytesWritten);
  free (tmp);
  memcpy(*s+bytesWritten, upLog.args, upLog.argSize);
  bytesWritten+=upLog.argSize;
  if (!upLog.invertible) {
    tmp = *s;
    *s = malloc(bytesWritten+1 +upLog.rid.size);
    memcpy(*s, tmp, bytesWritten);
    free (tmp);

    sprintf (*s+bytesWritten, " ");
    memcpy(*s+bytesWritten+1, upLog.preImage, upLog.rid.size);
    bytesWritten += 1 + upLog.rid.size;
  }
  return bytesWritten;
}

static int CLRLogToString (char **s, CLRLog clrlog) {
  /*  int estSize = 3*sizeof(long)+2*sizeof(int)+400; */
  int bytesWritten;
  /*  *s = malloc(estSize); */
  bytesWritten = asprintf (s, "%ld %d %d %ld %ld ", clrlog.thisUpdateLSN, clrlog.rid.page, clrlog.rid.slot, (long int)clrlog.rid.size, clrlog.undoNextLSN);
  /*  if (bytesWritten!=estSize) {
	  //error!
	  }*/
  return bytesWritten;
}
