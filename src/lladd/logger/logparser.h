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
 *
 * logparser implements the specific formats of the log entrys to be read and
 * written in the log file.
 * 
 * All log entries have some fields in common (always) - a previous LSN, a
 * transaction id, a log type, and any extra text specific to the type of entry.
 * The extra text may be NULL, in which case there is no extra text! If there is
 * extra text, additional public functions are written to put together and take
 * apart the string into its object.
 *
 * This has been entirely rewritten.  Don't use it.
 *
 * @deprecated @see logEntry.h
 *
 * Proposed api:


 All of these return a pointer to a single malloced region that should be freed. 

 allocCommonLogEntry(LSN, prevLSN, xid, type);
 allocUpdateLogEntry(LSN, prevLSN, xid, type=UPDATE, funcID,        rid, args, argSize, preImage);
 allocCLRLogEntry   (LSN, prevLSN, xid, type=CLR,    thisUpdateLSN, rid, undoNextLSN);

 struct {
   thisUpdateLSN;
   rid; 
   undoNextLSN;
 } CLRLogEntry;

 struct {
   funcID;
   rid;
   argSize;
   / * Implicit members:
     args;     @ ((byte*)ule) + sizeof(UpdateLogEntry)
     preImage; @ ((byte*)ule) + sizeof(UpdateLogEntry) + ule.argSize * /
 } UpdateLogEntry;

 struct {
   LSN;
   prevLSN;
   xid;
   type;
   union {
     UpdateLogEntry update;
     CLRLogEntry    clr;
   } contents;
 } LogEntry;

 sizeofLogEntry(LogEntry *);
 size_t getUpdateArgs(LogEntry *);
 size_t getUpdatePreImage(LogEntry *);

 *
 * $Id$
 **/

#ifndef __LOGPARSER_H__
#define __LOGPARSER_H__

#include <lladd/page.h>
#include <lladd/constants.h>

/* max the log line can be is 2 pages (set redo and undo data possibly) plus
 * some more log stuff */
/*#define maxLogLineLength 2*PAGE_SIZE + 100 */

/*
  fields common to all log entries
  extra could be null, indicating no extra data needed
*/
typedef struct {
  /**LSN field is ignored when writing to log, since it is determined
     by the writer */  
  long LSN; 
  long prevLSN;
  int xid;
  int type;
  void *extraData;
  int extraDataSize;
  int valid;
} CommonLog;

/**
  Update Log components (that are normally stored in the 'extra' field of a common log

  The preimage is used for undos of operations with no inverses, and
  the args field is used for the redo process, and inverse functions.
*/
typedef struct {
  /**what function it is*/
  int funcID; 
  /**page and slot number*/
  recordid rid;
  /**binary data of the function arguments*/
  const byte *args; 
  int argSize;
  int invertible;
  /**if funcID is not invertible will store a preImage with size=rid.size*/
  const byte *preImage; 
} UpdateLog;

typedef struct {
  /*	UpdateLog ul; //has everything in an update log */
	long thisUpdateLSN;
	recordid rid;
	long undoNextLSN;
} CLRLog;


/********* PUBLIC WRITING-RELATED FUNCTIONS **************/
/**
  Given all the necessary components of a log entry, assemble it and then write it to the log file

  return the LSN of this new log entry

*/
long writeNewLog (long prevLSN, int xid, int type, char *extra, int extraLength);

/**
  put all of the components of the extra part of an update log into a buffer
  returns the length of the string written to buf

  @ todo Logparser shouldn't write to char**
*/
int updatePartsToString (char **buf, int funcID, recordid rid, const char *args, int arglen, int invertible, const void *preImage);

/**
  Takes in a buffer and a record id and copies the recordid into the buffer
*/
int allocPartsToString (char **txt, recordid rid);

/**
   
*/
int CLRPartsToString (char **txt, long thisUpdateLSN, recordid rid, long undoNextLSN);
/********* PUBLIC READING-RELATED FUNCTIONS **************/
/**
  parse a log line that only parses entries common to all log records into its structure. Can specify an LSN

  NOTE: the extra part of CommonLog might have a malloc'ed string in it; should be freed after use
*/
CommonLog readNextCommonLog();
CommonLog readCommonLogFromLSN(long LSN);


CLRLog readCLRLogFromLSN(long LSN);
/**
  parse the extra part of an update log record into an UpdateLog structure 

  NOTE: the args part of the UpdateLog has a malloc'ed string in it; should be freed after use
*/
UpdateLog updateStringToParts(byte *txt);

CLRLog CLRStringToParts (void *txt);
/**
  Read a buffer and parse it into a recordid
*/
recordid allocStringToRID (void *txt);

#endif
