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
  $Id$

  the C implementation of logstreamer.h
 ******************/

#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <unistd.h>


#include <lladd/constants.h>
#include "logstreamer.h"

static FILE *log;

int startLogStream() {
	log = fopen(LOG_FILE, "a+");
	if (log==NULL) {
	  /*there was an error opening this file */
		return FILE_WRITE_OPEN_ERROR;
	}
	/*if file is empty, write a null at the 0th character */
	if (streamPos()==0)
	  fputs("0", log);	
	/*start reading at the 1st character*/
	fseek(log, 1, SEEK_SET);
	return 0;  /*otherwise return 0 for no errors*/
}

int writeLog(void *s, int length) {
  /*	int i; */
  fprintf(log, "%d ", length); /*print out the size of this log entry (not including this item and the \n */
	
	if(1 != fwrite(s, length, 1, log)) {

	/*	for( i = 0; i < length; i++ ) {
		if(fputc(*(char*)(s+i), log) == EOF) { //write the string to the log tail*/
	    perror("logstreamer: fputc");
	    assert(0);
	    /*	  } */
	}
	fprintf (log, "\n"); /*then write a carriage return just for our viewing pleasure*/
	return 0; /*otherwise, no errors*/
}

void flushLog() {
   	fflush(log); 
	/*	fsync(fileno(log));  */
	fdatasync(fileno(log));
}

void closeLogStream() {
	fclose(log);
}

void deleteLogStream() {
	remove(LOG_FILE);
}

long streamPos() {
	return ftell(log);
}


/* Assumes we're already @ EOF. */
long writeStreamPos() {
  /*  long cur = ftell(log);
  fseek(log, 0, SEEK_END);
  assert(cur == ftell(log));*/
  return ftell(log);
  /*	long curPos = streamPos();
	long returnVal;
	fseek(log, 0, SEEK_END);
    returnVal = ftell(log);
	fseek(log, curPos, SEEK_SET);
	return returnVal;*/
}
int readLog(byte **buffer) {
	int length, i;
	*buffer = 0;
	if (feof(log)) return 0;
	fscanf(log, "%d ", &length);
	if (feof(log)) return 0;
	/*get length characters from the log at this point and put them
	  into the buffer */
	*buffer = malloc(length+5); /*5 extra just to be safe*/
	for (i=0; i<length; i++)
		*(char*)(*buffer+i) = fgetc(log);
	/*experiment: null terminate the buffer to avoid read leaks over the memory just in case */
		*(char*)(*buffer+i) = '\0';
		/*then read in the newline that was put in to be nice */
	fgetc(log);
	return length;
}
			          
int seekAndReadLog(long pos, byte **buffer) {			
  fseek(log, pos, 0); /*seek to pos positions from the beginning of the file*/
	return readLog(buffer);	
}
void seekInLog(long pos) {
  fseek(log, pos, 0);
}  
long readPos () {
	return ftell(log);
}
