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

#include <stdlib.h>
#include <string.h>

#define SEED 0

char *LONG_STRING;
int LONG_STRING_LENGTH;

void rand_str_init() {
	char c;

	char first_printable_ascii = ' ';
	char last_printable_ascii = '~';
	LONG_STRING_LENGTH = (int)(last_printable_ascii - first_printable_ascii);
	LONG_STRING = (char *)malloc(sizeof(char) * LONG_STRING_LENGTH);

	for (c = 0; c < LONG_STRING_LENGTH; c++) {
		LONG_STRING[(int)c] = first_printable_ascii + c;
	}

	srand(SEED);
}

char *rand_str() {
	char *string;
	double r;

	int start = 0;
	int end = 0;
	while (start == end) {
		r = ((double)rand()/(double)((double)RAND_MAX+1)); /* returns [0, 1)*/
		r = r*LONG_STRING_LENGTH;
		start = (int)r; /* an int in the rand [0, length) */

		r = ((double)rand()/(double)((double)RAND_MAX+1)); /* re turns [0, 1)*/
		r = r*LONG_STRING_LENGTH;
		end = (int)r; /* an int in the rand [0, length) */
	}
	if (end < start) {
		int swap = start;
		start = end;
		end = swap;
	}

	string = (char *)malloc(sizeof(char) * (end - start) + 1);
	strncpy(string, LONG_STRING + start, end-start);
	string[end-start] = '\0'; /* make the string null terminated */
	return string;
}
