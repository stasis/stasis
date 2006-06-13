/** 
    @file 
	
    A few io wrapper functions to simplify file I/O code in LLADD. 

*/

#include <lladd/common.h>

BEGIN_C_DECLS


#ifndef __LLADD_IO_H
#define __LLADD_IO_H

long myFseek(FILE * f, long offset, int whence);
long myFseekNoLock(FILE * f, long offset, int whence);
void myFwrite(const void * dat, long size, FILE * f);

END_C_DECLS

#endif /* __LLADD_IO_H */
