#ifndef __COMMON_H
#define __COMMON_H

/* Architecture specific word size and alignment. */
#define WORDSIZE  sizeof(int)
#define WORDBITS  (WORDSIZE * 8)
#define ALIGN(s)  ((size_t) (((s) + (WORDSIZE - 1)) / WORDSIZE) * WORDSIZE)

#endif /* __COMMON_H */

