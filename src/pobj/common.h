#ifndef __COMMON_H
#define __COMMON_H

#define member_sizeof(s,x)  (sizeof(((s *)NULL)->x))
#define member_offset(s,x)  ((int)&(((s *)NULL)->x))

#endif /* __COMMON_H */

