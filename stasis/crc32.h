#ifndef STASIS_CRC32_H
#define STASIS_CRC32_H
/* This CRC code was taken from: http://www.axlradius.com/freestuff/crc2.c

   (It is presumably in the public domain.  Other files under /freestuff/ are...)

   ( Added a #include for this .h file.  Otherwise, crc32 is a verbatim copy of the file from the internet.)

*/



/**
   Usage:
   unsigned int crc = -1L
   crc = crc32(buffer, length, crc)
*/
#include <stasis/common.h>
uint32_t stasis_crc32(const void *buffer, unsigned int count, uint32_t crc);
#endif // STASIS_CRC32_H
