/* This CRC code was taken from: http://www.axlradius.com/freestuff/crc2.c

   (It is presumably in the public domain.  Other files under /freestuff/ are...)

   ( Added a #include for this .h file.  Otherwise, crc32 is a verbatim copy of the file from the internet.)

*/



/**
   Usage:
   unsigned long crc = -1L
   crc = crc32(buffer, length, crc)
*/

unsigned long crc32(void *buffer, unsigned int count, unsigned long crc);
