// Calculate a CRC 32 checksum.
#include <lladd/crc32.h>  /*Added 10-6-04 */
#include <stdlib.h>
#include <stdio.h>

// LAST MODIFIED:[7-28-93]

// Usage:
// unsigned long crc = -1L
// crc = crc32(buffer, length, crc)

unsigned int crc32(const void *buffer, unsigned int count, unsigned int crc);
static int BuildCRCTable(void);

static unsigned long *CRCTable;	// Table constructed for fast lookup.

#define CRC32_POLYNOMIAL	0xEDB88320

// Initialize the CRC calculation table
//
static int BuildCRCTable(void)
{
	int i, j;
	unsigned int crc;

	CRCTable = malloc(256 * sizeof(unsigned int));
	if (CRCTable == NULL)
	{
		fprintf(stderr, "Can't malloc space for CRC table in file %s\n", __FILE__);
		return -1L;
	}

	for (i = 0; i <= 255; i++)
	{
		crc = i;
		for (j = 8; j > 0; j--)
			if (crc & 1)
				crc = (crc >> 1) ^ CRC32_POLYNOMIAL;
			else
				crc >>= 1;
		CRCTable[i] = crc;
	}
	return 0;
}
/* changed long to int, void to const void - rusty. */
unsigned int crc32(const void *buffer, unsigned int count, unsigned int crc)
{
	unsigned int temp1, temp2;
	static int firsttime = 1;
	unsigned char *p = (unsigned char *)buffer;

	if (firsttime)
	{
		if (BuildCRCTable())
			return -1;
		firsttime = 0;
	}

	while (count-- != 0)
	{
		temp1 = (crc >> 8) & 0x00FFFFFF;
		temp2 = CRCTable[((int)crc ^ *p++) & 0xFF];
		crc = temp1 ^ temp2;
	}
	return crc;
}
