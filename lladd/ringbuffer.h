#include <lladd/transactional.h>

typedef struct ringBufferLog_s ringBufferLog_t;

ringBufferLog_t * openLogRingBuffer(size_t size, lsn_t initialOffset);
void closeLogRingBuffer(ringBufferLog_t * log);
int ringBufferAppend(ringBufferLog_t * log, byte * dat, size_t size);
int ringBufferTruncateRead(byte * buf, ringBufferLog_t * log,size_t size);
lsn_t ringBufferAppendPosition(ringBufferLog_t * log);
lsn_t ringBufferReadPosition(ringBufferLog_t * log);
