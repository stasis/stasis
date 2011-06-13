#include <stasis/common.h>
#include <stasis/experimental/ringbuffer.h>

//#define TRACK_OFFSETS

/* Could also use mod here, but 64-bit mod under intel is kinda slow,
   and (in theory) gcc should only calculate ((lsn)-(x)->start) one
   time...besides, I implemented it this way before I thought of using mod. ;) */

/*#define lsn_to_offset(x, lsn) \
   ((((lsn)-(x)->start) < (x)->size) ? \
     ((lsn)-(x)->start) :              \
     ((lsn)-(x)->start) - (x)->size); */

#undef end
struct ringBufferLog_s {
  /** An array of bytes that holds the contents of the ringbuffer. */
  byte * buf;
  /** The number of bytes in the ringbuffer */
  unsigned int size;
  /** The first byte in the ringbuffer that is valid. */
  lsn_t start;
  /** The last byte in the ringbuffer that is valid.  Note that due to
      the nature of a ringbuffer, end may be less than start.  This
      simply means that the ring buffer wraps around the end of
      buf. */
  lsn_t end;
#ifdef TRACK_OFFSETS
  /** The offset of the first byte in the ring buffer.  Ignoring
      wrap-around, lsn(buf[i]) = offset + (i-start). */
  lsn_t offset;
#endif
} ringBufferLog_s;


#define lsn_to_offset(x, lsn) ((lsn) % (x)->size)

#ifdef TRACK_OFFSETS
#define offset_to_lsn(x, lsn) ((lsn) + (x)->offset)
#endif

static int stasis_ringbuffer_truncate(ringBufferLog_t * log, lsn_t lsn);

ringBufferLog_t * openLogRingBuffer(size_t size, lsn_t initialOffset) {
  ringBufferLog_t * ret = malloc(sizeof(ringBufferLog_t));
  ret->buf = malloc(size);
  ret->size = size;
  ret->start = initialOffset % size;
  ret->end   = initialOffset % size;

#ifdef TRACK_OFFSETS
  ret->offset= initialOffset / size;
#endif
  return ret;
}

void closeLogRingBuffer(ringBufferLog_t * log) {
  free(log->buf);
  free(log);
}
/**
    This function copies size bytes from the ringbuffer at offset
    'offset'.  size must be less than log->size.

    It probably also should lie within the boundaries defined by start
    and end, but this is optional.
*/
static void memcpyFromRingBuffer(byte * dest, ringBufferLog_t * log, lsn_t lsn, size_t size) {
  lsn_t offset = lsn_to_offset(log, lsn);
  if(offset + size < log->size) {
    memcpy(dest, &(log->buf[offset]), size);
  } else {
    int firstPieceLength = log->size - offset;
    int secondPieceLength = size - firstPieceLength;
    memcpy(dest, &(log->buf[offset]), firstPieceLength);
    memcpy(dest + firstPieceLength, &(log->buf[0]), secondPieceLength);
  }
}

static void memcpyToRingBuffer(ringBufferLog_t * log, byte *src, lsn_t lsn, size_t size) {
  int offset = lsn_to_offset(log, lsn);
  if(offset + size < log->size) {
    memcpy(&(log->buf[offset]), src, size);
  } else {
    int firstPieceLength = log->size - offset;
    int secondPieceLength = size - firstPieceLength;
    memcpy(&(log->buf[offset]), src, firstPieceLength);
    memcpy(&(log->buf[0]), src + firstPieceLength, secondPieceLength);
  }
}
/** @todo Return values for ringBufferAppend! */

int ringBufferAppend(ringBufferLog_t * log, byte * dat, size_t size) {

  assert(lsn_to_offset(log, log->end + size)  == (lsn_to_offset(log, log->end + size)));

  if(size > log->size) {
    //    printf("!");
    return -1;       // the value cannot possibly fit in the ring buffer.
  }

  if(log->size < (log->end-log->start) + size) {
    //    printf("[WX]");
    return -2;       // there is not enough room in the buffer right now.
  }

  memcpyToRingBuffer(log, dat, log->end, size);
  log->end += size; // lsn_to_offset(log, log->end + size);

  return 0;

}

lsn_t ringBufferAppendPosition(ringBufferLog_t * log) {
  return log->end;
}

lsn_t ringBufferReadPosition(ringBufferLog_t * log) {
  return log->start;
}

int ringBufferTruncateRead(byte * buf, ringBufferLog_t * log, size_t size) {
  if(size > log->size) {
    return -1;       // Request for chunk larger than entire ringbuffer
  }
  if(log->start + size > log->end) {
    //    printf("[RX]");
    return -2;
  }
  memcpyFromRingBuffer(buf, log, lsn_to_offset(log, log->start), size);

  return stasis_ringbuffer_truncate(log, log->start + size);

}

/** static because it does no error checking. */
static int stasis_ringbuffer_truncate(ringBufferLog_t * log, lsn_t lsn) {

#ifdef TRACK_OFFSETS
  lsn_t newStart = lsn_to_offset(log, lsn);

  if(newStart < lsn_to_offset(log, log->start)) {  // buffer wrapped.
    log->offset += log->size;
  }
#endif

  log->start = lsn;

  return 0;

}
