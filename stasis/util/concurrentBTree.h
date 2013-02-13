#include <stasis/common.h>

#define PAGE void*

typedef uint64_t metadata_t;

// TODO: These need to be packed!
typedef struct stasis_btree_index_page_header {
  metadata_t metadata;
  PAGE parent;
  pthread_rwlock_t sx_latch;
} stasis_btree_index_page;

typedef struct stasis_btree_data_page_header {
  metadata_t metadata;
  PAGE parent;
  PAGE rightSibling;
  PAGE leftSibling;
  pthread_rwlock_t latch;
} stasis_btree_data_page_header;

static inline byte metadata_is_leaf(metadata_t m) {
  return m & 0x1;
}
static inline metadata_t metadata_set_leaf(metadata_t m) {
  return m | 0x1;
}
static inline metadata_t metadata_clear_leaf(metadata_t m) {
  return m & ~0x1;
}

static inline byte metadata_is_balanced(metadata_t m) {
  return m & 0x2;
}
static inline byte metadata_set_balanced(metadata_t m) {
  return m | 0x2;
}
static inline byte metadata_clear_balanced(metadata_t m) {
  return m & ~0x2;
}
typedef enum {
  TEMP = 0,
  RED = 1,
  GREEN = 2,
  BLUE = 3
} color_t;

static inline color_t leaf_metadata_get_color(metadata_t m) {
  return (color_t)((m & (0x4 | 0x8)) >> 2);
}
static inline metadata_t leaf_metadata_set_color(metadata_t m, color_t c) {
  return (m & ~(0x4 | 0x8)) | (c << 2);
}

static inline int index_metadata_get_level(metadata_t m) {
  return (m & (0x4 | 0x8 | 0x10)) >> 2;
}
static inline metadata_t index_metadata_set_level(metadata_t m, int c) {
  return (m & ~(0x4 | 0x8 | 0x10)) | (c << 2);
}
