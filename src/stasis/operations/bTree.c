/*
 * bTree.c
 *
 *  Created on: Jul 26, 2009
 *      Author: sears
 */
#include<stasis/operations/bTree.h>

#include <stasis/bufferManager.h>

static stasis_comparator_t * btree_comparators;

static int stasis_btree_byte_array_comparator(const void * a, size_t alen, const void * b, size_t blen, void* arg) {
  size_t len = alen;
  if(len > blen) { len = blen; }
  if(arg && len > (uintptr_t) arg) { len = (uintptr_t) arg; }
  if(len == 1) { DEBUG("%c <-> %c", *(char*)a, *(char*)b); }
  if(len == 4) { DEBUG("%d <-> %d", *(int*)a, *(int*)b); }
//  DEBUG("%lld %lld %lld %lld", (long long) arg, alen, blen, len);
  int ret = memcmp(a, b, len);
  if(!(ret || arg)) {
    ret = (alen < blen) ? -1 : ((alen > blen) ? 1 : 0);
  }
  DEBUG(" = %d\n", ret);
  return ret;
}

void BtreeInit() {
  // todo: register iterator

  // register comparators
  btree_comparators = calloc(MAX_COMPARATOR, sizeof(stasis_comparator_t));
  btree_comparators[BYTE_ARRAY_COMPARATOR] = stasis_btree_byte_array_comparator;

}
void BtreeDeinit() {
  free(btree_comparators);
}
typedef struct {
  int height;
  pageid_t root;
  stasis_comparator_id_t cmp_id;
} stasis_op_btree_page_header;

typedef struct {
  pageid_t page;
} btree_internal_pair;

typedef struct {
  uint16_t keylen;
} btree_leaf_pair;

recordid TbtreeCreate(int xid, stasis_comparator_id_t cmp_id) {
  stasis_op_btree_page_header header;
  header.root = TpageAlloc(xid);
  TinitializeSlottedPage(xid, header.root);

  header.height = 1; // leaf
  header.cmp_id = cmp_id;
  recordid rid = Talloc(xid,  sizeof(header));
  Tset(xid, rid, &header);
  return rid;
}

static int cmp_slot(int xid, Page *p, slotid_t slot, byte * key, size_t keySize, int leaf, stasis_comparator_t cmp, void * cmp_arg) {
  recordid rid = {p->id, slot, 0};
  rid.size = stasis_record_length_read(xid, p, rid);
  if(rid.size == INVALID_SLOT) { return 1; } // treat invalid slots as infinity.
  byte * cur = stasis_malloc(rid.size, byte);
  stasis_record_read(xid, p, rid, cur);
  byte * cur_ptr;
  size_t cur_len;
  if(leaf) {
    cur_ptr = (byte*)(1+(btree_leaf_pair*)cur);
    cur_len = ((btree_leaf_pair*)cur)->keylen;
  } else {
    cur_ptr = (byte*)(1+(btree_internal_pair*)cur);
    cur_len = stasis_record_length_read(xid, p, rid)-sizeof(btree_internal_pair);
  }
  int res = cmp(key, keySize, cur_ptr, cur_len, cmp_arg);
  free(cur);
  return res;
}

static slotid_t find_in_page(int xid, Page * p, byte * key, size_t keySize, int leaf, int* found, stasis_comparator_t cmp, void * cmp_arg) {
  slotid_t slot;
  int res = 1;
  recordid lastrid = stasis_record_last(xid, p);
  for(slot = 0; slot <= lastrid.slot; slot++) {
    res = cmp_slot(xid, p, slot, key, keySize, leaf, cmp, cmp_arg);
    if(res <= 0) { break; }  // if the key is less than or equal to the slot, then stop looking.
  }
  if(res == 0) {
    // have the right slot.
    *found = 1;
  } else {
    // have the slot after the one we want, or are at edge of page.
    *found = 0;
    if(slot > lastrid.slot) {
      DEBUG("end of page\n");
      // ret is the slot we want.
    } else if(slot == 0) {
      DEBUG("start of page\n");
    } else {
      slot --;
      DEBUG("too far\n");
    }
  }
  DEBUG("found = %d, res = %d, ret = %lld\n", *found, res, slot);
  return slot;
}

static slotid_t stasis_btree_helper(int xid, stasis_op_btree_page_header h, byte* key, size_t keySize,
    int * found, pageid_t ** path, stasis_comparator_t cmp, void * cmp_arg) {
  pageid_t next = h.root;
  *path = calloc(h.height, sizeof(pageid_t));
  for(int i = 0; i < h.height-1; i++) {
    Page *p = loadPage(xid, next);
    (*path)[i] = next;
    readlock(p->rwlatch,0);
    int ignored;
    next = find_in_page(xid, p, key, keySize, 0, &ignored, cmp, cmp_arg);
    unlock(p->rwlatch);
    releasePage(p);
  }
  // leaf page
  Page * p = loadPage(xid, next);
  (*path)[h.height-1] = next;
  DEBUG("leaf: %lld\n", next);
  readlock(p->rwlatch, 0);
  slotid_t slot = find_in_page(xid, p, key, keySize, 1, found, cmp, cmp_arg);
  DEBUG("slot: %lld (found = %d)\n", slot, *found);
  unlock(p->rwlatch);
  releasePage(p);
  return slot;
}
int TbtreeLookup(int xid, recordid rid, void * cmp_arg, byte * key, size_t keySize, byte ** value, size_t* valueSize) {
  stasis_op_btree_page_header h;
  Tread(xid, rid, (byte*)&h);
  pageid_t * path;
  int found;
  slotid_t slot = stasis_btree_helper(xid, h, key, keySize, &found, &path, btree_comparators[h.cmp_id], cmp_arg);
  recordid slotrid = { path[h.height-1], slot, 0 };
  free(path);
  if(found) {
    Page * p = loadPage(xid, slotrid.page);
    readlock(p->rwlatch, 0);
    slotrid.size = stasis_record_length_read(xid, p, slotrid);
    btree_leaf_pair * buf = malloc(slotrid.size);
    stasis_record_read(xid, p, slotrid, (byte*)buf);
    *valueSize = slotrid.size - (buf->keylen + sizeof(btree_leaf_pair));
    *value = stasis_malloc(*valueSize, byte);
    memcpy(*value, ((byte*)(buf+1))+buf->keylen, *valueSize);
    unlock(p->rwlatch);
    releasePage(p);
  } else {
    *value = 0;
    *valueSize = INVALID_SLOT;
  }
  return found;
}
int TbtreeInsert(int xid, recordid rid, void *cmp_arg, byte *key, size_t keySize, byte *value, size_t valueSize) {
  stasis_op_btree_page_header h;
  Tread(xid, rid, (byte*)&h);
  pageid_t * path;
  int found;
  slotid_t slot = stasis_btree_helper(xid, h, key, keySize, &found, &path, btree_comparators[h.cmp_id], cmp_arg);
  recordid slotrid = {path[h.height-1], slot, 0};
  Page *p = loadPage(xid, slotrid.page);
  writelock(p->rwlatch,0);
  if(found) {
    // delete old value
    stasis_record_free(xid, p, slotrid);
    stasis_record_compact_slotids(xid, p); // could do better with different api
  }
  size_t sz = sizeof(btree_leaf_pair) + keySize + valueSize;
  btree_leaf_pair *buf = malloc(sz);
  buf->keylen = keySize;
  memcpy(buf+1, key, keySize);
  memcpy(((byte*)(buf+1))+keySize, value, valueSize);
  recordid newrid = stasis_record_alloc_begin(xid, p, sz);
  if(newrid.size != sz) {
    // split leaf into two halves (based on slot count)
    //pageid_t leftpage = p->id;
    pageid_t rightpage = TpageAlloc(xid);
    TinitializeSlottedPage(xid, rightpage);
    Page * rightp = loadPage(xid, rightpage);
    writelock(rightp->rwlatch,0);
    const recordid lastrid = stasis_record_last(xid, p);
    for(slotid_t i = lastrid.slot / 2; i <= lastrid.slot; i++) {
      recordid leftrid = {p->id, i, 0};
      leftrid.size = stasis_record_length_read(xid, p, leftrid);
      recordid rightrid = stasis_record_alloc_begin(xid, rightp, leftrid.size);
      stasis_record_alloc_done(xid, rightp, rightrid);
      byte * buf = stasis_record_write_begin(xid, rightp, rightrid);
      stasis_record_read(xid, p, leftrid, buf);
      stasis_record_write_done(xid, rightp, rightrid, buf);
      stasis_record_free(xid, p, leftrid);
    }
    stasis_record_compact(p);
    stasis_record_compact_slotids(xid, p);
    if(slotrid.slot < lastrid.slot / 2) {
      // put into left page (p)
      newrid = stasis_record_alloc_begin(xid, p, sz);
    } else {
      // put into right page(rightp)
      slotrid.slot -= (lastrid.slot/2);
      Page * swpp = p;
      p = rightp;
      rightp = swpp;
      slotrid.slot = p->id;
      newrid = stasis_record_alloc_begin(xid, p, sz);
    }
    unlock(rightp->rwlatch);
    releasePage(rightp);
    int next_to_split = h.height - 2;  // h.height-1 is the offset of the leaf, which we just split.
    while(next_to_split >= 0) {
      // insert value into intermediate node.  Break out of loop if the value fit...
    }
    // need new root.
    h.height++;
    h.root = TpageAlloc(xid);
    TinitializeSlottedPage(xid, h.root);
    TallocFromPage(xid, h.root, -1/*oldsz*/);
    // TODO now what?!?
  }
  stasis_record_alloc_done(xid, p, newrid);
  stasis_record_write(xid, p, newrid, (byte*)buf);
  stasis_record_splice(xid, p, slotrid.slot, newrid.slot);
  DEBUG("created new record: %lld %d -> %d %d\n", newrid.page, newrid.slot, slotrid.slot, newrid.size);
  free(path);
  unlock(p->rwlatch);
  releasePage(p);
  return found;
}
