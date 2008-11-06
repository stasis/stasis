#include <stasis/transactional.h>

#define FUSE_USE_VERSION 26
#include <fuse/fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <libgen.h> // for basename, dirname.
#include <fcntl.h>

typedef struct {
  struct stat s; // what type is this entry?
} stasis_dir_entry;

static int stasis_lookup_helper(const char *path, stasis_dir_entry** subentry) {
  return ThashLookup(-1, ROOT_RECORD, (byte*)path, strlen(path)+1,
                     (byte**)subentry);
}

static int stasis_link_dirent(int xid, const char * path) {
  char * parentbuf = strdup(path);
  char * parent = dirname(parentbuf);
  printf("attaching to parent %s\n", parent); fflush(stdout);
  char * filebuf = strdup(path);
  char * file = basename(filebuf);
  stasis_dir_entry * entry;
  printf("lu ->%s<- %d\n", parent, (int)strlen(parent)+1);fflush(stdout);
  int entrylen = ThashLookup(xid, ROOT_RECORD,
                             (const byte*)parent, strlen(parent)+1,
                             (byte**)&entry);
  printf("entrylen %d\n",entrylen); fflush(stdout);
  if(entrylen == -1) {
    return -ENOENT;
  } else {
    const char * next;
    for(next = (const char*)(entry+1);
        next != ((char*)entry) + entrylen;
        next += (strlen(next)+1)) {
      assert(next < ((char*)entry)+entrylen);
      printf("next: %s\n", next); fflush(stdout);fflush(stdout);
      if(0 == strcmp(next, path)) {
        break;
      }
    }
    printf("forloop done\n"); fflush(stdout);
    if(next == ((char*)entry)+entrylen) {
      printf("inserting %s\n", path); fflush(stdout);
      int newentrylen = entrylen+strlen(file)+1;

      entry = realloc(entry, newentrylen);
      strcpy(((char*)entry)+entrylen,file);

      printf("parent ->%s<- %d; newlen=%d\n", parent, (int)strlen(parent)+1,
             newentrylen);fflush(stdout);

      ThashInsert(xid, ROOT_RECORD, (const byte*)parent, strlen(parent)+1,
                  (const byte*)entry, newentrylen);
    }
  }
  printf("returning 0\n"); fflush(stdout);
  return 0;
}

static int stasis_unlink_dirent(int xid, const char * path) {

  printf("in unlink\n"); fflush(stdout);

  int res = 0;

  char * pathcpy1 = strdup(path);
  char * pathcpy2 = strdup(path);

  char * base = basename(pathcpy1);
  char * dir  = dirname(pathcpy2);

  // remove from parent
  stasis_dir_entry * parent;
  ssize_t parentsz = ThashLookup(xid, ROOT_RECORD,
                                 (const byte*)dir,strlen(dir)+1,
                                 (byte**)&parent);
  assert(parentsz != -1);

  size_t newentsz = parentsz - (strlen(base)+1);

  byte * newent = malloc(newentsz);
  memcpy(newent, parent, sizeof(*parent));
  byte * newnext = newent + sizeof(*parent);
  int found = 0;
  for(char* next = (char*)(parent+1);
      next != ((char*)parent)+parentsz;
      next += (1+strlen(next))) {
    assert(next < ((char*)parent)+parentsz);
    printf("%s %s\n", base, next);
    if(strcmp(base,next)) {
      assert(((char*)newnext + strlen(next)+1) <= ((char*)newent)+newentsz);
      strcat((char*)newnext,next);
      newnext+=(1+strlen(next));
    } else {
      found++;
    }
  }
  assert(found == 1);
  ThashInsert(xid, ROOT_RECORD, (const byte*)dir, strlen(dir)+1,
              newent, newentsz);
  free(pathcpy1);
  free(pathcpy2);
  free(parent);
  free(newent);
  return res;
}
static int stasis_mknod_helper(int xid, const char * path,
                               stasis_dir_entry * dir, int sz, int parent) {
  int res = 0;
  printf("mk ->%s<- %d\n", path, (int)strlen(path)+1);fflush(stdout);
  if(ThashInsert(xid, ROOT_RECORD, (const byte*)path, strlen(path)+1, (byte*)dir, sz)) {
    // value existed.  set error so xactn will abort.
    res = -EEXIST;
  } else {
    if(parent) {
      res = stasis_link_dirent(xid, path);
    }
  }
  return res;
}

/** removes directory entry, without checking for data, subdirs etc... */
static int stasis_rmnod_helper(int xid, const char *path, int parent) {
  printf("in rmnod\n"); fflush(stdout);
  int res = 0;
  if(!ThashRemove(xid, ROOT_RECORD, (const byte*)path, strlen(path)+1)) {
    res = -ENOENT;
  } else {
    if(parent) {
      res = stasis_unlink_dirent(xid, path);
    }
  }
  return res;
}

// ------ Fuse operations ---------------------------------------------

static int stasis_getattr(const char *path, struct stat *stbuf)
{
    int res = 0;

    memset(stbuf, 0, sizeof(struct stat));

    stasis_dir_entry * e;
    int sz = stasis_lookup_helper(path, &e);
    if(sz == -1) {
      res = -ENOENT;
    } else {
      memcpy(stbuf, &(e->s), sizeof(*stbuf));
      free(e);
    }
    return res;
}

static stasis_dir_entry* malloc_nod(int*sz) {
  *sz = sizeof(stasis_dir_entry);
  return calloc(1, sizeof(stasis_dir_entry));
}


static int stasis_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                         off_t offset, struct fuse_file_info *fi)
{
    stasis_dir_entry * entry;
    printf("lu ->%s<- %d\n", path, (int)strlen(path)+1);fflush(stdout);
    int entrylen = ThashLookup(-1, ROOT_RECORD, (byte*)path, strlen(path)+1,(byte**)&entry);
    printf("readdir %s %d\n", path, entrylen); fflush(stdout);

    int res;

    if(entrylen == -1) {
      res = -ENOENT;
    } else {
      if(entry->s.st_mode & S_IFDIR) { // dir

        filler(buf, ".", &(entry->s), 0); // xxx offsets
        filler(buf, "..", NULL, 0);

        for(char* next = (char*)(entry+1);
            next != ((char*)entry) + entrylen;
            next += (strlen(next)+1)) {
          assert(next < ((char*)entry)+entrylen);
          printf("found entry %s\n", next); fflush(stdout);

          char * subentrypath = malloc(strlen(next)+strlen(path)+1);
          if(0==strcmp(path,"/")) {
            subentrypath = malloc(strlen(next)+strlen(path)+1);
            strcpy(subentrypath,path);
            strcat(subentrypath,next);
          } else {
            subentrypath = malloc(strlen(next)+strlen(path)+2);
            strcpy(subentrypath,path);
            strcat(subentrypath,"/");
            strcat(subentrypath,next);
          }
          stasis_dir_entry * subentry;
          int subentry_len = stasis_lookup_helper(subentrypath,&subentry);
          if(subentry_len == -1) { abort(); }

          filler(buf, next, &(subentry->s), 0);
          free(subentry);
        }
      } else {
        res = -ENOTDIR;
      }
    }

    free(entry);
    return 0;
}

static int stasis_open(const char *path, struct fuse_file_info *fi)
{
  int res = 0;

  stasis_dir_entry * entry;
  int entrylen = ThashLookup(-1, ROOT_RECORD,
                             (const byte*)path, strlen(path)+1,
                             (byte**)&entry);

  if(entrylen != -1) {
    // XXX check permissions?
    if(entry->s.st_mode & S_IFDIR) {
      printf("found a directory entry %s\n",path); fflush(stdout);
      res = -EISDIR;
    } else if(entry->s.st_mode & S_IFREG) {
      printf("found and regular entry %s\n",path); fflush(stdout);
      res = 0;
    } else { // XXX do something else?
      res = 0;
    }
    free(entry);

  } else {
    res = -ENOENT;
  }
  return res;
}

static int stasis_write(const char *path, const char *buf, const size_t sz, const off_t off,
                        struct fuse_file_info * fi) {
  stasis_dir_entry *e;
  int xid = Tbegin();
  int res = 0;

  int entsz = ThashLookup(xid, ROOT_RECORD, (const byte*)path, strlen(path)+1, (byte**)&e);

  if(entsz == -1) {
    res = -ENOENT;
  } else {
    assert(entsz > sizeof(stasis_dir_entry));
    if(S_ISREG(e->s.st_mode)) {
      assert(entsz == (sizeof(stasis_dir_entry)+sizeof(recordid)));
      recordid * blob_rid = (recordid*)(e+1);
      int updateRid = 0;
      if(blob_rid->page == NULLRID.page &&
         blob_rid->slot == NULLRID.slot) {
        // alloc blob
        updateRid = 1;
        *blob_rid = Talloc(xid, sz+off);
        char* realbuf;
        if(off) {
          realbuf = calloc(sz+off,sizeof(char));
          memcpy(realbuf+off,buf,sz);
        } else {
          realbuf = (char*)buf;
        }
        Tset(xid, *blob_rid, realbuf);
        if(off) {
          free(realbuf);
        }
      } else {
        // memcpy old blob into buffer, free old blob, alloc + write new blob, update hash
        size_t oldlen = TrecordSize(xid, *blob_rid);
        if(oldlen >= sz+off) {
          byte * tmp = malloc(oldlen);
          Tread(xid, *blob_rid, tmp);
          memcpy(tmp+off,buf,sz);
          Tset(xid, *blob_rid, tmp);
          free(tmp);
        } else {
          byte * tmp = calloc(sz+off,sizeof(char));
          Tread(xid, *blob_rid, tmp);
          memcpy(tmp+off,buf,sz);
          Tdealloc(xid, *blob_rid);

          updateRid = 1;
          *blob_rid = Talloc(xid, off+sz);
          Tset(xid,*blob_rid,tmp);

          free(tmp);
        }
      }
      if(updateRid) {
        e->s.st_size = off+sz;
        ThashInsert(xid, ROOT_RECORD, (const byte*)path, strlen(path)+1, (byte*)e,
                    sizeof(stasis_dir_entry)+sizeof(recordid));
      }
    } else {
      res = -ENOTSUP;
    }
  }
  if(!res) {
    res = sz;
    Tcommit(xid);
  } else {
    Tabort(xid);
  }
  return res;
}

static int stasis_read(const char *path, char *buf, size_t size, off_t offset,
                      struct fuse_file_info *fi)
{
  int res = 0;
  stasis_dir_entry *e;
  int entsz = ThashLookup(-1, ROOT_RECORD, (const byte*)path, strlen(path)+1, (byte**)&e);

  if(entsz == -1) {
    res = -ENOENT;
  } else {
    assert(entsz > sizeof(stasis_dir_entry));
    if(S_ISREG(e->s.st_mode)) {
      assert(entsz == (sizeof(stasis_dir_entry)+sizeof(recordid)));
      recordid * blob_rid = (recordid*)(e+1);
      if(size+offset > e->s.st_size) {
        if(offset > e->s.st_size) {
          size = 0;
        } else {
          size = e->s.st_size-offset;
        }
      }
      if(size) {
        byte * tmp = malloc(e->s.st_size);
        Tread(-1, *blob_rid, tmp);
        memcpy(buf, tmp+offset, size);
        free(tmp);
      }
      res = size;
    } else {
      res = -ENOTSUP;
    }
  }
  return res;
}
static int stasis_unlink(const char * path) {
  int res = 0;
  stasis_dir_entry *ent;

  int entsz = ThashLookup(-1, ROOT_RECORD, (const byte*)path, strlen(path)+1, (byte**)&ent);

  if(entsz == -1) {
    res = -ENOENT;
  } else {
    assert(entsz >= sizeof(stasis_dir_entry));
    if(S_ISREG(ent->s.st_mode)) {
      assert(entsz == (sizeof(stasis_dir_entry)+sizeof(recordid)));
      recordid * blob_rid = (recordid*)(ent+1);
      int xid = Tbegin();
      if(blob_rid->page != NULLRID.page || blob_rid->slot != NULLRID.slot) {
        Tdealloc(xid, *blob_rid);
      }
      res = stasis_rmnod_helper(xid,path,1);
      if(res) {
        Tabort(xid);
      } else {
        Tcommit(xid);
      }
    } else {
      // XXX check for block devices, etc...
      res = -EISDIR;
    }
    free(ent);
  }
  return res;
}
static int stasis_rmdir(const char *path) {
  int res = 0;
  stasis_dir_entry *ent;

  int entsz = ThashLookup(-1, ROOT_RECORD, (const byte*)path, strlen(path)+1, (byte**)&ent);

  if(entsz == -1) {
    res = -ENOENT;
  } else {
    assert(entsz >= sizeof(stasis_dir_entry));
    if(S_ISDIR(ent->s.st_mode)) {
      if(entsz > sizeof(stasis_dir_entry)) {
        res = -ENOTEMPTY;
      } else {
        int xid = Tbegin();
        res = stasis_rmnod_helper(xid,path,1);
        if(res) {
          Tabort(xid);
        } else {
          Tcommit(xid);
        }
      }
    } else {
      res = -ENOTDIR;
    }
  }
  return res;
}
static int stasis_mknod(const char *path, mode_t mode, dev_t dev) {
  int res = 0;
  int xid = Tbegin();

  int sz;
  stasis_dir_entry * dir = malloc_nod(&sz);
  dir->s.st_nlink = 1;
  dir->s.st_mode = mode & 0777;
  assert(!(dir->s.st_mode & S_IFDIR));

  if(S_ISREG(mode)) {

    dir->s.st_mode |= S_IFREG;
    //    dir->s.st_uid = 0;
    //    dir->s.st_gid = 0;
    //    dir->s.st_blksize = 4096; //USABLE_SIZE_OF_PAGE;
    //    dir->s.st_blocks = 1;

    sz += sizeof(recordid);
    dir = realloc(dir, sz);
    *(recordid*)(dir+1) = NULLRID;

  } else if(S_ISCHR(mode)) {
    //XXX
    res = -ENOTSUP;
  } else if(S_ISBLK(mode)) {
    //XXX
    res = -ENOTSUP;
  } else if(S_ISFIFO(mode)) {
    //XXX
    res = -ENOTSUP;
  } else if(S_ISSOCK(mode)) {
    //XXX
    res = -ENOTSUP;
  } else {
    printf("invalid dev parameter to mknod: %ld", (long)dev); fflush(stdout);
    res = -EINVAL;
  }
  if(!res) {
    res = stasis_mknod_helper(xid,path,dir,sz,1);
  }
  if(!res) {
    Tcommit(xid);
  } else {
    Tabort(xid);
  }
  free(dir);
  return res;
}

static int stasis_mkdir(const char *path, mode_t mode) {
  int xid = Tbegin();

  int sz;
  stasis_dir_entry * dir = malloc_nod(&sz);

  dir->s.st_mode = S_IFDIR | mode;
  dir->s.st_nlink = 2;
  //    dir->s.st_uid = 0;
  //    dir->s.st_gid = 0;
  //    dir->s.st_blksize = 4096; //USABLE_SIZE_OF_PAGE;
  //    dir->s.st_blocks = 1;
  int ret = stasis_mknod_helper(xid,path,dir,sz,1);
  free(dir);
  if(ret) {
    Tabort(xid);
  } else {
    Tcommit(xid);
  }
  return ret;
}

static void* stasis_init(struct fuse_conn_info *conn) {
  Tinit();

  int xid = Tbegin();

  if(TrecordType(xid, ROOT_RECORD) == INVALID_SLOT) {
    recordid rootEntry = ThashCreate(xid, VARIABLE_LENGTH, VARIABLE_LENGTH);
    assert(ROOT_RECORD.page == rootEntry.page);
    assert(ROOT_RECORD.slot == rootEntry.slot);

    int sz;
    stasis_dir_entry * dir = malloc_nod(&sz);

    dir->s.st_mode = S_IFDIR | 0755; // XXX mode?
    dir->s.st_nlink = 2;
    //    dir->s.st_uid = 0;
    //    dir->s.st_gid = 0;
    //    dir->s.st_blksize = 4096; //USABLE_SIZE_OF_PAGE;
    //    dir->s.st_blocks = 1;
    stasis_mknod_helper(xid, "/", dir, sz, 0);
  }
  Tcommit(xid);

  // XXX xid caching
  return 0;
}
static void stasis_destroy(void * parm) {
  Tdeinit();
}

static struct fuse_operations stasis_oper = {
    .getattr	= stasis_getattr,
  //.readlink   = stasis_readlink,
    .mknod      = stasis_mknod,
    .mkdir      = stasis_mkdir,
    .unlink     = stasis_unlink,
    .rmdir      = stasis_rmdir,
  //.symlink    = stasis_symlink,
  //.rename     = stasis_rename,
  //.link       = stasis_link,
  //.chmod      = stasis_chmod,
  //.chown      = stasis_chown,
  //.truncate   = stasis_truncate,
  //.utime      = stasis_utime,
    .open	= stasis_open,
    .read	= stasis_read,
    .write      = stasis_write,
  //.statfs     = stasis_statfs,
  //.flush      = stasis_flush,
  //.release    = stasis_release,
  //.fsync      = stasis_fsync,
  //.setxattr   = stasis_setxattr,
  //.getxattr   = stasis_getxattr,
  //.listxattr  = stasis_listxattr,
  //.removexattr= stasis_removexattr,
  //.opendir    = stasis_opendir,
    .readdir	= stasis_readdir,
  //.releasedir = stasis_releasedir,
  //.fsyncdir   = stasis_fsyncdir,
    .init       = stasis_init,
    .destroy    = stasis_destroy,
  //.access     = stasis_access,
  //.ftruncate  = stasis_ftruncate,
  //.lock       = stasis_lock,
  //.utimens    = stasis_utimens,
  //.bmap       = stasis_bmap,
};


int main(int argc, char *argv[])
{
  int ret = fuse_main(argc, argv, &stasis_oper, 0);
  return ret;
}
