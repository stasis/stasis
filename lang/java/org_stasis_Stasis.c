#include "org_stasis_Stasis.h"
#include <stasis/transactional.h>
#include <stasis/page.h>

static jlongArray jlongArray_recordid(JNIEnv *e, recordid rid) {
  jlong l[3] = {rid.page, rid.slot, rid.size};
  jlongArray jla = (*e)->NewLongArray(e, 3);
  if((*e)->ExceptionOccurred(e)) return 0;
  (*e)->SetLongArrayRegion(e, jla, 0, 3, (jlong*)&l);
  if((*e)->ExceptionOccurred(e)) return 0;
  return jla;
}

static recordid recordid_jlongArray(JNIEnv *e, jlongArray jla) {
  jlong l[3];
  (*e)->GetLongArrayRegion(e, jla, 0, 3, (jlong*)&l);
  if((*e)->ExceptionOccurred(e))
    return NULLRID;
  recordid ret = {l[0], l[1], l[2]};
  return ret;
}

static byte * bytes_jbyteArray(JNIEnv *e, jbyteArray jba, size_t * sz) {
  *sz = (*e)->GetArrayLength(e, jba) * sizeof(byte);
  if((*e)->ExceptionOccurred(e)) return 0;
  assert(sizeof(jbyte) == 1);
  jbyte * ret = malloc(*sz);
  (*e)->GetByteArrayRegion(e, jba, 0, *sz, (jbyte*)ret);
  if((*e)->ExceptionOccurred(e)) return 0;
  return (byte*)ret;
}
static jbyteArray jbyteArray_bytes(JNIEnv *e, byte* b, size_t sz) {
  jbyteArray jba = (*e)->NewByteArray(e, sz);
  (*e)->SetByteArrayRegion(e, jba, 0, sz, (jbyte*)b);
  if((*e)->ExceptionOccurred(e)) return 0;
  return jba;
}
static int initted = 0;
JNIEXPORT jint JNICALL Java_stasis_jni_Stasis_init
  (JNIEnv *e, jclass c) {
  if(!initted) {
    Tinit();
  }
  initted++;
  return initted;
}
JNIEXPORT void JNICALL Java_stasis_jni_Stasis_deinit
  (JNIEnv *e, jclass c) {
  initted--;
  if(!initted) {
    Tdeinit();
  }
}
JNIEXPORT jlongArray JNICALL Java_stasis_jni_Stasis_root_1record
  (JNIEnv *e, jclass c) {
  return jlongArray_recordid(e, ROOT_RECORD);
}
JNIEXPORT jlong JNICALL Java_stasis_jni_Stasis_begin
  (JNIEnv *e, jclass c) {
  assert(initted);
  jlong xid = (jlong) Tbegin();
  return xid;
}
JNIEXPORT void JNICALL Java_stasis_jni_Stasis_commit
  (JNIEnv *e, jclass c, jlong xid) {
  DEBUG("commiting %lld\n", (long long)xid);
  Tcommit((int)xid);
}
JNIEXPORT void JNICALL Java_stasis_jni_Stasis_abort
  (JNIEnv *e, jclass c, jlong xid) {
  Tabort((int)xid);
}
JNIEXPORT void JNICALL Java_stasis_jni_Stasis_prepare
  (JNIEnv *e, jclass c, jlong xid) {
  Tprepare((int)xid);
}
JNIEXPORT jlong JNICALL Java_stasis_jni_Stasis_record_1type
  (JNIEnv *e, jclass c, jlong xid, jlongArray jlaRid) {
  recordid rid = recordid_jlongArray(e, jlaRid);
  if((*e)->ExceptionOccurred(e)) return 0;
  return (jlong)TrecordType(xid, rid);
}
JNIEXPORT jlongArray JNICALL Java_stasis_jni_Stasis_hash_1create
  (JNIEnv *e, jclass c, jlong xid) {
  return jlongArray_recordid(e,ThashCreate(xid, VARIABLE_LENGTH, VARIABLE_LENGTH));
}
/*  // ThashDelete is unimplemented...
JNIEXPORT void JNICALL Java_stasis_jni_Stasis_hash_1delete
  (JNIEnv *e, jclass c, jlong xid, jlongArray jlaRid) {
  ThashDelete(xid, recordid_jlongArray(e, jlaRid));
  } */
/* // no equivalent call in stasis api
JNIEXPORT jlong JNICALL Java_stasis_jni_Stasis_hash_1cardinality
  (JNIEnv *, jclass, jlong, jlongArray);
*/
JNIEXPORT jbyteArray JNICALL Java_stasis_jni_Stasis_hash_1insert
  (JNIEnv *e, jclass c, jlong xid, jlongArray jbarid, jbyteArray jbakey, jbyteArray jbaval) {
  recordid rid = recordid_jlongArray(e,jbarid);
  if((*e)->ExceptionOccurred(e)) return 0;
  size_t keylen;
  byte * key = bytes_jbyteArray(e,jbakey, &keylen);
  if((*e)->ExceptionOccurred(e)) return 0;
  size_t vallen;
  byte * val = bytes_jbyteArray(e,jbaval, &vallen);
  if((*e)->ExceptionOccurred(e)) return 0;
  byte * ret;
  int retsize = ThashLookup((int)xid,rid,key,keylen,&ret);
  jbyteArray jbaret;
  if(retsize == -1) {
    jbaret = 0;
  } else {
    jbaret = jbyteArray_bytes(e,ret,retsize);
  }
  if((*e)->ExceptionOccurred(e)) return 0;
  //  printf("Calling insert %lld %lld %lld %lx %lld %lx %lld (retsize  %lld)", (long long)xid, (long long)rid.page, (long long)rid.slot, (unsigned long)key, (long long) keylen, (unsigned long)val, (long long) keylen, (long long)retsize); fflush(0);
  ThashInsert((int)xid, rid, key, keylen, val, vallen);
  //  printf("Returned from insert"); fflush(0);
  return jbaret;
}

JNIEXPORT jbyteArray JNICALL Java_stasis_jni_Stasis_hash_1remove
  (JNIEnv *e, jclass c, jlong xid, jlongArray jbarid, jbyteArray jbakey) {

  recordid rid = recordid_jlongArray(e, jbarid);
  if((*e)->ExceptionOccurred(e)) return 0;

  size_t keylen;
  byte * key = bytes_jbyteArray(e,jbakey,&keylen);
  if((*e)->ExceptionOccurred(e)) return 0;

  byte * ret;
  int retsize = ThashLookup((int)xid,rid,key,keylen,&ret);

  jbyteArray jbaret;
  if(retsize == -1) {
    jbaret = 0;
  } else {
    jbaret = jbyteArray_bytes(e,ret,retsize);
  }
  if((*e)->ExceptionOccurred(e)) return 0;
  ThashRemove((int)xid,rid,key,keylen);
  return jbaret;
}

JNIEXPORT jbyteArray JNICALL Java_stasis_jni_Stasis_hash_1lookup
  (JNIEnv *e, jclass c, jlong xid, jlongArray jbarid, jbyteArray jbakey) {
  //  printf("in lookup");
  recordid rid = recordid_jlongArray(e, jbarid);
  if((*e)->ExceptionOccurred(e)) return 0;
  size_t keylen;
  byte * key = bytes_jbyteArray(e,jbakey,&keylen);
  if((*e)->ExceptionOccurred(e)) return 0;


  byte * ret;
  //  printf("calling thashlookup");
  int retsize = ThashLookup((int)xid,rid,key,keylen,&ret);
  //  printf("returned thashlookup");
  jbyteArray jbaret;
  if(retsize == -1) {
    jbaret = 0;
  } else {
    jbaret = jbyteArray_bytes(e,ret,retsize);
  }
  if((*e)->ExceptionOccurred(e)) return 0;
  return jbaret;
}
JNIEXPORT jbyteArray JNICALL Java_stasis_jni_Stasis_hash_1iterator
  (JNIEnv *e, jclass c, jlong xid, jlongArray rid) {
  lladdIterator_t * it = ThashGenericIterator(xid, recordid_jlongArray(e,rid));
  return jbyteArray_bytes(e,(byte*)&it,sizeof(it));
}
JNIEXPORT void JNICALL Java_stasis_jni_Stasis_iterator_1close
  (JNIEnv *e, jclass c, jlong xid, jbyteArray jbait) {
  size_t sz;
  lladdIterator_t** it = (lladdIterator_t**)bytes_jbyteArray(e,jbait,&sz);
  Titerator_close(xid,*it);
}
JNIEXPORT jboolean JNICALL Java_stasis_jni_Stasis_iterator_1next
  (JNIEnv *e, jclass c, jlong xid, jbyteArray jbait) {
  size_t sz;
  lladdIterator_t** it = (lladdIterator_t**)bytes_jbyteArray(e,jbait,&sz);
  return (jboolean)Titerator_next((int)xid, *it);
}
JNIEXPORT jbyteArray JNICALL Java_stasis_jni_Stasis_iterator_1key
  (JNIEnv *e, jclass c, jlong xid, jbyteArray jbait) {
  size_t sz;
  lladdIterator_t** it = (lladdIterator_t**)bytes_jbyteArray(e,jbait,&sz);

  byte * key;
  sz = Titerator_key(xid, *it, &key);

  return jbyteArray_bytes(e,key,sz);
}
JNIEXPORT jbyteArray JNICALL Java_stasis_jni_Stasis_iterator_1value
  (JNIEnv *e, jclass c, jlong xid, jbyteArray jbait) {
  size_t sz;
  lladdIterator_t** it = (lladdIterator_t**)bytes_jbyteArray(e,jbait,&sz);

  byte * val;
  sz = Titerator_value(xid, *it, &val);

  return jbyteArray_bytes(e,val,sz);
}
JNIEXPORT void JNICALL Java_stasis_jni_Stasis_iterator_1tuple_1done
  (JNIEnv *e, jclass c, jlong xid, jbyteArray jbait) {
  size_t sz;
  lladdIterator_t** it = (lladdIterator_t**)bytes_jbyteArray(e,jbait,&sz);

  Titerator_tupleDone(xid, *it);
}
