/**
  concurrenthash.c

  @file implementation of a concurrent, fixed-size hashtable


============================
Discussion of implementation
============================

Before reading the rest of this (giant) comment, you probably should read the
implementation, as it is shorter, and the text below assumes an understanding
of the API, and the general idea behind the algorithm.  If you just want to use
the hashtable (or haven't yet looked at it), you should read concurrentHash.h
for a discussion of this data structure's API, and non-standard concurrency
primitives.

This concurrent hash table implementation completely avoids the need for
global latches, but is incapable of resizing itself at runtime.  It is based
upon three ideas:

 - Islands, which we use to implement the NEAR primitive from navigational
   databases of lore.

 - Modular arithmetic and two's complement, which we use to precisely define
   the concepts of "before" and "after" that the island implementation relies
   upon.

 - Lock (latch) crabbing and linearizability, which form the basis of the
   argument that the concurrency control protocol is both live and safe.

Implementing NEAR with islands
==============================

Ignoring wrapping and locking for a moment, inserting, looking up and
removing an element from the hash table can be summarized as follows.
Let us call each sequence of non-null elements in the hash table an
"island".  When looking up an entry in the hashtable, we start at the element
the key hashes to.  If this element matches, we return it.  If it is null,
then we return null.  Otherwise, we repeat the process at the next entry.  It
is easy to see that we will find a element that hashes to element N iff it is
in the same island as N, and in a position that is >= N.

This trick is extremely old and general-purpose; navigational databases
actually implemented the primitives described here, and allowed applications to
request that some key K be placed NEAR some other key K', then lookup K using
the key K'.

Insertion in this scheme is straightforward.  We simply start at entry N,
then move to N+1, N+2 and so on until we find a null entry.  We store the
element in this entry and return.  Clearly, the entry is in the same island
as N, and is in a position >= N.

Removing elements is more complicated.  When removing an element from an
island, we will either replace it with a null, or replace it with a copy of an
element that resides in a different bucket, then delete the entry from the
second bucket.

Replacing an element with null breaks an island. Say, we are removing C:

null,A,B,C,D,E,F,null -> null,A,B,null,D,E,F,null

This is OK to do as long D,E and F do not hash to position of A,B or C.
Otherwise, they will no longer be discoverable.

The alternative is to replace C with some other element from the same island,
say E, and then apply the same algorithm to delete E:

null,A,B,C,D,E,F,null -> null,A,B,E,D,?,F,null -> ...

this is OK to do as long as E hashes to the position of A,B or C.

These two observations lead to the following algorithm. Say we need to
remove an element at position P:

Keep checking elements at position P+1, P+2 and so on until either:
  A. We discover a null element, or
  B. We discover an element that hashes to position P' <= P

In case A, we know that all elements in (P,P') are mapped to positions
that are > P. Therefore, breaking the island at position P by replacing
P with null is allowed. So we do exactly that.

In case B, we should move the element from position P' to position P and
then apply the same deletion algorithm to position P'. This is OK
because if element was discoverable at position P', it will remain
discoverable at position P. Lets prove it. Say P' hashes to position I.
We know that I <= P due to condition B. Since P' was discoverable before
transformation, and because P' >= P, we know [I,P] contains only non-null
elements. So, the search for that element will iterate starting from I
until it finds element at position P.

Dealing with wraparound
=======================

The wraparound is actually not complicating things too much as long as
we guarantee that the number of items in the hash is less than 50% of
total hash capacity; to decide whether A is before or after B, we see
whether there are more buckets in the range (A,B), or in the range (B,A).
Wraparound guarantees that both ranges are well-defined.

Once we get above 50% occupancy, it gets hard to figure out which position is
"greater" than the other one as we can theoretically have an island that is
1/2 as long as the hash table.  Based on the definition in the previous
paragraph, the "head" of such an island would be "after" its tail.  We could
circumvent the problem by walking the entire island to see whether or not a
null occurs between the head and an entry of interest, but this would incur
O(n) overhead, and dramatically increase the number of latches held by any
given thread.

At any rate, hashtables with over 50% utilization are likely to have poor
performance characteristics.  Rather than risk poor CPU utilization, users
of this hash size the bucket list conservatively, preventing it from having
over 25% utilization.  Compared to the memory overhead of pointers (and
fragmentation) that would be required by a scheme that implemented a linked
list per bucket, this overhead seems reasonable.

At any rate, the 50% utilization requirement allows us define "after" as
follows:

  position Y is after X iff "(X - Y) mod N > N/2"

, where N is the number of hash buckets.  We constrain N to be a power of
two, giving us:

  position Y is after X iff "(X - Y) bitwise_and (N-1) > N/2"

Note that if (X-Y) is negative, the bitwise_and will truncate any leading 1
bits so that the resulting expression is less than N.  Assuming the difference
between X and Y is less than N/2 (ie: the island that led to this range
computation is less than N/2 buckets long), the properties of 2's complement
arithmetic tell us that this number will be greater than N/2, and the
expression will return true.  If the difference is positive (and again, by
assumption, less than N/2), the expression will correctly return false;
iterating from Y to Y+1 and so on will reach X faster than iterating in the
other direction.  The hashtable used to use two expressions that special-cased
the above reasoning, but they did not cover corner cases involving entries 0
and maxbucketid.  This led to silent hashtable corruption for over a year until
it was eventually spotted.  Modular arithmetic handles strange wraparound cases
correctly and implicitly.

Lock crabbing and linearizability
=================================

According to wikipedia, a history is linearizable if:

 * its invocations and responses can be reordered to yield a sequential history

 * that sequential history is correct according to the sequential definition of
   the object

 * if a response preceded an invocation in the original history, it must still
   precede it in the sequential reordering

The first two bullets define serializability.  The third strengthens the
concept.  Serializability allows the system to choose an ordering (the
"commit order") for a set of requests.  Linearizability restricts the choice
of schedules to those that do not involve reordering of concurrent requests
from the callers' perspective.  This is important if the callers communicate
via external data structures; without the third requirement, apparent temporal
paradoxes could arise.

With this in mind, let us prove that the crabbing scheme is deadlock-free and
linearizable.

Lookup and insertion keep at most two hash table entries locked at a time;
deletion temporarily grabs a third lock.  Ignoring wraparound, this makes it
easy to show liveness.  The only way a deadlock could occur would be if there
existed a cycle of processes, each waiting to grab a latch held by the next.
Since each process grabs latches according to the bucket ordering, no process
will ever hold a high-numbered latch while waiting for a lower-numbered latch.
Therefore, the processes cannot possibly form a cycle, and no deadlock exists.

Informally, linearizability is achieved by using a latch protocol that leads
to an ordering of operations, and that ensures each operation sees all updates
from operations performed before it and no updates from operations performed
after it.

Unfortunately, wraparound introduces the possibility of deadlock and leads to
cycles of non-linearizable operations.

To see why deadlock is not a problem in practice, note that, for deadlock to
occur, we need to have threads obtain mutexes in a way that creates a cycle.
Each thread holds at most 3 latches at a time, and no thread will ever block
while holding a latch on a null bucket (making it difficult for the cycle to
span multiple islands).  Therefore, such cycles can be eliminated by limiting
hashtable bucket occupancy.  Similarly, the definitions of "before" and
"after" (in the temporal, linearizability sense) are correct, up to cycles; the
hash is linearizable so long as a cycle of concurrent requests do not all come
(by the above definition) "before" each other.  It is likely that such a cycle
could be broken arbitrarily in practice, as such requests would be, by
definition, concurrent, and each operates on a single key-value pair.

However, with this high-level intuition, we've gotten ahead of ourselves.  Let
us start by considering deletion in detail, as it is the most complex of the
operations.

Deletion starts by exclusively locking element to be removed (P), and then
potentially moves the exclusive lock to P', starting with P+1 and moving
forward.  If it moves forward, a third lock is temporarily grabbed on entry
P'+1, and then the lock on P' is released.

Let's consider the deletion algorithm described above and reason about cases
A and B:

  A. We discover a null element, or
  B. We discover an element that hashes to position P' <= P

                                   (here "<=" means "before or equal to)

In both cases, we want to ensure that the operation we perform is not
making any element in (P,P') undiscoverable. This is more complicated than
in the single-threaded case, as we released locks on the elements we observed
in this interval during crabbing.

At this point in the discussion, we fall back on the definition of
linearizability for guidance.  From the point of view of any hashtable
operation, we can partition all other operations as follows:
they happen "before" or "after" this operation, or they touch a
non-overlapping portion of the hashtable.

Non-overlapping operations are trivially linearizable (read "somebody else's
problem"), as linearizability across mutex acquisitions is guaranteed by
pthreads.

This leaves operations that touch the same hash buckets as our operation.
Each operation maintains a number of cursors, and occurs in two phases.
In phase 1, it simply crabs along from the bucket that the element of interest
hashes to, looking for a null bucket, or the element.  When our operation grabs
a mutex in phase one, it is forced into being "before" any other operations
that hold a bucket latch "before" our latch.  (The terminology here is
unfortunate.)  Similarly, our operation is "after" any operations that hold a
latch on a bucket "after" ours.

(NOTE: These definitions do not lead to transitive "before" and "after"
relationships.  For the proof hold, we would need to make use of the fact that
dependency cycles cannot exist due to low occupancy, just as we do for deadlock
freedom.)

During phase 1, the latches that are held are adjacent (ie: P+1=P'; therefore,
no intervening thread can get a latch inside of the range.  Similarly, all
operations obtain latches by crabbing, making it impossible for our definition
to say some operation is "before" and "after" this operation.

Phase 1 is read only, so the "before" and "after" definition trivially
corresponds to phase 1's side effects.

Phase 2 can be broken into three sub-cases.  The first two: lookup and
insertion, are trivial.  Phase 1 positioned the cursor at the correct bucket
(either the bucket containing the value, or the first null bucket after the
value, respectively).  Showing that lookups and insertions are always
linearizable reduces to applying the definition of "before" and "after", and
observing that:

 (1) operations blocked on this operation must be "after" it, and must reflect
     any update we performed, and

 (2) operations this insert operation blocked on must be "before" it, and
     cannot observe any modifications we made to the hashtable.

We now move onto deletion, which is (as always) more complicated.  Instead of
making use of simple crabbing, deletion leaves an "anchor" mutex at the location
of the value to be deleted, then creates a crabbing pair of mutexes that walk
along the island, looking for something to stick in the empty bucket at the
anchor.  This allows concurrent threads to place cursors between the anchor and
the crabbing pair.  It turns out that such threads are "after" the deletion
operation.  They do not notice any modifications made by deletion (deletion
would have moved the anchor if it had modified anything between the two
cursors), and deletion does not observe any empty buckets that future
operations could have created, since it never relatches buckets between the
anchor and the crabbing pair.  This property is not important from an
application point of view, but it does form the basis of our reasoning about
the correctness of concurrent deletion:

Case I: the deletion is not nested inside another deletion:

Because deletion keeps an exclusive lock on P, the only way for another
thread to get into (P,P') is to operate on a node that hashes between P and P',
as it could not arrive inside this interval by skipping over a locked P.

In case A (deletion encountered a null) it breaks the island by replacing
element P with null.  Recall that, in the sequential case, this is OK as long
as (P,P') does not contain entries mapped to indexes before P. It didn't when
we went through it in deletion algorithm, and it couldn't get elements like
that inserted because P has been continuously latched.  So it is safe to break
the island in this case.

In case B, we are moving an element from P' to P. This will cause trouble
only if there is a search (or phase 1) operation in progress in (P, P') looking
for the element at P'.  Since the element at P' hashes at or before position
P, and P is exclusively locked, the search operation must be scheduled
before the deletion operation. By linearizability, the deletion operation
cannot discover P' ahead of the search (the crabbing operation in search will
not let lookup of deletion operation pass it. So we can be certain that in
case B (P' hashes before or at P) there are no ongoing searches for element P'
in (P,P') and we can therefore safely move it over to P.

Case II: the deletion is nested inside another deletion:

The concern here is that we may break an invariant by breaking an island in
two (case A), or by moving some values around (case B).

A (Inserting a null):  Had that null existed, the outside deletion would have
terminated upon reaching the null.  This is OK because, in order to create a
null, the inner traversal must first inspect the remainder of the island.
Since the null is between the outer deletion's anchor and crabs, the portion of
the island that comes after the null does not contain any value that hashes
before the null.  The outer deletion's anchor, P, is before P''.  Since
P < P'', and all values after P'' in the island belong after P'', such values
must not belong before or at P, and therefore would not have been moved by the
outer delete, had it created the null.

B (Moving a value):  When we move a value from one part of the island to
another, it remains discoverable, exactly as in the sequential case.  This case
eventually will reduce to case A.

Liveness, revisited
-------------------

It is easy to prove that even though we cannot statically rank all the
locks covering hash table, deadlock is still impossible. For deadlock to
occur, we need to have a "ring" covering entire hash table contained of
"islands" and connected by in-flight crabbing operations.  No hashtable operations
block upon further latch acquisitions upon encountering an existing null bucket.
Such a bucket exists at the end of each island, preventing any latch chain from
spanning multiple islands.

There could be more involved cases of deadlock involving application code that
holds hashtable locks and then locks over some external data structures, or
attempts to latch a second hashtable bucket.  These deadlocks are inherent to
unsafe usages of the hashtable API, and not the underlying implementation.
Most should be detectable by ranking all locks and assigning the same rank to
all hashtable locks.

Conclusion
==========

As far as we can tell, the hash table implementation is correct. It is
conservative because it caps the utilization of hash at 25% instead of
50% minus one element. This is OK as it is relatively cheap and
decreases the average size of hash collision chains.

History:
========

   Created on: Oct 15, 2009
       Author: sears

-r1275 09 Nov 2009  Finalized API
-r1410 16 Sep 2010  Discovered need for three mutexes during deletion crabbing.
-r1429 30 Sep 2010  Added fsck logic.  (To no avail)
-r1475 14 Feb 2011  Slava found the mod bug, and wrote version 1 of the extensive
                    documentation above.  I expanded it into v2, and committed it.
 */
#define _XOPEN_SOURCE 600
#include <config.h>
#include <stasis/util/concurrentHash.h>
#include <stasis/util/hashFunctions.h>
#include <assert.h>
#include <stdio.h>

//#define STASIS_HASHTABLE_FSCK_THREAD

struct bucket_t {
  pageid_t key;
  pthread_mutex_t mut;
  void * val;
};

struct hashtable_t {
  bucket_t* buckets;
  pageid_t maxbucketid;
#ifdef STASIS_HASHTABLE_FSCK_THREAD
  int is_open;
  pthread_t fsck_thread;
#endif
};

static inline pageid_t hashtable_wrap(hashtable_t *ht, pageid_t p) {
  return p & ht->maxbucketid;
}
static inline pageid_t hash6432shift(pageid_t key)
{
  //  return key * 13;

//  key = (~key) + (key << 21); // key = (key << 21) - key - 1;
//  key = key ^ (key >> 24);
//  key = (key + (key << 3)) + (key << 8); // key * 265
//  key = key ^ (key >> 14);
//  key = (key + (key << 2)) + (key << 4); // key * 21
//  key = key ^ (key >> 28);
//  key = key + (key << 31);
//  return key;

//    key = (~key) + (key << 18); // key = (key << 18) - key - 1;
//    key = key ^ (key >> 31);
//    key = key * 21; // key = (key + (key << 2)) + (key << 4);
//    key = key ^ (key >> 11);
//    key = key + (key << 6);
//    key = key ^ (key >> 22);
//  return (key | 64) ^ ((key >> 15) | (key << 17));

#ifdef DBUG_TEST
  return key;
#else
  //return stasis_util_hash_fnv_1_uint32_t((const byte*)&key, sizeof(key));
  return stasis_crc32(&key, sizeof(key), 0);
  //return key * 13;
#endif
}
static inline pageid_t hashtable_func(hashtable_t *ht, pageid_t key) {
  return hashtable_wrap(ht, hash6432shift(key));
}

#ifdef STASIS_HASHTABLE_FSCK_THREAD
void * hashtable_fsck_worker(void * htp);
#endif

hashtable_t * hashtable_init(pageid_t size) {
  pageid_t newsize = 1;
  for(int i = 0; size; i++) {
    size /= 2;
    newsize *= 2;
  }
  hashtable_t *ht = malloc(sizeof(*ht));

  ht->maxbucketid = (newsize) - 1;
  ht->buckets = calloc(ht->maxbucketid+1, sizeof(bucket_t));
  for(int i = 0; i <= ht->maxbucketid; i++) {
    ht->buckets[i].key = -1;
  }
  pthread_mutexattr_t attr;
  pthread_mutexattr_init(&attr);
  pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
  for(pageid_t i = 0; i <= ht->maxbucketid; i++) {
    pthread_mutex_init(&(ht->buckets[i].mut), &attr);
  }
#ifdef STASIS_HASHTABLE_FSCK_THREAD
  ht->is_open = 1;
  pthread_create(&ht->fsck_thread,0, hashtable_fsck_worker, ht);
#endif
  return ht;
}
void hashtable_deinit(hashtable_t * ht) {
#ifdef STASIS_HASHTABLE_FSCK_THREAD
  ht->is_open = 0;
  pthread_join(ht->fsck_thread, 0);
#endif
  for(pageid_t i = 0; i <= ht->maxbucketid; i++) {
    pthread_mutex_destroy(&ht->buckets[i].mut);
  }
  free(ht->buckets);
  free(ht);
}

int hashtable_debug_number_of_key_copies(hashtable_t *ht, pageid_t pageid) {
  int count = 0;
  for(int i = 0; i <= ht->maxbucketid; i++) {
    if(ht->buckets[i].key == pageid) { count ++; }
  }
  if(count > 0) { fprintf(stderr, "%d copies of key %lld in hashtable!", count, (unsigned long long) pageid); }
  return count;
}

void hashtable_fsck(hashtable_t *ht) {
  pthread_mutex_lock(&ht->buckets[0].mut);
  for(int i = 1; i <= ht->maxbucketid; i++) {
    pthread_mutex_lock(&ht->buckets[i].mut);
    if(ht->buckets[i].key != -1) {
      pageid_t this_hash_code = hashtable_func(ht, ht->buckets[i].key);
      if(this_hash_code != i) {
        assert(ht->buckets[i-1].key != -1);
        assert(ht->buckets[i-1].val != 0);
        assert(this_hash_code < i || (this_hash_code > i + (ht->maxbucketid/2)));
      }
    } else {
      assert(ht->buckets[i].val == NULL);
    }
    pthread_mutex_unlock(&ht->buckets[i-1].mut);
  }
  pthread_mutex_lock(&ht->buckets[0].mut);
  if(ht->buckets[0].key != -1) {
    pageid_t this_hash_code = hashtable_func(ht, ht->buckets[0].key);
    if(this_hash_code != 0) {
      assert(ht->buckets[ht->maxbucketid].key != -1);
      assert(ht->buckets[ht->maxbucketid].val != 0);
      assert(this_hash_code < 0 || (this_hash_code > 0 + (ht->maxbucketid/2)));
    }
  } else {
    assert(ht->buckets[ht->maxbucketid].val == NULL);
  }
  pthread_mutex_unlock(&ht->buckets[ht->maxbucketid].mut);
  pthread_mutex_unlock(&ht->buckets[0].mut);
}
typedef enum {
  LOOKUP,
  INSERT,
  TRYINSERT,
  REMOVE
} hashtable_mode;
static inline void * hashtable_begin_op(hashtable_mode mode, hashtable_t *ht, pageid_t p, void *val, hashtable_bucket_handle_t *h) {
  static int warned = 0;
  assert(p != -1);
  pageid_t idx = hashtable_func(ht, p);
  void * ret;
  bucket_t *b1 = &ht->buckets[idx], *b2 = NULL;
  pthread_mutex_lock(&b1->mut); // start crabbing

  int num_incrs = 0;

  while(1) {
    // Loop invariants:
    //         b1 is latched, b2 is unlatched
    if(num_incrs > 10 && !warned) {
      warned = 1;
      printf("The hashtable is seeing lots of collisions.  Increase its size?\n");
    }
    assert(num_incrs < (ht->maxbucketid/4));
    num_incrs++;
    if(b1->key == p) { assert(b1->val); ret = b1->val; break; }
    if(b1->val == NULL) { assert(b1->key == -1); ret = NULL; break; }
    idx = hashtable_wrap(ht, idx+1);
    b2 = b1;
    b1 = &ht->buckets[idx];
    pthread_mutex_lock(&b1->mut);
    pthread_mutex_unlock(&b2->mut);
  }
  h->b1 = b1; // at this point, b1 is latched.
  h->key = p;
  h->idx = idx;
  h->ret = ret;
  return ret;
}


#ifdef STASIS_HASHTABLE_FSCK_THREAD
void * hashtable_fsck_worker(void * htp) {
  hashtable_t * ht = htp;
  while(ht->is_open) {
    fprintf(stderr, "Scanning hashtable %x", (unsigned int)ht);
    sleep(1);
    hashtable_fsck(ht);
  }
  return 0;
}
#endif


void hashtable_end_op(hashtable_mode mode, hashtable_t *ht, void *val, hashtable_bucket_handle_t *h) {
  pageid_t idx = h->idx;
  bucket_t * b1 = h->b1;
  bucket_t * b2 = NULL;
  if(mode == INSERT || (mode == TRYINSERT && h->ret == NULL)) {
    b1->key = h->key;
    b1->val = val;
  } else if(mode == REMOVE && h->ret != NULL)  {
    pageid_t idx2 = idx;
    idx = hashtable_wrap(ht, idx+1);
    b2 = b1;
    b1 = &ht->buckets[idx];
    pthread_mutex_lock(&b1->mut);
    while(1) {
      // Loop invariants: b2 needs to be overwritten.
      //                  b1 and b2 are latched
      //                  b1 is the next bucket to consider for copying into b2.

      // What to do with b1?
      // Case 1: It is null, we win.
      if(b1->val == NULL) {
//        printf("d\n"); fflush(0);
        assert(b1->key == -1);
        b2->key = -1;
        b2->val = NULL;
        break;
      } else {
        // Case 2: b1 belongs "after" b2

        pageid_t newidx = hashtable_func(ht, b1->key);

        // If newidx is past idx2, lookup will never find b1->key in position
        // idx2. Taking wraparound into account, and noticing that we never
        // have more than maxbucketid/4 elements in hash table, the following
        // expression detects if newidx is past idx2:
        if(((idx2 - newidx) & ht->maxbucketid) > ht->maxbucketid/2) {
          // skip this b1.
  //        printf("s\n"); fflush(0);
          idx = hashtable_wrap(ht, idx+1);
          bucket_t * b0 = &ht->buckets[idx];
          // Here we have to hold three buckets momentarily.  If we released b1 before latching its successor, then
          // b1 could be deleted by another thread, and the successor could be compacted before we latched it.
          pthread_mutex_lock(&b0->mut);
          pthread_mutex_unlock(&b1->mut);
          b1 = b0;
        } else {
          // Case 3: we can compact b1 into b2's slot.

//        printf("c %lld %lld %lld  %lld\n", startidx, idx2, newidx, ht->maxbucketid); fflush(0);
          b2->key = b1->key;
          b2->val = b1->val;
          pthread_mutex_unlock(&b2->mut);
          // now we need to overwrite b1, so it is the new b2.
          idx2 = idx;
          idx = hashtable_wrap(ht, idx+1);
          b2 = b1;
          b1 = &ht->buckets[idx];
          pthread_mutex_lock(&b1->mut);
        }
      }
    }
    pthread_mutex_unlock(&b2->mut);
  }
  pthread_mutex_unlock(&b1->mut);  // stop crabbing
}
static inline void * hashtable_op(hashtable_mode mode, hashtable_t *ht, pageid_t p, void *val) {
  hashtable_bucket_handle_t h;
  void * ret = hashtable_begin_op(mode, ht, p, val, &h);
  hashtable_end_op(mode, ht, val, &h);
  return ret;
}
static inline void * hashtable_op_lock(hashtable_mode mode, hashtable_t *ht, pageid_t p, void *val, hashtable_bucket_handle_t *h) {
  void * ret = hashtable_begin_op(mode, ht, p, val, h);
  // Nasty for a few reasons.  This forces us to use (slow) recursive mutexes.
  // Also, if someone tries to crab over this bucket in order to get to an
  // unrelated key, then it will block.
  pthread_mutex_lock(&h->b1->mut);
  hashtable_end_op(mode, ht, val, h);
  return ret;
}

void * hashtable_insert(hashtable_t *ht, pageid_t p, void * val) {
  void * ret = hashtable_op(INSERT, ht, p, val);
  return ret;
}
void * hashtable_test_and_set(hashtable_t *ht, pageid_t p, void * val) {
  hashtable_bucket_handle_t h;
  void * ret = hashtable_begin_op(TRYINSERT, ht, p, val, &h);
  if(ret) {
    hashtable_end_op(LOOKUP, ht, val, &h);
  } else {
    hashtable_end_op(INSERT, ht, val, &h);
  }
  return ret;
}
void * hashtable_lookup(hashtable_t *ht, pageid_t p) {
  void * ret = hashtable_op(LOOKUP, ht, p, NULL);
  return ret;
}
void * hashtable_remove(hashtable_t *ht, pageid_t p) {
  void * ret = hashtable_op(REMOVE, ht, p, NULL);
  return ret;
}

void * hashtable_insert_lock(hashtable_t *ht, pageid_t p, void * val, hashtable_bucket_handle_t *h) {
  return hashtable_op_lock(INSERT, ht, p, val, h);
}
void * hashtable_test_and_set_lock(hashtable_t *ht, pageid_t p, void * val, hashtable_bucket_handle_t *h) {
  return hashtable_op_lock(TRYINSERT, ht, p, val, h);
}
void * hashtable_lookup_lock(hashtable_t *ht, pageid_t p, hashtable_bucket_handle_t *h) {
  return hashtable_op_lock(LOOKUP, ht, p, NULL, h);
}
void hashtable_unlock(hashtable_bucket_handle_t *h) {
  pthread_mutex_unlock(&h->b1->mut);
}

void * hashtable_remove_begin(hashtable_t *ht, pageid_t p, hashtable_bucket_handle_t *h) {
  return hashtable_begin_op(REMOVE, ht, p, NULL, h);
}
void hashtable_remove_finish(hashtable_t *ht, hashtable_bucket_handle_t *h) {
 // when begin_remove_lock returns, it leaves the remove half done.  we then call this to decide if the remove should happen.  Other than hashtable_unlock, this is the only method you can safely call while holding a latch.
  hashtable_end_op(REMOVE, ht, NULL, h);
}
void hashtable_remove_cancel(hashtable_t *ht, hashtable_bucket_handle_t *h) {
 // when begin_remove_lock returns, it leaves the remove half done.  we then call this to decide if the remove should happen.  Other than hashtable_unlock, this is the only method you can safely call while holding a latch.
  hashtable_end_op(LOOKUP, ht, NULL, h);  // hack
}
