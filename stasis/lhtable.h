
#define LH_ENTRY(foo) lh##foo

typedef void LH_ENTRY(value_t);
typedef void LH_ENTRY(key_t);

struct LH_ENTRY(pair_t) { 
  const LH_ENTRY(key_t) * key;
  int keyLength;
  LH_ENTRY(value_t) * value;
  struct LH_ENTRY(pair_t) * next;
};

#define PBL_COMPAT 1

/**
   @todo The current implementation (and interface) hardcodes the idea
   that (key,value) pairs are inserted into the hashtable.  It would
   be better if it were more flexible.  Something like redblack.h
   would be nice, but that requires complex macros, code generation,
   etc...
*/
struct LH_ENTRY(table) * LH_ENTRY(create)(int initialSize);

/**
   @return 0 if item was not already in hashtable, pointer to old
   value otherwise.
*/
LH_ENTRY(value_t) * LH_ENTRY(insert)  (struct LH_ENTRY(table) * table, 
				       const  LH_ENTRY(key_t) * key, int len,
                                              LH_ENTRY(value_t) * value);

/**
   @return 0 if item not found
*/
LH_ENTRY(value_t) * LH_ENTRY(remove)  (struct LH_ENTRY(table) * table,
				       const  LH_ENTRY(key_t) * key, int len);

/**
   @return 0 if item not found
*/
LH_ENTRY(value_t) * LH_ENTRY(find)    (struct LH_ENTRY(table) * table,
				       const  LH_ENTRY(key_t) * key, int len);

void LH_ENTRY(destroy) (struct LH_ENTRY(table) * table);

struct LH_ENTRY(list) {
  const struct LH_ENTRY(table) * table;
  struct LH_ENTRY(pair_t) * currentPair;
  struct LH_ENTRY(pair_t) * nextPair;
  long currentBucket;
};

/**
   Print out stats regarding the lhtable implementation.  Currently,
   only prints anything if MEASURE_GLOBAL_BUCKET_LENGTH is defined.
*/
void LH_ENTRY(stats)();

void LH_ENTRY(openlist)(const struct LH_ENTRY(table) * table,
			struct LH_ENTRY(list) * newList);
/**
   @return 0 at end of list.
*/
const struct LH_ENTRY(pair_t)* LH_ENTRY(readlist)(struct LH_ENTRY(list)* list);
void LH_ENTRY(closelist)(struct LH_ENTRY(list) * list);
