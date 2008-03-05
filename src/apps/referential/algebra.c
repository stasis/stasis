#define _GNU_SOURCE
#include <stdio.h>
#include <stasis/transactional.h>
#include <string.h>
#include <ctype.h>
#include "algebra.h"
#include <stdlib.h>
#include <errno.h>
#define SELECT_ITERATOR (USER_DEFINED_ITERATOR+1)
#define PROJECT_ITERATOR (USER_DEFINED_ITERATOR+2)
#define KEYVAL_ITERATOR (USER_DEFINED_ITERATOR+3)
#define JOIN_ITERATOR (USER_DEFINED_ITERATOR+4)

/* static void ts_close(int xid, void * it) {

}
static int ts_next(int xid, void * it) {

}
static int ts_tryNext(int xid, void * it) {

}
static int ts_key(int xid, void * it, byte ** key) {

}
static int ts_value(int xid, void * it, byte ** val) {

}
static void ts_tupleDone(int xid, void * it) {

}
static void ts_releaseLock(int xid, void *it) {

} */
/*static const lladdIterator_def_t ts_it = {
  ts_close, ts_next, ts_tnext, ts_key, ts_value, ts_tupleDone, noopTupDone
  }; */


char ** split(char * in, char ** freeme, int* count, char * delim) {
  *freeme = strdup(in);
  *count = 0;
  char * strtoks;
  char * tok = strtok_r(*freeme, delim,&strtoks);
  char ** ret = 0;
  while(tok) {
    (*count)++;
    ret = realloc(ret, sizeof(char*) * *count);
    ret[(*count)-1] = tok;
    tok = strtok_r(NULL,delim,&strtoks);
  }
  ret = realloc(ret, sizeof(char*) * ((*count)+1));
  ret[*count]=0;
  return ret;
}

char * tplRid(recordid rid) {
  char * ret;
  asprintf(&ret, "%d,%d,%lld", rid.page, rid.slot, (long long)rid.size);
  return ret;
}
recordid ridTpl(char * tpl) {
  int count;
  char * freeme;
  char ** tok = split(tpl, &freeme, &count, ", ");
  recordid ret;
  errno = 0;
  assert(count == 3);
  ret.page = strtol(tok[0],NULL,10);
  ret.slot = strtol(tok[1],NULL,10);
  ret.size = strtol(tok[2],NULL,10);
  if(errno) {
    perror("Couldn't parse rid");
    abort();
  }
  free(freeme);
  free(tok);
  return ret;
}

char ** tplDup(char ** tup) {
  int i = 0;
  for(; tup[i]; i++) { } // i = num non-null entries
  char ** ret = malloc(sizeof(char**) * (i+1));
  ret[i] = 0;
  while(i) {
    i--;
    ret[i] = strdup(tup[i]);
  }
  return ret;
}
void tplFree(char ** tup) {
  for(int i = 0; tup[i]; i++) {
    free(tup[i]);
  }
  free(tup);
}

int isWhitelistChar(char c) {
  return(c == ';'
	 ||
	 c == '/'
	 ||
	 c == '?'
	 ||
	 c == ':'
	 ||
	 c == '@'
	 ||
	 c == '&'
	 ||
	 c == '='
	 ||
	 c == '+'
	 ||
	 c == '$'
	 ||
	 c == '['
	 ||
	 c == ']'
	 ||
	 c == '-'
	 ||
	 c == '_'
	 ||
	 c == '.'
	 ||
	 c == '!'
	 ||
	 c == '~'
	 ||
	 c == '*'
	 ||
	 c == '\''
	 ||
	 c == '%'
	 ||
	 c == '\\');
}

lladdIterator_t* ReferentialAlgebra_OpenTableScanner(int xid, recordid catalog,
						    char * tablename) {
  char * table;
  size_t sz = ThashLookup(xid, catalog, (byte*)tablename, strlen(tablename)+1, (byte**)&table);
  if(sz == -1) {
    printf("Unknown table %s\n", tablename);
    return 0;
  }
  assert(sz == strlen(table)+1);
  recordid tableRid = ridTpl(table);
  lladdIterator_t * it = ThashGenericIterator(xid, tableRid);
  free(table);
  return it;
}
typedef struct select_predicate {
  char ** vals;
  int count;
  void * freeme;
} select_predicate;

typedef struct select_impl {
  lladdIterator_t * it;
  char ** p;
} select_impl;

//////////////////////////////////////////////////////////////////////////////////
///                                                                            ///
///                KEY VALUE TABLE FORMAT                                      ///
///                                                                            ///
//////////////////////////////////////////////////////////////////////////////////

typedef struct kv_impl {
  char * catted;
  lladdIterator_t * it;
} kv_impl;

lladdIterator_t* ReferentialAlgebra_KeyValCat(int xid, lladdIterator_t * it) {
  kv_impl * kv = malloc(sizeof(kv_impl));
  kv->catted = 0;
  kv->it = it;
  lladdIterator_t * new_it = malloc(sizeof(lladdIterator_t));
  new_it->type = KEYVAL_ITERATOR;
  new_it->impl = kv;
  return new_it;
}

static void kv_close(int xid, void * it) {
  kv_impl * kv = it;
  Titerator_close(xid, kv->it);
  if(kv->catted) { free(kv->catted); kv->catted = 0; }
  free(kv);
}
static int kv_next(int xid, void * it) {
  kv_impl * kv = it;
  if(kv->catted) { free(kv->catted); kv->catted = 0; }
  return Titerator_next(xid, kv->it);
}
static int kv_tryNext(int xid, void * it) {
  kv_impl * kv = it;
  if(kv->catted) { free(kv->catted); kv->catted = 0; }
  return Titerator_tryNext(xid, kv->it);
}
static int mkCatted(int xid, kv_impl * kv) {
  char * key = 0;
  char * val = 0;
  Titerator_key(xid, kv->it, (byte**)&key);
  Titerator_value(xid, kv->it, (byte**)&val);

  if(!strlen(val)) {
    kv->catted = strdup(key);
  } else {
    kv->catted = malloc(strlen(key) + 1 + strlen(val) + 1);
    kv->catted[0]=0;
    strcat(strcat(strcat(kv->catted,key),","),val);
  }
  return(strlen(kv->catted));
}
static int kv_key(int xid, void * it, byte ** key) {
  kv_impl * kv = it;
  if(!kv->catted) {
    int ret = mkCatted(xid, kv);
    *key = (byte*)kv->catted;
    return ret;
  } else {
    *key = (byte*)kv->catted;
    return(strlen(kv->catted)+1);
  }
}
static int kv_value(int xid, void * it, byte ** val) {
  kv_impl * kv = it;
  if(!kv->catted) {
    int ret = mkCatted(xid, kv);
    *val = (byte*)kv->catted;
    return ret;
  } else {
    *val = (byte*)kv->catted;
    return strlen(kv->catted)+1;
  }
}
static void kv_tupleDone(int xid, void * it) {
  kv_impl * kv = it;
  Titerator_tupleDone(xid, kv->it);
}
static void kv_releaseLock(int xid, void *it) {
  kv_impl * kv = it;
  Titerator_releaseLock(xid, kv->it);
}

//////////////////////////////////////////////////////////////////////////////////
///                                                                            ///
///                SELECT                                                      ///
///                                                                            ///
//////////////////////////////////////////////////////////////////////////////////
lladdIterator_t* ReferentialAlgebra_Select(int xid, lladdIterator_t * it, char ** pred) {
  if(!it) {
    tplFree(pred);
    return 0;
  }
  select_impl * s = malloc(sizeof(select_impl));
  s->p = pred;
  s->it = it;
  lladdIterator_t * new_it = malloc(sizeof(lladdIterator_t));
  new_it->type = SELECT_ITERATOR;
  new_it->impl = s;
  return new_it;
}

char * strtokempt(char *str, const char * delim, char ** saveptr, int * extra) {
  char * ret;
  if(str) {
    *saveptr = str;
  }
  ret = *saveptr;

  if(!(**saveptr)) {
    if(*extra) {
      *extra = 0;
      return ret;
    } else {
      return 0;
    }
  }
  do {
    for(int i = 0; delim[i]; i++) {
      if(**saveptr == delim[i]) {
	// terminate returned string
	**saveptr = 0;
	// consume deliminator character from last time.
	(*saveptr)++;
	//printf("extra: %d\n",*extra);
	*extra = 1;
	return ret;
      }
    }
    // consume character we just looked at.
    (*saveptr)++;
  } while(**saveptr);
  //  printf("extra: %d\n",*extra);
  *extra = 0;
  return ret;
}

static int matchPredicate(const char const * tup, char ** pred) {
  char * tupcpy = strdup(tup);
  int colcount = 0;
  int predcount = 0;
  while(pred[predcount]) {predcount++;}
  char ** tok = malloc((predcount+1) * sizeof(char**));

  char * ths;
  const char const * DELIM = ",";
  char * strtoks;
  int extra = 0;
  if((ths = strtokempt(tupcpy, DELIM,&strtoks,&extra))) {
    colcount++;
    if(colcount > predcount) {
      free(tupcpy);
      free(tok);
      return 0;
    } else {
      tok[colcount-1] = ths;
    }
  }
  while((ths = strtokempt(NULL, DELIM,&strtoks,&extra))) {
    colcount++;
    if(colcount > predcount) {
      free(tupcpy);
      free(tok);
      return 0;
    } else {
      tok[colcount-1] = ths;
    }
  }
  int match = 0;
  if(colcount == predcount) {
    match=1;
    for(int i = 0; i < predcount; i++) {
      if(strcmp(pred[i],"*") && strcmp(pred[i], tok[i])) {
	match = 0;
	break;
      }
    }
  }
  free(tupcpy);
  free(tok);
  return match;
}
static void s_close(int xid, void * it) {
  select_impl * impl = it;
  Titerator_close(xid, impl->it);
  tplFree(impl->p);
  free(impl);
}
static int s_next(int xid, void * it) {
  select_impl * impl = it;
  while(Titerator_next(xid,impl->it)) {
    char * val;
    Titerator_value(xid, impl->it, (byte**)&val);
    //    printf("matching w/ %s\n", val);
    if(matchPredicate(val, impl->p)) {
      return 1;
    } else {
      Titerator_tupleDone(xid, impl->it);
    }
  }
  return 0;
}
static int s_tryNext(int xid, void * it) {
  abort();
}
static int s_key(int xid, void * it, byte ** key) {
  return Titerator_key(xid, ((select_impl*)it)->it, key);
}
static int s_value(int xid, void * it, byte ** val) {
  return Titerator_value(xid, ((select_impl*)it)->it, val);
}
static void s_tupleDone(int xid, void * it) {
  Titerator_tupleDone(xid, ((select_impl*)it)->it);
}
static void s_releaseLock(int xid, void *it) {
  Titerator_releaseLock(xid, ((select_impl*)it)->it);
}

//////////////////////////////////////////////////////////////////////////////////
///                                                                            ///
///                PROJECT                                                     ///
///                                                                            ///
//////////////////////////////////////////////////////////////////////////////////

typedef struct project_impl {
  int count;
  short * cols;
  lladdIterator_t * it;
  char * val;
} project_impl;

lladdIterator_t* ReferentialAlgebra_Project(int xid, lladdIterator_t * it, char ** project) {
  if(!it) {
    if(project) {
      tplFree(project);
    }
    return 0;
  }
  project_impl * p = malloc(sizeof(project_impl));
  int projectcount = 0;
  while(project[projectcount]){projectcount++;}
  p->count = projectcount;
  p->cols = malloc(sizeof(short) * projectcount);
  p->it = it;
  p->val = 0;
  for(int i = 0; project[i]; i++) {
    errno = 0;
    char * eos;
    long col = strtol(project[i], &eos, 10);
    if(*eos!='\0' || ((col == LONG_MIN || col == LONG_MAX) && errno)) {
      printf("Couldn't parse column descriptor\n");
      tplFree(project);
      free(p->cols);
      free(p);
      return 0;
    } else {
      p->cols[i] = col;
    }
  }
  lladdIterator_t * new_it = malloc(sizeof(lladdIterator_t));
  new_it->type = PROJECT_ITERATOR;
  new_it->impl = p;
  tplFree(project);
  return new_it;
}

static void p_close(int xid, void * it) {
  project_impl * impl = it;
  Titerator_close(xid, impl->it);
  free(impl->cols);
  free(impl);
}
static int p_next(int xid, void * it) {
  project_impl * impl = it;
  return Titerator_next(xid, impl->it);
}
static int p_tryNext(int xid, void * it) {
  project_impl * impl = it;
  return Titerator_tryNext(xid, impl->it);
}
static int p_key(int xid, void * it, byte ** key) {
  project_impl * impl = it;
  return Titerator_key(xid,impl->it,key);
}
static int p_value(int xid, void * it, byte ** val) {
  project_impl * impl = it;
  byte * in_val;
  if(impl->val) {
    *val = (byte*)impl->val;
    return 1;
  }
  int ret = Titerator_value(xid,impl->it,&in_val);
  if(ret) {
    char * freeme;
    int count;
    char ** tok = split((char*)in_val, &freeme, &count, ", ");
    *val = malloc(sizeof(char));
    (*val)[0] = '\0';

    for(int i = 0; i < impl->count; i++) {
      if(impl->cols[i] < count) {
	if(i) {
	  (*val) = realloc((*val), strlen((char*)(*val)) + 1 + strlen(tok[impl->cols[i]]) + 1);
	  (*val) = (byte*)strcat(strcat((char*)(*val), ","), tok[impl->cols[i]]);
	} else {
	  (*val) = realloc((*val), strlen((char*)(*val)) + strlen(tok[impl->cols[i]]) + 1);
	  (*val) = (byte*)strcat((char*)(*val), tok[impl->cols[i]]);
	}
      } else {
	printf("Tuple is too short for pattern.\n");
      }
    }
    free(freeme);
    free(tok);
  }
  impl->val = (char*)*val;
  return ret;
}
static void p_tupleDone(int xid, void * it) {
  project_impl * impl = it;
  if(impl->val) { free(impl->val); impl->val = 0; }
  Titerator_tupleDone(xid,impl->it);
}
static void p_releaseLock(int xid, void *it) {
  project_impl * impl = it;
  Titerator_releaseLock(xid,impl->it);
}

//////////////////////////////////////////////////////////////////////////////////
///                                                                            ///
///                JOIN (naive nested loops)                                   ///
///                                                                            ///
//////////////////////////////////////////////////////////////////////////////////

typedef struct join_impl {
  char *** inner_tpls;
  char ** inner_strs;
  char ** freethese;
  lladdIterator_t * outer_it;
  lladdIterator_t * inner_it;
  char ** pred;
  int inner_pos;
  char ** outer_tpl;
  char * outer_str;
  char * freeouter;
} join_impl;

static int matchComparator(char ** tup1,
			   char ** tup2,
			   char ** pred) {
  int match = 1;
  int col = 0;
  while(pred[col] && match) {
    char * lhs_start = pred[col];
    char * lhs_end = lhs_start;
    while(isWhitelistChar(*lhs_end)||isalnum(*lhs_end)) { lhs_end++; }
    int lhs_len = lhs_end - lhs_start;

    char * lhs = calloc(lhs_len+1,sizeof(char));
    memcpy(lhs, lhs_start, lhs_len);

    char * op_start = lhs_end;
    while(isblank(*op_start)) { op_start++; }
    char * op_end = op_start;
    while(!(isblank(*op_end) || isWhitelistChar(*lhs_end)||isalnum(*op_end))) { op_end++; }
    int op_len = op_end - op_start;

    char * op = calloc(op_len+1,sizeof(char));
    memcpy(op, op_start, op_len);

    char * rhs_start = op_end;
    while(isblank(*rhs_start)) { rhs_start++; }
    char * rhs_end = rhs_start;
    while(isWhitelistChar(*lhs_end)||isalnum(*rhs_end)) { rhs_end++; }
    int rhs_len = rhs_end - rhs_start;

    char * rhs = calloc(rhs_len+1,sizeof(char));
    memcpy(rhs, rhs_start, rhs_len);

    long col1 = strtol(lhs, NULL, 10);
    long col2 = strtol(rhs, NULL, 10);

    int colcount1 = 0;
    int colcount2 = 0;
    while(tup1[colcount1]) { colcount1++; }
    while(tup2[colcount2]) { colcount2++; }

    if(colcount1 <= col1 || colcount2 <= col2) {
      printf("not enough columns for join!\n");
      match = 0;
    } else if(!strcmp(op,"=")) {
      if(strcmp(tup1[col1], tup2[col2])) {
	match = 0;
      }
    }
    col++;
    free(rhs);
    free(lhs);
    free(op);
  }
  return match;
}

lladdIterator_t* ReferentialAlgebra_Join(int xid,
					 char ** pred,
					 lladdIterator_t * outer_it,
					 lladdIterator_t * inner_it) {
  if(!(outer_it && inner_it)) {
    if(pred) { tplFree(pred); }
    return 0;
  }

  join_impl * j = malloc(sizeof(join_impl));

  j->inner_it = inner_it;
  j->outer_it = outer_it;
  j->pred = pred;

  j->inner_tpls = calloc(1, sizeof(char ***));
  j->inner_strs = calloc(1, sizeof(char **));
  j->freethese = malloc(sizeof(char**));
  int i = 0;
  while(Titerator_next(xid, inner_it)) {
    char * in_val;
    Titerator_value(xid, inner_it, (byte**)&in_val);
    int count;
    char ** tok = split((char*)in_val, (j->freethese)+i, &count, ", ");
    j->inner_tpls = realloc(j->inner_tpls, sizeof(char***)*(i+2));
    j->inner_strs = realloc(j->inner_strs, sizeof(char**)*(i+2));
    j->freethese = realloc(j->freethese, sizeof(char**)*(i+2));
    j->inner_tpls[i] = tok;
    j->inner_tpls[i+1] = 0;
    j->freethese[i+1] = 0;
    j->inner_strs[i] = strdup(in_val);
    j->inner_strs[i+1] = 0;
    Titerator_tupleDone(xid, inner_it);
    i++;
  }
  j->inner_pos = 0;
  j->outer_tpl = 0;
  lladdIterator_t * new_it = malloc(sizeof(lladdIterator_t));
  new_it->type = JOIN_ITERATOR;
  new_it->impl = j;
  return ReferentialAlgebra_KeyValCat(xid,new_it);
}


static void j_close(int xid, void * it) {
  join_impl * j = it;
  Titerator_close(xid,j->outer_it);
  Titerator_close(xid,j->inner_it);
  for(int i = 0; j->inner_tpls[i]; i++) {
    //    tplFree(j->inner_tpls[i]);
    free(j->freethese[i]);
    free(j->inner_strs[i]);
    free(j->inner_tpls[i]);
  }
  tplFree(j->pred);
  free(j->inner_tpls);
  free(j->inner_strs);
  free(j->freethese);
  if(j->freeouter) { free(j->freeouter); }
  // don't free pred; that's the caller's problem.
  free(j);
}
static int j_next(int xid, void * it) {
  join_impl * j = it;
  while(1) {
    if((!j->inner_tpls[j->inner_pos]) || (!j->outer_tpl)) {
      j->inner_pos = 0;
      Titerator_tupleDone(xid, j->outer_it);
      if(Titerator_next(xid, j->outer_it)) {
	int count;
	Titerator_value(xid, j->outer_it, (byte**)&j->outer_str);
	j->outer_tpl = split((char*)j->outer_str, &j->freeouter, &count, ", ");
      } else {
	return 0;
      }
    }
    if(matchComparator(j->outer_tpl, j->inner_tpls[j->inner_pos], j->pred)) {
      j->inner_pos++;
      return 1;
    } else {
      j->inner_pos++;
    }
  }
}
static int j_tryNext(int xid, void * it) {
  abort();
}
static int j_key(int xid, void * it, byte ** key) {
  join_impl * j = it;
  *key = (byte*)j->outer_str;
  return 1;
}
static int j_value(int xid, void * it, byte ** val) {
  join_impl * j = it;
  *val = (byte*)j->inner_strs[j->inner_pos-1];
  return 1;
}
static void j_tupleDone(int xid, void * it) {
  join_impl * j = it;
  free(j->outer_tpl);
  free(j->freeouter);
  j->outer_tpl = 0;
  j->freeouter = 0;
}
static void j_releaseLock(int xid, void *it) {
  // noop
}
//////////////////////////////////////////////////////////////////////////////////
///                                                                            ///
///                ININTIALIZATION                                             ///
///                                                                            ///
//////////////////////////////////////////////////////////////////////////////////

void ReferentialAlgebra_init() {
  lladdIterator_def_t select_def = {
    s_close, s_next, s_tryNext, s_key, s_value, s_tupleDone, s_releaseLock
    };
  lladdIterator_register(SELECT_ITERATOR, select_def);
  lladdIterator_def_t project_def = {
    p_close, p_next, p_tryNext, p_key, p_value, p_tupleDone, p_releaseLock
  };
  lladdIterator_register(PROJECT_ITERATOR, project_def);
  lladdIterator_def_t keyval_def = {
    kv_close, kv_next, kv_tryNext, kv_key, kv_value, kv_tupleDone, kv_releaseLock
  };
  lladdIterator_register(KEYVAL_ITERATOR, keyval_def);
  lladdIterator_def_t j_def = {
    j_close, j_next, j_tryNext, j_key, j_value, j_tupleDone, j_releaseLock
  };
  lladdIterator_register(JOIN_ITERATOR, j_def);
}


//////////////////////////////////////////////////////////////////////////////////
///                                                                            ///
///                PARSER                                                      ///
///                                                                            ///
//////////////////////////////////////////////////////////////////////////////////

// Reserved characters:  (, ), [, ], ",", " ".
// Grammar:
// E = (s [tuple] E) | (p [tuple] E) | (j [tuple] E E) | TABLENAME
// tuple = V | V,tuple
// V = string of non-reserved characters
// TABLENAME = string of non-reserved characters

#define LPAREN   '{'
#define RPAREN   '}'
#define LBRACKET '('
#define RBRACKET ')'
#define COMMA    ','
#define SPACE    ' '
#define STRING   's'
#define EOS      '\0'
/*
  @return one of the above.  If returns STRING, set *tok to be the new
  token. (*tok should be freed by caller in this case)
 */
int nextToken(char ** head, char ** tok, int breakOnSpace);

char** parseTuple(char ** head) {
  char **tok = calloc(1,sizeof(char*));;
  char * mytok;
  char ret = nextToken(head, &mytok,0);
  assert(ret == LBRACKET);
  int count = 0;
  while(1) {
    ret = nextToken(head, &mytok,0);
    if(ret == RBRACKET) {
      break;
    }
    if(ret == COMMA) {
      tok = realloc(tok, sizeof(char*)*(count+1));
      tok[count] = 0;
      tok[count-1] = calloc(1,sizeof(char));
    } else if(ret == STRING) {
      count++;
      tok = realloc(tok, sizeof(char*)*(count+1));
      tok[count] = 0;
      tok[count-1] = mytok;
      ret = nextToken(head, &mytok,0);
      if(ret == STRING) { free(mytok); }
      if(ret == RBRACKET) {
	break;
      }
      if(ret != COMMA) {
	tplFree(tok);
	return 0;
      }
    } else {
      tplFree(tok);
      return 0;
    }
  }
  return tok;
}

lladdIterator_t * parseExpression(int xid, recordid catalog,
				  char **head) {
  while(isblank(**head)) { (*head)++; }
  if(**head == LPAREN) {
    (*head)++;
    lladdIterator_t * it;
    if(**head == 's') {
      (*head)++;
      char ** pred = parseTuple(head);
      lladdIterator_t * it2 =  parseExpression(xid, catalog, head);
      it  =  ReferentialAlgebra_Select(xid, it2, pred);
      if(it2 && !it) {
	Titerator_close(xid,it2);
      }
    } else if(**head == 'p') {
      (*head)++;
      char ** pred = parseTuple(head);
      lladdIterator_t * it2 =  parseExpression(xid, catalog, head);
      it  =  ReferentialAlgebra_Project(xid, it2, pred);
      if(it2 && !it) {
	Titerator_close(xid,it2);
      }
    } else if(**head == 'j') {
      (*head)++;
      char ** pred = parseTuple(head);
      lladdIterator_t * outer = parseExpression(xid, catalog, head);
      lladdIterator_t * inner = parseExpression(xid, catalog, head);
      it = ReferentialAlgebra_Join(xid, pred, outer, inner);
      if(outer && !it) {
	Titerator_close(xid,outer);
      }
      if(inner && !it) {
	Titerator_close(xid,inner);
      }
    } else {
      printf("Unknown operator\n");
      it = 0;
    }
    if(!it) {
      printf("parse error\n");
      return 0;
    }
    char * foo;
    char ret = nextToken(head, &foo,0);

    if(ret != RPAREN) {
      Titerator_close(xid,it);
      return 0;
    } else {
      return it;
    }
  } else {
    char * tablename;
    char ret = nextToken(head, &tablename,1);
    assert(ret == STRING);
    lladdIterator_t * it2 =
      ReferentialAlgebra_OpenTableScanner(xid, catalog, tablename);
    free(tablename);

    if(!it2) { return 0; }


    lladdIterator_t * it = ReferentialAlgebra_KeyValCat(xid,it2);

    return it;
  }
  abort();
}

int nextToken(char ** head, char ** tok, int breakOnSpace) {
  while(isblank(**head) && **head) { (*head)++; }
  switch(**head) {
  case LPAREN: {
    (*head)++;
    return LPAREN;
  } break;
  case RPAREN: {
    (*head)++;
    return RPAREN;
  } break;
  case LBRACKET: {
    (*head)++;
    return LBRACKET;
  } break;
  case RBRACKET: {
    (*head)++;
    return RBRACKET;
  } break;
  case COMMA: {
    (*head)++;
    return COMMA;
  } break;
  case SPACE: {
    (*head)++;
    return SPACE;
  } break;
  default: {
    if(!**head) { return EOS; };
    char * first = *head;
    while(isalnum(**head)
	  ||isWhitelistChar(**head)
	  ||(**head==' '&&!breakOnSpace)) {
      (*head)++;
    }
    char * last = *head;
    *tok = calloc(1 + last - first, sizeof(char));
    // The remaining byte is the null terminator
    strncpy(*tok, first, last - first);
    int i = (last-first)-1;
    int firstloop = 1;
    while((*tok)[i] == ' ') {
      (*tok)[i] = '\0';
      i++;
      if(firstloop) {
	(*head)--;
	firstloop = 0;
      }
    }
    return STRING;
  } break;
  }
}

char ** executeQuery(int xid, recordid hash, char * line) {
  char * lineptr = line;

  lladdIterator_t * it = parseExpression(xid,hash,&lineptr);
  if(it) {
    char ** tuples = malloc(sizeof(char*));
    int count = 0;
    while(Titerator_next(xid, it)) {
      count++;
      tuples = realloc(tuples, sizeof(char*)*(count+1));
      Titerator_value(xid,it,(byte**)(tuples+count-1));
      tuples[count-1] = strdup(tuples[count-1]);
      Titerator_tupleDone(xid,it);
    }
    Titerator_close(xid,it);
    tuples[count] = 0;
    return tuples;
  } else {
    return 0;
  }
}
