#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include "pobj.h"
#include "debug.h"

#ifndef NULL
#define NULL 0
#endif
#define MAXLINELEN 255

#define SUN 0
#define MON 1
#define TUE 2
#define WED 3
#define THU 4
#define FRI 5
#define SAT 6

#define DAILY 0
#define MONTHLY 1
#define WEEKLY 2
#define ONCE 3
#define COMMENT 4

#define PSET_INDEX(x, y, z) pobj_memcpy(x, x + y, &z, sizeof(z))
#define PSET_STATIC(x, y) pobj_static_set_ref(&x, y)

struct string {
  char *data;
  int len;
};

struct entry {
  int month;
  int day;
  char dayofweek;
  char type;
  struct string *text;
  struct entry *next;
};

int string_ref_fields[] = {
  member_offset(struct string, data),
  -1
};

int entry_ref_fields[] = {
  member_offset(struct entry, text),
  member_offset(struct entry, next),
  -1
};

typedef struct string String;
typedef struct entry Entry;

Entry *head = NULL;
Entry *tail = NULL;

// Assumes input is non-persistent.
void trim(String *s) {
  int i;
  for (i = 0; i < s->len && s->data[i] == ' '; i++) {
    s->data++;
  }
  s->len -= i;
  for (i = s->len - 1; i >= 0 && s->data[i] == ' '; i--) {
    s->len--;
  }
  s->data[s->len] = '\0';
}

int findWordEnd(String *s, int start) {
  int end;
  while (s->data[start] == ' ') start++;
  for (end = start; end < s->len && s->data[end] != ' '; end++);
  return end < s->len ? end : s->len;
}

// Assumes there is enough space in the destination.
void persistentStringCopy(String *src, int off1, String *dst, int off2, int length) {
/*   int i; */
/*   char c = '\0'; */
/*   // Can't use strncpy here without some sort of pobj_update() function. */
/*   for (i = 0; i < length; i++) { */
/*     PSET_INDEX(dst->data, off2 + i, src->data[off1 + i]); */
/*   } */
  debug("in persistent string copy, src = %x, off1 = %d, dst = %x, off2 = %d, length = %d", src, off1, dst, off2, length);
  debug("src->len is %d, data at src->data[off1] = %s", src->len, src->data + off1);
  strncpy(dst->data + off2, src->data + off1, length);
  dst->data[off2+length] = '\0';
  pobj_update(dst->data);
  POBJ_SET_INT(dst, len, length);
  debug("end of copy, dst->data = %s, dst->len = %d", dst->data, dst->len);
}

String *makePersistentString(String *s, int start, int len) {
  pobj_start();
  String *p = (String *) pobj_malloc(sizeof(String));
  pobj_ref_typify(p, string_ref_fields);
  POBJ_SET_REF(p, data, pobj_malloc(sizeof(char) * (len + 1)));
  persistentStringCopy(s, start, p, 0, len);
  pobj_end();
  return p;
}

Entry *makeEntry(String *s, char t, int off) {
  Entry *e = (Entry *) pobj_malloc(sizeof(Entry));
  pobj_ref_typify(e, entry_ref_fields);
  POBJ_SET_CHAR(e, type, t);
  s = makePersistentString(s, off, s->len - off);
  POBJ_SET_REF(e, text, s);
  return e;
}

Entry *makeCommentEntry(String *s) {
  pobj_start();
  Entry *e = makeEntry(s, COMMENT, 0);
  pobj_end();
  return e;
}

Entry *makeDayEntry(String *s, int off) {
  pobj_start();
  Entry *e = makeEntry(s, DAILY, off);
  pobj_end();
  return e;
}

Entry *makeMonthEntry(String *s, int d, int off) {
  pobj_start();
  Entry *e = makeEntry(s, MONTHLY, off);
  POBJ_SET_INT(e, day, d);
  pobj_end();
  return e;
}

Entry *makeWeekEntry(String *s, int start, int off) {
  pobj_start();
  Entry *e = makeEntry(s, WEEKLY, off);
  switch (s->data[start]) {
  case 'm':
    POBJ_SET_CHAR(e, dayofweek, MON);
    break;
  case 't':
    switch (s->data[start+1]) {
    case 'u':
      POBJ_SET_CHAR(e, dayofweek, TUE);
      break;
    default:
      POBJ_SET_CHAR(e, dayofweek, THU);
    }
    break;
  case 'w':
    POBJ_SET_CHAR(e, dayofweek, WED);
    break;
  case 'f':
    POBJ_SET_CHAR(e, dayofweek, FRI);
    break;
  default:
    switch (s->data[start+1]) {
    case 'a':
      POBJ_SET_CHAR(e, dayofweek, SAT);
      break;
    default:
      POBJ_SET_CHAR(e, dayofweek, SUN);
    }
  }
  pobj_end();
  return e;
}
  
Entry *makeOnceEntry(String *s, int m, int d, int off) {
  pobj_start();
  Entry *e = makeEntry(s, ONCE, off);
  POBJ_SET_INT(e, month, m);
  POBJ_SET_INT(e, day, d);
  pobj_end();
  return e;
}

// Parse a (non-persistent) string into a (persistent) entry.
Entry *parse(String *s) {
  int start, end, tmp;
  trim(s);
  if (s->data[0] == '#' || s->len < 1) {
    return makeCommentEntry(s);
  }
  start = 0;
  end = findWordEnd(s, start);
  switch (s->data[0]) {
  case 'D':
    return makeDayEntry(s, end + 1);
  case 'M':
    start = end;
    end = findWordEnd(s, start);
    return makeMonthEntry(s, atoi(s->data + start), end + 1);
  case 'W':
    start = end;
    end = findWordEnd(s, start);
    return makeWeekEntry(s, start + 1, end + 1);
  case 'O':
    start = end;
    end = findWordEnd(s, start);
    tmp = atoi(s->data + start);
    start = end;
    end = findWordEnd(s, start);
    return makeOnceEntry(s, tmp, atoi(s->data + start), end + 1);
  default:
    return makeCommentEntry(s);
  }
}

// Should be rewritten to be sorted?
void addEntry(String *str) {
  Entry *e = parse(str);
  pobj_start();
  if (head == NULL) {
    PSET_STATIC(head, e);
  } else {
    POBJ_SET_REF(tail, next, e);
  }
  PSET_STATIC(tail, e);
  POBJ_SET_REF(e, next, NULL);
  pobj_end();
}

void print(Entry *e) {
  //e->text->data[e->text->len] = '\0';
  printf("%s\n", e->text->data);
}

void findEntries(int month, int day, int dayofweek) {
  Entry *tmp = head;
  Entry *prev = NULL;
  while (tmp != NULL) {
    switch (tmp->type) {
    case DAILY:
      print(tmp);
      break;
    case MONTHLY:
      if (day == tmp->day) {
        print(tmp);
      }
      break;
    case WEEKLY:
      if (dayofweek == tmp->dayofweek) {
        print(tmp);
      }
      break;
    case ONCE:
      if (day == tmp->day && month == tmp->month) {
        print(tmp);
        // Remove this entry, since it is no longer needed.
        pobj_start();
        if (prev == NULL) {
          PSET_STATIC(head, tmp->next);
        } else {
          POBJ_SET_REF(prev, next, tmp->next);
        }
        if (tail == tmp) {
          PSET_STATIC(tail, prev);
        }
        pobj_free(tmp);
        pobj_end();
      }
      break;
    }
    prev = tmp;
    tmp = tmp->next;
  }
}

void getEntries(String *str) {
  time_t timer;
  struct tm asdf;
  struct tm *stm = &asdf;
  debug("1");
  time(&timer);
  debug("2");
  localtime_r(&timer, stm);
  debug("3");
  findEntries(stm->tm_mon + 1, stm->tm_mday, stm->tm_wday);
}

void dumpEntries(String *str) {
  Entry *tmp = head;
  while (tmp != NULL) {
    switch (tmp->type) {
    case DAILY:
      printf("DAILY ");
      break;
    case MONTHLY:
      printf("MONTHLY %d ", tmp->day);
      break;
    case WEEKLY:
      printf("WEEKLY ");
      switch (tmp->dayofweek) {
      case SUN:
        printf("sun ");
        break;
      case MON:
        printf("mon ");
        break;
      case TUE:
        printf("tue ");
        break;
      case WED:
        printf("wed ");
        break;
      case THU:
        printf("thu ");
        break;
      case FRI:
        printf("fri ");
        break;
      default:
        printf("sat ");
      }
      break;
    case ONCE:
      printf("ONCE %d %d ", tmp->month, tmp->day);
      break;
    default:
      printf("COMMENT ");
    }
    print(tmp);
    tmp = tmp->next;
  }
}

int getline(char *string, int num) {
  if (fgets(string, num, stdin) != string) {
    return -1;
  }
  num = strlen(string);
  if (string[num-1] == '\n') {
    string[num-1] = '\0';
    return num - 1;
  } else {
    return num;
  }
}

void error(char *a, char *b, int len) {
  b[len] = '\0';
  printf("%s%s\n", a, b);
}

void process(String *str) {
  int end = findWordEnd(str, 0);
  str->data += end;
  str->len -= end;
  switch (str->data[-end]) {
  case 'A':
    addEntry(str);
    break;
  case 'G':
    getEntries(str);
    break;
  case 'D':
    dumpEntries(str);
    break;
  default:
    error("Unknown command: ", str->data - end, end);
  }    
}

void usage() {
  printf("Simple calendar program. Reads in commands from stdin.\n\n");
  printf("Commands:\n");
  printf("A <entry> -- adds the specified entry into the calendar\n");
  printf("  <entry> can be of one of the following forms:\n");
  printf("    DAILY <message> -- denotes that <message> should be printed every day\n");
  printf("    WEEKLY <wday> <message> -- denotes that <message> should be printed on the\n");
  printf("                               specified day of the week\n");
  printf("      <wday> must be one of sun, mon, tue, wed, thu, fri, and sat\n");
  printf("    MONTHLY <mday> <message> -- denotes that <message> should be printed on the\n");
  printf("                                specified day of the month\n");
  printf("      <mday> must be a number between 1 and 31, inclusive\n");
  printf("    ONCE <mon> <mday> <message> -- denotes that <message> should be only\n");
  printf("                                   printed on the specified date\n");
  printf("      <mon> must be a number between 1 and 12, inclusive\n");
  printf("      <mday> must be a number between 1 and 31, inclusive\n");
  printf("  All other entries are interpreted as comments.\n");
  printf("G -- displays the messages for the current day\n");
  printf("     deletes ONCE type entries for current day\n");
  printf("D -- dumps all entries currently in the calendar\n");
  printf("<EOF> -- exits the program\n");
}

int main(int argc, char **argv) {
  char line[MAXLINELEN+1];
  String str;
  str.data = line;
  if (argc > 1 && !strncmp(argv[1], "--help", 6)) {
    usage();
    return 0;
  } else if (argc > 1) {
    error("Illegal option: ", argv[1], strlen(argv[1]));
    return 1;
  }
  pobj_init(NULL);
  while ((str.len = getline(line, MAXLINELEN)) != -1) {
    process(&str);
    str.data = line; // Could have been moved by process().
  }
  pobj_shutdown();
  return 0;
}

