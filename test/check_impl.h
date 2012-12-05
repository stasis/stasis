/**
 * @file check_impl.h
 *
 *  A quick-and-dirty implementation of check, the C-unit test suite.
 *
 *  Created on: Apr 14, 2009
 *      Author: sears
 */
#ifndef CHECK_IMPL_H_
#define CHECK_IMPL_H_

#include <stasis/common.h>
#include <sys/time.h>
#include <pthread.h>

#define CK_NORMAL 0

#define START_TEST(x) static void x(void) {
#define END_TEST }

static int reaper_enabled = 0;
static pthread_t reaper;
static pthread_cond_t reaper_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t reaper_mut = PTHREAD_MUTEX_INITIALIZER;
void * reaper_impl(void* arg) {
  long long seconds = (intptr_t)arg;
  struct timeval now;
  struct timespec timeout;

  gettimeofday(&now, 0);

  timeout.tv_sec = now.tv_sec + seconds;
  timeout.tv_nsec = now.tv_usec * 1000;

  pthread_mutex_lock(&reaper_mut);
  int timedout;
  timedout = pthread_cond_timedwait(&reaper_cond, &reaper_mut, &timeout);
  assert(timedout != EINTR);
  assert(!timedout);

  pthread_mutex_unlock(&reaper_mut);

  return 0;
}

#define fail_unless(x,y) assert(x)

typedef struct {
	void(*setup)(void);
	void(*teardown)(void);
	void(**tests)(void);
	char **names;
	int count;
} TCase;

void tcase_set_timeout(TCase* tc, int seconds) {
  if(seconds == 0) return;
  pthread_create(&reaper, 0, reaper_impl, (void*)(intptr_t)seconds);
  reaper_enabled = 1;
}

typedef struct {
	char * name;
	TCase * tc;
} Suite;
typedef struct {
	Suite * s;
} SRunner;

static TCase* tcase_create(const char * ignored) {
	TCase* tc = stasis_alloc(TCase);
	tc->count = 0;
	tc->names = 0;
	tc->tests = 0;
	tc->setup = 0;
	tc->teardown = 0;
	return tc;
}
static inline void tcase_add_checked_fixture(TCase * tc, void(*setup)(void), void(*teardown)(void)) {
	assert(tc->setup == 0);
	assert(tc->teardown == 0);
	tc->setup = setup;
	tc->teardown = teardown;
}
#define tcase_add_test(tc, fcn) tcase_add_test_(tc, fcn, #fcn)
static void tcase_add_test_(TCase * tc, void(*fcn)(void), const char* name) {
	(tc->count)++;
	tc->tests = (void(**)(void)) stasis_realloc(tc->tests, tc->count, void*);
	tc->names = stasis_realloc(tc->names, tc->count, char*);
	tc->tests[tc->count-1] = fcn;
	tc->names[tc->count-1] = strdup(name);
}
static void tcase_free(TCase * tc) {
	int i = 0;
	for(i = 0; i < tc->count; i++) {
		free(tc->names[i]);
	}
	free(tc->names);
	free(tc->tests);
	free(tc);
}

static Suite * suite_create(const char * name) {
	Suite* ret = stasis_alloc(Suite);
	ret->name = strdup(name);
	ret->tc = 0;
	return ret;
}
static void suite_add_tcase(Suite* s, TCase* tc) {
	assert(s->tc == 0);
	s->tc = tc;
}
static void suite_free(Suite* s) {
	free(s->name);
	tcase_free(s->tc);
	free(s);
}
static SRunner * srunner_create(Suite* s) {
	SRunner * ret = stasis_alloc(SRunner);
	ret->s = s;
	return ret;
}
static void srunner_set_log(void * p, const char * ignored) { /* noop */ }
static void srunner_run_all(SRunner* sr, int ignored) {
	int i = 0;
	fprintf(stderr, "%s:\n", sr->s->name);
	for(i = 0; i < sr->s->tc->count; i++) {
		fprintf(stderr, "\t%s...",sr->s->tc->names[i]);
		if(sr->s->tc->setup) { sr->s->tc->setup(); }
		sr->s->tc->tests[i]();
		if(sr->s->tc->teardown) { sr->s->tc->teardown(); }
		fprintf(stderr, "pass\n");
	}
	if(reaper_enabled) {
	  pthread_cond_signal(&reaper_cond);
	  pthread_join(reaper,0);
	}
	fprintf(stderr,"All tests passed.\n");
}
static void srunner_free(SRunner* sr) {
	suite_free(sr->s);
	free(sr);
}
static int srunner_ntests_failed(SRunner * sr) {
	return 0;
}
#endif /* CHECK_IMPL_H_ */
