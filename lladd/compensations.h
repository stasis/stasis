#include <pthread.h>
#include <lladd/common.h>
#ifndef __COMPENSATIONS_H
#define __COMPENSATIONS_H

BEGIN_C_DECLS

/** Rants about cpp:

  There seems to be no way to add this syntax:
  
foo() {
  lock * l = hashLookup(foo);
 
  // stuff

  compensate_(l) {
     lock(l);
     // blah blah
  } with {
     unlock(l);
  }

  // more stuff
}

 => 

foo() {
  push_compensation_stack(lock_c_line_1231, l);

  lock(l);

  pop_compensation_stack();  // remove top stack entry and execute it.
}

void lock_c_line_1231(lock * l) {
  unlock(l);
}

  (note that this syntax doesn't require closures!)

  There are a few problems: 

  1: 'compensate' and 'with' need to know the name of the
  compensation's implementation function.

  2: the 'with' block needs to move its code to the outside of the
  enclosing function's scope, since nested functions cannot be called
  after the function they are declared in returns.

  You could try #defining a temporary variable, and reading from it in
  the 'with' macro, but you seem to need a stack in order to support
  that.

  Here is the syntax that I've settled on:
  
  lock_t * l = foo();

  begin_action(unlock, l) {
     lock(l);
     // ...
  } compensate;

  // Or:  (not recommended)

  begin_action(unlock, l) {
     lock(l);
     // ...
     unlock(l);
  } end_action;
  
  // This is a useful variant, however:

  lock(l);
  // while loop , complicated stuff, etc
   {
     begin_action(unlock, l) {
        // something that can cause an error.
     } end_action;

   }
  unlock(l);

  In all cases, an error can be raised using:

  compensation_set_error(int i);

  If an error is set, then each instance of begin_action and
  end_action will cause the function to return, as appropriate.

  Currently, nesting begin_actions within each other in the same
  function will not work.  This could probably be partially fixed by
  replacing return statements with 'break' statements, or a GOTO to
  the proper enclosing end_action/compensate.  There may be a way to
  #define/#undefine a variable in a way that would handle this
  properly.

  Also, begin_action(NULL, NULL) is supported, and is useful for
  checking the return value of a called function, but, for
  efficiency, try{ } end; is recommended
*/

void compensations_init();
void compensations_deinit();
long compensation_error();
void compensation_clear_error();
void compensation_set_error(long code);

#define try        do { if(compensation_error()) return;     do 
#define try_ret(x) do { if(compensation_error()) return (x); do 

#define end        while(0); if(compensation_error()) return;     }while(0)
#define end_ret(x) while(0); if(compensation_error()) return (x); }while(0)

extern int ___compensation_count___;

#define begin_action(func, var)      \
  if(compensation_error()) return;   \
  do{                                \
  void (*_func_)(void*);             \
  assert(func);                      \
  pthread_cleanup_push(_func_=(void(*)(void*))(func), (void*)(var));\
  assert(_func_);                      \
  do
/** @todo compensation variables don't need _func_ anymore. */
#define end_action                   \
  while(0);                          \
  pthread_cleanup_pop(/*_func_ &&*/compensation_error());        \
  if(compensation_error()) return;   \
  } while(0)

#define compensate                   \
  while(0);                          \
  pthread_cleanup_pop(1/*(int)_func_*/);  \
  if(compensation_error()) return;   \
  } while(0)

#define begin_action_ret(func, var, ret)      \
  if(compensation_error()) return (ret);   \
  do{                                \
  void (*_func_)(void*);             \
  pthread_cleanup_push(_func_=(void(*)(void*))(func), (void*)(var));\
  do

#define end_action_ret(ret)                   \
  while(0);                          \
  pthread_cleanup_pop(/*_func_ &&*/compensation_error());        \
  if(compensation_error()) return (ret);   \
  } while(0)

#define compensate_ret(ret)                   \
  while(0);                          \
  pthread_cleanup_pop(1/*(int)_func*/);  \
  if(compensation_error()) return (ret);   \
  } while(0)

#define compensated_function 

#endif

END_C_DECLS

