#ifndef _STASIS_FLAGS_H__
#define _STASIS_FLAGS_H__
/**
    This is the type of buffer manager that is being used.

    Before Stasis is intialized it will be set to a default value.
    It may be changed before Tinit() is called, or overridden at
    compile time by defining USE_BUFFER_MANAGER.

    (eg: gcc ... -DUSE_BUFFER_MANAGER=BUFFER_MANAGER_FOO)

    @see constants.h for a list of recognized buffer manager implementations.
         (The constants are named BUFFER_MANAGER_*)

 */
extern int bufferManagerType;
/**
   This determines which type of file handle the buffer manager will use.

   It defaults to BUFFER_MANAGER_FILE_HANDLE_NON_BLOCKING for a
   non-blocking handle.  It can be overridden at compile time by defining
   BUFFER_MANAGER_FILE_HANDLE_TYPE.

   @see constants.h for potential values.  (The constants are named
   BUFFER_MANAGER_FILE_HANDLE_*)
*/
extern int bufferManagerFileHandleType;
/**
   Determines which type of slow handle non_blocking will use for the
   buffer manager.  Override at compile time by defining
   BUFFER_MANAGER_NON_BLOCKING_SLOW_TYPE.

   @see constants.h: Constants named IO_HANDLE_* are potential values.
*/
extern int bufferManagerNonBlockingSlowHandleType;
/**
   If true, the buffer manager will use O_DIRECT.  Set at compile time by
   defining BUFFER_MANAGER_O_DIRECT.
*/
extern int bufferManagerO_DIRECT;
#endif
