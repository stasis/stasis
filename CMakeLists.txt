cmake_minimum_required(VERSION 2.4) # For all I know, 2.0 works too...

# Make cmake warning go away.  They changed the default library search behavior.
# The new behavior seems preferable, though it shouldn't affect us either way.

if(COMMAND cmake_policy)
#  cmake_policy(SET CMP0014 OLD)  ## This breaks builds on older cmake...
  cmake_policy(SET CMP0003 NEW)
endif(COMMAND cmake_policy)

Project(Stasis)

SET(PACKAGE_VERSION 1)

SUBDIRS(src test utilities benchmarks examples lang)
SET(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
SET(FLEX_DIR  ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
SET(BISON_DIR ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

# Main decisions
SET(BUILD_SHARED_LIBS ON)
#ENABLE_TESTING()
INCLUDE(CTest)

FIND_PACKAGE(FLEX)
FIND_PACKAGE(BISON)

STRING(REGEX MATCH NEW_ENOUGH_FLEX "^flex 2\\.5\\.3" FLEX_VERSION)

FIND_PACKAGE(Doxygen)
if(DOXYGEN_EXECUTABLE)
  configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/doc/Doxyfile.in
    ${CMAKE_CURRENT_BINARY_DIR}/doc/Doxyfile
  )
  add_custom_command(
    OUTPUT ${CMAKE_BINARY_DIR}/doc/developers/html/index.html
    COMMAND rm -rf ${CMAKE_BINARY_DIR}/doc/developers
    COMMAND mkdir -p ${CMAKE_BINARY_DIR}/doc/developers
    COMMAND ${DOXYGEN_EXECUTABLE} ${CMAKE_BINARY_DIR}/doc/Doxyfile
    DEPENDS ${CMAKE_BINARY_DIR}/doc/Doxyfile
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
  )
  add_custom_target(
    docs ALL
    DEPENDS
    ${CMAKE_BINARY_DIR}/doc/developers/html/index.html
  )
else(DOXYGEN_EXECUTABLE)
  message(STATUS "doxygen not found; documentation will not be built")
endif(DOXYGEN_EXECUTABLE)

FIND_LIBRARY(HAVE_CONFUSE NAMES confuse)
if(NOT HAVE_CONFUSE)
  message(STATUS "libconfuse not found; sample application 'libdfa' will not be built") 
endif(NOT HAVE_CONFUSE)

FIND_LIBRARY(HAVE_FUSE NAMES fuse)
if(NOT HAVE_FUSE)
  message(STATUS "fuse not found; sample application 'stasis_fuse' will not be built")
endif(NOT HAVE_FUSE)


INCLUDE(CheckFunctionExists)
INCLUDE(CheckCSourceCompiles)
CHECK_FUNCTION_EXISTS(sync_file_range HAVE_SYNC_FILE_RANGE)
CHECK_FUNCTION_EXISTS(fdatasync HAVE_FDATASYNC)
CHECK_FUNCTION_EXISTS(tdestroy HAVE_TDESTROY)

FIND_LIBRARY(DBUG_TEST dbug-stubs  /home/sears/local/dbug/lib/)
if(NOT DBUG_TEST)
  message(STATUS "dbug not found. proceeding with normal build")
  SET(DBUG "")
else(NOT DBUG_TEST)
  message(STATUS "found dbug ${DBUG_TEST}")
  SET(DBUG ${DBUG_TEST})
endif(NOT DBUG_TEST)

SET(CMAKE_REQUIRED_FLAGS "-lm")
CHECK_FUNCTION_EXISTS(powl HAVE_POWL)
CHECK_C_SOURCE_COMPILES("#define _GNU_SOURCE
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

int main(int argc, char * argv[]) {
 argc = O_DIRECT;
}
" HAVE_O_DIRECT) 

CHECK_C_SOURCE_COMPILES("#include <alloca.h>
int main(int argc, char * argv[]) { alloca(1); }" HAVE_ALLOCA_H)

CHECK_C_SOURCE_COMPILES("#define _GNU_SOURCE
#include <stdio.h>

int main(int argc, char * argv[]) {
    getline(0,0,0);
}
" HAVE_GETLINE) 

CHECK_C_SOURCE_COMPILES("
int main(int argc, char* argv[]) {
  int i;
  __sync_bool_compare_and_swap(&i, 0, 1);
}
" HAVE_GCC_ATOMICS)

CHECK_C_SOURCE_COMPILES("
#include <pthread.h>
#include <limits.h>
int main(int argc, char* argv[]) {
  pthread_attr_t a;
  pthread_attr_setstacksize(&a, PTHREAD_STACK_MIN);
  return 0;
}
" HAVE_PTHREAD_STACK_MIN)

MACRO(CREATE_CHECK NAME)
  ADD_EXECUTABLE(${NAME} ${NAME}.c)
  TARGET_LINK_LIBRARIES(${NAME} ${COMMON_LIBRARIES})
  ADD_TEST(${NAME} nice ./${NAME})
ENDMACRO(CREATE_CHECK)

MACRO(CREATE_EXPERIMENTAL_CHECK NAME)
  ADD_EXECUTABLE(${NAME} ${NAME}.c)
  TARGET_LINK_LIBRARIES(${NAME} stasis_experimental ${COMMON_LIBRARIES})
  ADD_TEST(${NAME} nice ./${NAME})
ENDMACRO(CREATE_EXPERIMENTAL_CHECK)

MACRO(CREATE_CHECK_OPT NAME OPT)
  ADD_EXECUTABLE(${NAME} ${NAME}.c)
  TARGET_LINK_LIBRARIES(${NAME} ${COMMON_LIBRARIES})
  ADD_TEST(${NAME} nice ./${NAME} ${OPT})
ENDMACRO(CREATE_CHECK_OPT)

MACRO(CREATE_EXECUTABLE NAME)
  ADD_EXECUTABLE(${NAME} ${NAME}.c)
  TARGET_LINK_LIBRARIES(${NAME} ${COMMON_LIBRARIES})
ENDMACRO(CREATE_EXECUTABLE)

# Output the config.h file
#CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/config.h.cmake ${CMAKE_CURRENT_BINARY_DIR}/config.h)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR}
		    ${CMAKE_CURRENT_SOURCE_DIR}
		    ${CMAKE_CURRENT_SOURCE_DIR}/src
		    /usr/include)
# set linker path for this and all subdirs
LINK_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR}/src/stasis)

IF ( "${CMAKE_C_COMPILER_ID}" STREQUAL "GNU" )
  SET(COMMON_LIBRARIES stasis m pthread stdc++ ${DBUG}) # profiler)
  SET(CMAKE_C_FLAGS "-g -Wall -pedantic -std=gnu99 -DPBL_COMPAT -D_FILE_OFFSET_BITS=64 ${CMAKE_C_FLAGS}")
  SET(CMAKE_CXX_FLAGS "-g -Wall -Wno-long-long -pedantic -DPBL_COMPAT -D_FILE_OFFSET_BITS=64 ${CMAKE_CXX_FLAGS}")
ELSEIF ( "${CMAKE_C_COMPILER_ID}" STREQUAL "SunPro" )
  SET(COMMON_LIBRARIES stasis m pthread Crun Cstd)
  SET(CMAKE_C_FLAGS "-g -xc99=all -DPBL_COMPAT -D_FILE_OFFSET_BITS=64 ${CMAKE_C_FLAGS}")
  SET(CMAKE_CXX_FLAGS "-g -DPBL_COMPAT -D_FILE_OFFSET_BITS=64 ${CMAKE_CXX_FLAGS}")
ELSE( "${CMAKE_C_COMPILER_ID}" STREQUAL "GNU" )
  # TODO: how to detect compilers correctly on old cmake???  This is a huge hack; it uses old gcc 
  # options, since cmake is old...
  SET(COMMON_LIBRARIES stasis m pthread stdc++ ${DBUG})
  SET(CMAKE_C_FLAGS "-g -Wall -pedantic -std=gnu99 -DPBL_COMPAT -D_FILE_OFFSET_BITS=64 ${CMAKE_C_FLAGS}")
  SET(CMAKE_CXX_FLAGS "-g -Wall -Wno-long-long -pedantic -DPBL_COMPAT -D_FILE_OFFSET_BITS=64 ${CMAKE_CXX_FLAGS}")
ENDIF ( "${CMAKE_C_COMPILER_ID}" STREQUAL "GNU" )

INSTALL(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/src/stasis
        DESTINATION include/
        #FILES_MATCHING
        PATTERN "*.h"
        PATTERN ".svn" EXCLUDE
        PATTERN ".deps" EXCLUDE)
INSTALL(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/stasis
        DESTINATION include/
        #FILES_MATCHING
        PATTERN "*.h"
        PATTERN ".svn" EXCLUDE
        PATTERN ".deps" EXCLUDE)

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/config.h.cmake ${CMAKE_CURRENT_BINARY_DIR}/config.h)
