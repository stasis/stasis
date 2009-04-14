cmake_minimum_required(VERSION 2.4) # For all I know, 2.0 works too...

# Make cmake warning go away.  They changed the default library search behavior.
# The new behavior seems preferable, though it shouldn't affect us either way.

if(COMMAND cmake_policy)
  cmake_policy(SET CMP0003 NEW)
endif(COMMAND cmake_policy)

Project(Stasis)

SET(PACKAGE_VERSION 1)

SUBDIRS(src test utilities benchmarks examples)
SET(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
SET(FLEX_DIR  ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
SET(BISON_DIR ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

# Main decisions
SET(BUILD_SHARED_LIBS ON)
#ENABLE_TESTING()
INCLUDE(CTest)

FIND_PACKAGE(FLEX)
FIND_PACKAGE(BISON)


#FIND_LIBRARY(CHECK_LIBRARY NAMES check)
#if(NOT CHECK_LIBRARY)
#  message(STATUS "libcheck not found; unit tests will not be built.")
#endif(NOT CHECK_LIBRARY)

FIND_LIBRARY(HAVE_CONFUSE NAMES confuse)
if(NOT HAVE_CONFUSE)
  message(STATUS "libconfuse not found; sample application 'libdfa' will not be built") 
endif(NOT HAVE_CONFUSE)

FIND_LIBRARY(HAVE_FUSE NAMES fuse)
if(NOT HAVE_FUSE)
  message(STATUS "fuse not found; sample application 'stasis_fuse' will not be built")
endif(NOT HAVE_FUSE)

MACRO(CREATE_CHECK NAME)
  ADD_EXECUTABLE(${NAME} ${NAME}.c)
  TARGET_LINK_LIBRARIES(${NAME} ${COMMON_LIBRARIES})
  ADD_TEST(${NAME} ${NAME})
ENDMACRO(CREATE_CHECK)

MACRO(CREATE_CHECK_OPT NAME OPT)
  ADD_EXECUTABLE(${NAME} ${NAME}.c)
  TARGET_LINK_LIBRARIES(${NAME} ${COMMON_LIBRARIES})
  ADD_TEST(${NAME} ${NAME} ${OPT})
ENDMACRO(CREATE_CHECK_OPT)

MACRO(CREATE_EXECUTABLE NAME)
  ADD_EXECUTABLE(${NAME} ${NAME}.c)
  TARGET_LINK_LIBRARIES(${NAME} ${COMMON_LIBRARIES})
ENDMACRO(CREATE_EXECUTABLE)

# Output the config.h file
#CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/config.h.cmake ${CMAKE_CURRENT_BINARY_DIR}/config.h)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}
		    ${CMAKE_CURRENT_SOURCE_DIR}/src
		    /usr/include)
# set linker path for this and all subdirs
LINK_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR}/src/stasis ${CMAKE_CURRENT_BINARY_DIR}/src/libdfa)

SET(COMMON_LIBRARIES stasis m pthread)
SET(CMAKE_C_FLAGS "-g -Wall -pedantic -std=gnu99 -DPBL_COMPAT ${CMAKE_C_FLAGS}")
SET(CMAKE_CXX_FLAGS "-g -Wall -Wno-variadic-macros -Wno-long-long -pedantic -DPBL_COMPAT ${CMAKE_CXX_FLAGS}")
