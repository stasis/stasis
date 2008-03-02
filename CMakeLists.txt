Project(Stasis)

SET(PACKAGE_VERSION 1)

SUBDIRS(src test utilities benchmarks examples)

# Main decisions
SET(BUILD_SHARED_LIBS ON)
#ENABLE_TESTING()
INCLUDE(CTest)

# Look for include files
INCLUDE (CheckIncludeFiles)
FIND_LIBRARY(CHECK_LIBRARY NAMES check)
if(!CHECK_LIBRARY)
  message("warning: libcheck not found; unit tests will not be built.")
endif(!CHECK_LIBRARY)
#CHECK_INCLUDE_FILES(check.h HAVE_LIBCHECK)
CHECK_INCLUDE_FILES(confuse.h HAVE_LIBCONFUSE)

MACRO(CREATE_CHECK NAME)
  ADD_EXECUTABLE(${NAME} ${NAME}.c)
  TARGET_LINK_LIBRARIES(${NAME} -lcheck ${COMMON_LIBRARIES})
  ADD_TEST(${NAME} ${NAME})
ENDMACRO(CREATE_CHECK)

MACRO(CREATE_CHECK_OPT NAME OPT)
  ADD_EXECUTABLE(${NAME} ${NAME}.c)
  TARGET_LINK_LIBRARIES(${NAME} -lcheck ${COMMON_LIBRARIES})
  ADD_TEST(${NAME} ${NAME} ${OPT})
ENDMACRO(CREATE_CHECK_OPT)



# Output the config.h file
#CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/config.h.cmake ${CMAKE_CURRENT_BINARY_DIR}/config.h)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}
		    ${CMAKE_CURRENT_SOURCE_DIR}/src
		    /usr/include)
# set linker path for this and all subdirs
LINK_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR}/src/stasis ${CMAKE_CURRENT_BINARY_DIR}/src/libdfa)

SET(COMMON_LIBRARIES stasis rw m pthread)
SET(CMAKE_C_FLAGS "-g -Wall -pedantic -std=gnu99 -DPBL_COMPAT ${CMAKE_C_FLAGS}")
