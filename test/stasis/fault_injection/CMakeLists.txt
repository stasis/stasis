ADD_EXECUTABLE(check_faultInjection fitest.c fisubject.c fichecker.c)
TARGET_LINK_LIBRARIES(check_faultInjection  ${COMMON_LIBRARIES})

SET_TARGET_PROPERTIES(check_faultInjection PROPERTIES
   PREFIX ""
   LINK_FLAGS -Wl,--wrap,malloc,--wrap,fwrite,--wrap,pwrite,--wrap,pwrite64
)

CONFIGURE_FILE( 
    ${CMAKE_CURRENT_SOURCE_DIR}/config.txt
    ${CMAKE_CURRENT_BINARY_DIR}/config.txt
    COPYONLY
  )
ADD_TEST(check_faultInjection check_faultInjection)
