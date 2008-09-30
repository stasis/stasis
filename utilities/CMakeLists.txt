ADD_EXECUTABLE(logfile_dump logfile_dump.c)
TARGET_LINK_LIBRARIES(logfile_dump ${COMMON_LIBRARIES})

ADD_EXECUTABLE(truncate_log truncate_log.c)
TARGET_LINK_LIBRARIES(truncate_log ${COMMON_LIBRARIES})

ADD_EXECUTABLE(run_recovery run_recovery.c)
TARGET_LINK_LIBRARIES(run_recovery ${COMMON_LIBRARIES})
