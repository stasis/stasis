SUBDIRS(multicore)

IF(HAVE_GETLINE)
ADD_EXECUTABLE(rose rose.cpp)
TARGET_LINK_LIBRARIES(rose stasis_experimental ${COMMON_LIBRARIES})

ADD_EXECUTABLE(roseTable roseTable.cpp)
TARGET_LINK_LIBRARIES(roseTable stasis_experimental ${COMMON_LIBRARIES})

ADD_EXECUTABLE(roseTableTpcCH-workload1 roseTableTpcCH-workload1.cpp)
TARGET_LINK_LIBRARIES(roseTableTpcCH-workload1 stasis_experimental ${COMMON_LIBRARIES})

ADD_EXECUTABLE(roseTableTpcCH-workload2 roseTableTpcCH-workload2.cpp)
TARGET_LINK_LIBRARIES(roseTableTpcCH-workload2 stasis_experimental ${COMMON_LIBRARIES})

ADD_EXECUTABLE(transitiveClosure transitiveClosure.c)
TARGET_LINK_LIBRARIES(transitiveClosure stasis_experimental ${COMMON_LIBRARIES})


ENDIF(HAVE_GETLINE)


CREATE_EXECUTABLE(lhtableThreaded)
CREATE_EXECUTABLE(naiveHash)
CREATE_EXECUTABLE(naiveMultiThreaded)
CREATE_EXECUTABLE(rawSet)
CREATE_EXECUTABLE(arrayListSet)
CREATE_EXECUTABLE(linearHashNTA)
CREATE_EXECUTABLE(linkedListNTA)
CREATE_EXECUTABLE(pageOrientedListNTA)
CREATE_EXECUTABLE(linearHashNTAThreaded)
CREATE_EXECUTABLE(linearHashNTAMultiReader)
CREATE_EXECUTABLE(linearHashNTAWriteRequests)
CREATE_EXECUTABLE(zeroCopy)
CREATE_EXECUTABLE(sequentialThroughput)
CREATE_EXECUTABLE(qos)
CREATE_EXECUTABLE(writeBack)
CREATE_EXECUTABLE(distributedLsnFree)
CREATE_EXECUTABLE(bufferManager)
CREATE_EXECUTABLE(redBlackMemoryOverhead)
CREATE_EXECUTABLE(hashPerformance)
CREATE_EXECUTABLE(seekMap)
CREATE_EXECUTABLE(rawIOPS)

IF(CHECK_LIBRARY)
  ADD_TEST(rose rose)
ENDIF(CHECK_LIBRARY)
CREATE_EXECUTABLE(slotted)
