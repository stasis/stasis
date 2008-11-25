ADD_LIBRARY(pobj xmem.c debug.c hash.c queue.c pobj.c)
TARGET_LINK_LIBRARIES(pobj ${COMMON_LIBRARIES})

