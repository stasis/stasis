LIBSRC = $(wildcard src/stasis/*.c)   $(wildcard src/stasis/*/*.c)   $(wildcard src/stasis/*/*/*.c) \
         $(wildcard src/stasis/*.cpp) $(wildcard src/stasis/*/*.cpp) $(wildcard src/stasis/*/*/*.cpp)
LIBNAME = stasis

include config/Makefile.stasis