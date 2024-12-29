cmake_minimum_required(VERSION 3.18)

# Avoid warning about DOWNLOAD_EXTRACT_TIMESTAMP in CMake 3.24:
if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
  cmake_policy(SET CMP0135 NEW)
endif()

set(CMAKE_CXX_STANDARD 17)
project(pika-migrate)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
enable_testing()

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  # using Clang
  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS "5.0")
    message(FATAL_ERROR "Clang version must be greater than 5.0")
  endif()
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  # using GCC
  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS "7.0")
    message(FATAL_ERROR "GCC G++ version must be greater than 7.0")
  endif()
endif()

link_directories("/opt/rh/gcc-toolset-13/root/lib/gcc/x86_64-redhat-linux/13")

############# You should enable sanitizer if you are developing pika #############
# Uncomment the following two lines to enable AddressSanitizer to detect memory leaks and other memory-related bugs.
#set(CMAKE_BUILD_TYPE "Debug")
#set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address  -O0 -fno-omit-frame-pointer -fno-optimize-sibling-calls")

# [Notice] AddressSanitizer and ThreadSanitizer can not be enabled at the same time.

# Uncomment the following two lines to enable ThreadSanitizer to detect data race and other thread-related issue.
#set(CMAKE_BUILD_TYPE "Debug")
#set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=thread -O0 -fno-omit-frame-pointer -fno-optimize-sibling-calls")

string(TOLOWER ${CMAKE_HOST_SYSTEM_PROCESSOR} HOST_ARCH)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RELEASE)
endif()

string(TOUPPER ${CMAKE_BUILD_TYPE} BUILD_TYPE)

if(${BUILD_TYPE} STREQUAL DEBUG)
  set(LIB_BUILD_TYPE DEBUG)
elseif(${BUILD_TYPE} STREQUAL MINSIZEREL)
  set(LIB_BUILD_TYPE MINSIZEREL)
elseif(${BUILD_TYPE} STREQUAL RELWITHDEBINFO)
  set(LIB_BUILD_TYPE RELWITHDEBINFO)
else()
  set(LIB_BUILD_TYPE RELEASE)
  set(CMAKE_CXX_FLAGS_RELEASE "-O2 -g -DNDEBUG")
endif()

if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
   set(CMAKE_CXX_FLAGS "-pthread")
   add_definitions(-DOS_MACOSX)
elseif (CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
    set(CMAKE_CXX_FLAGS "-pthread")
    add_definitions(-DOS_FREEBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "Linux")
   if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
     set(CMAKE_EXE_LINKER_FLAGS "-stdlib=libc++ -fuse-ld=lld -lc++ -lc++abi ${CMAKE_EXE_LINKER_FLAGS}")
     set(CMAKE_CXX_FLAGS "-stdlib=libc++ -pthread ${CMAKE_CXX_FLAGS}")
   elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
     set(CMAKE_EXE_LINKER_FLAGS "-static-libgcc -static-libstdc++")
     set(CMAKE_CXX_FLAGS "-pthread -Wl,--no-as-needed -ldl")
   endif()
   add_definitions(-DOS_LINUX)
else()
   message(FATAL_ERROR "only support linux or macOs or FreeBSD")
endif()

if(HOST_ARCH MATCHES "x86_64" OR HOST_ARCH MATCHES "i386")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse -msse4.2")
elseif(HOST_ARCH MATCHES "arm")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=armv8-a+crc+crypto -moutline-atomics")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")


message(STATUS "${PROJECT_NAME} staged install: ${STAGED_INSTALL_PREFIX}")
message(STATUS "Current platform: ${OS_VERSION} ")
cmake_host_system_information(RESULT CPU_CORE QUERY NUMBER_OF_LOGICAL_CORES)
message(STATUS "Cpu core ${CPU_CORE}")

find_program(AUTOCONF
        autoconf
        PATHS /usr/bin /usr/local/bin)

if (${AUTOCONF} MATCHES AUTOCONF-NOTFOUND)
  message(FATAL_ERROR "not find autoconf on localhost")
endif()

#set(CLANG_SEARCH_PATH "/usr/local/bin" "/usr/bin" "/usr/local/opt/llvm/bin"
#                      "/usr/local/opt/llvm@12/bin")
find_program(CLANG_TIDY_BIN
        NAMES clang-tidy clang-tidy-12
        HINTS ${CLANG_SEARCH_PATH})
if ("${CLANG_TIDY_BIN}" STREQUAL "CLANG_TIDY_BIN-NOTFOUND")
  message(WARNING "couldn't find clang-tidy.")
else ()
  message(STATUS "found clang-tidy at ${CLANG_TIDY_BIN}")
endif ()

find_program(CLANG_APPLY_REPLACEMENTS_BIN
        NAMES clang-apply-replacements clang-apply-replacements-12
        HINTS ${CLANG_SEARCH_PATH})

if ("${CLANG_APPLY_REPLACEMENTS_BIN}" STREQUAL "CLANG_APPLY_REPLACEMENTS_BIN-NOTFOUND")
    message(WARNING "couldn't find clang-apply-replacements.")
else ()
    message(STATUS "found clang-apply-replacements at ${CLANG_APPLY_REPLACEMENTS_BIN}")
endif ()

option(WITH_COMMAND_DOCS "build with command docs support" OFF)
if (WITH_COMMAND_DOCS)
  add_definitions(-DWITH_COMMAND_DOCS)
endif()

include(protogen.cmake)
include(ExternalProject)

aux_source_directory(src DIR_SRCS)

# # generate version
string(TIMESTAMP TS "%Y-%m-%d %H:%M:%S" UTC)
set(PIKA_BUILD_DATE "${TS}" CACHE STRING "the time we first built pika")


set(PIKA_BUILD_VERSION_CC ${CMAKE_BINARY_DIR}/pika_build_version.cc
        src/pika_cache_load_thread.cc
        )
message("PIKA_BUILD_VERSION_CC : " ${PIKA_BUILD_VERSION_CC})
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/src/build_version.cc.in ${PIKA_BUILD_VERSION_CC} @ONLY)

set(PROTO_FILES ${CMAKE_CURRENT_SOURCE_DIR}/src/pika_inner_message.proto ${CMAKE_CURRENT_SOURCE_DIR}/src/rsync_service.proto)
custom_protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})
message("pika PROTO_SRCS = ${PROTO_SRCS}")
message("pika PROTO_HDRS = ${PROTO_HDRS}")

add_executable(${PROJECT_NAME}
  ${DIR_SRCS}
  ${PROTO_SRCS}
  ${PROTO_HDRS}
  ${PIKA_BUILD_VERSION_CC})

target_link_directories(${PROJECT_NAME}
  PUBLIC ${INSTALL_LIBDIR_64}
  PUBLIC ${INSTALL_LIBDIR})

add_dependencies(${PROJECT_NAME}
  gflags
  gtest
  ${LIBUNWIND_NAME}
  glog
  fmt
  snappy
  zstd
  lz4
  zlib
  ${LIBGPERF_NAME}
  ${LIBJEMALLOC_NAME}
  rocksdb
  protobuf
  pstd
  net
  rediscache
  storage
  cache
  hiredis
)

target_include_directories(${PROJECT_NAME}
  PUBLIC ${CMAKE_CURRENT_BINARY_DIR}
  PUBLIC ${PROJECT_SOURCE_DIR}
  ${INSTALL_INCLUDEDIR}
)

target_link_libraries(${PROJECT_NAME}
  cache
  storage
  net
  pstd
  ${GLOG_LIBRARY}
  librocksdb.a
  ${LIB_PROTOBUF}
  ${LIB_GFLAGS}
  ${LIB_FMT}
  libsnappy.a
  libzstd.a
  liblz4.a
  libz.a
  librediscache.a
  ${LIBUNWIND_LIBRARY}
  ${JEMALLOC_LIBRARY}
  libhiredis.a)

option(USE_SSL "Enable SSL support" OFF)

