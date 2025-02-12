project(pika-migrate)

aux_source_directory(src DIR_SRCS)

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
  ${JEMALLOC_LIBRARY})