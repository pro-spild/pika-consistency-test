set(WARNING_FLAGS "-g -W -Wextra -Wall -Wsign-compare \
-Wno-unused-parameter -Wno-redundant-decls -Wwrite-strings -Wno-unused-result \
-Wpointer-arith -Wreorder -Wswitch -Wsign-promo \
-Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers")

set(CXXFLAGS "${WARNING_FLAGS} -std=c++17 -g")

set(SRC_DIR .)
aux_source_directory(${SRC_DIR} BASE_OBJS)

add_executable(consistent_check ${BASE_OBJS})

target_include_directories(consistent_check PRIVATE ${INSTALL_INCLUDEDIR} PRIVATE ${PROJECT_SOURCE_DIR} ${ROCKSDB_SOURCE_DIR} ${GLOG_INCLUDE_DIR})

target_link_libraries(consistent_check pthread)
target_link_libraries(consistent_check ${ROCKSDB_LIBRARY})
target_link_libraries(consistent_check ${GLOG_LIBRARY})
target_link_libraries(consistent_check ${SNAPPY_LIBRARY})
target_link_libraries(consistent_check pstd)
target_link_libraries(consistent_check ${HIREDIS_LIBRARY})
set_target_properties(consistent_check PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
    CMAKE_COMPILER_IS_GNUCXX TRUE
    COMPILE_FLAGS ${CXXFLAGS})
add_dependencies(consistent_check snappy hiredis)