set(ZSTD_INSTALL "OFF")

if(NOT ZSTD_ROOT_DIR)
    set(ZSTD_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/third_party/zstd)
endif()
include_directories("${ZSTD_ROOT_DIR}")
add_subdirectory(third_party/zstd/build/cmake)

set(_gRPC_ZSTD_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/zstd/lib/")
set(_gRPC_ZSTD_LIBRARIES libzstd_static)
if(gRPC_INSTALL AND _gRPC_INSTALL_SUPPORTED_FROM_MODULE)
    install(TARGETS zstd EXPORT gRPCTargets
            RUNTIME DESTINATION ${gRPC_INSTALL_BINDIR}
            LIBRARY DESTINATION ${gRPC_INSTALL_LIBDIR}
            ARCHIVE DESTINATION ${gRPC_INSTALL_LIBDIR})
endif()
