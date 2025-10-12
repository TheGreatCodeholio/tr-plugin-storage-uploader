cmake_minimum_required(VERSION 3.13)
project(tr-plugin-storage-uploader VERSION 0.2.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Boost REQUIRED)        # headers only (property_tree)
find_package(CURL REQUIRED)

add_library(storage_uploader
        MODULE
        src/storage_uploader_plugin.cc
        src/s3_client.cc
        src/sftp_client.cc
)

include_directories(
        ${CMAKE_BINARY_DIR}/../                 # /src/trunk-recorder (TR tree root)
        ${CMAKE_CURRENT_SOURCE_DIR}/include     # your plugin headers: config.hpp, s3_client.hpp, ...
)

target_link_libraries(storage_uploader
        PRIVATE
        CURL::libcurl
        # Optional but harmless; many plugins link this:
        trunk_recorder_library
)

# GCC < 9 only
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9)
    target_link_libraries(storage_uploader PRIVATE stdc++fs)
endif()

set_target_properties(storage_uploader PROPERTIES
        OUTPUT_NAME "storage_uploader"
        PREFIX "lib"
)

install(TARGETS storage_uploader LIBRARY DESTINATION ${CMAKE_INSTALL_PREFIX}/lib/trunk-recorder)
