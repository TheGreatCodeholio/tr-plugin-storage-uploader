
cmake_minimum_required(VERSION 3.13)
project(tr-plugin-storage-uploader VERSION 0.2.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Boost REQUIRED COMPONENTS filesystem system)
find_package(CURL REQUIRED)

add_library(storage_uploader_rdio SHARED
    src/storage_uploader_plugin.cc
    src/s3_client.cc
    src/sftp_client.cc
)

target_include_directories(storage_uploader_rdio
    PRIVATE
        ${Boost_INCLUDE_DIRS}
        ${CURL_INCLUDE_DIRS}
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(storage_uploader_rdio
    PRIVATE
        ${Boost_LIBRARIES}
        CURL::libcurl
)

set_target_properties(storage_uploader_rdio PROPERTIES
    OUTPUT_NAME "storage_uploader_rdio"
    PREFIX "lib"
)

install(TARGETS storage_uploader_rdio LIBRARY DESTINATION lib/trunk-recorder)
