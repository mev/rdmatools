cmake_minimum_required(VERSION 3.20)
project(rdmatools_0_3_ssh)

set(CMAKE_CXX_STANDARD 17)

include_directories(src)
include_directories(src/apps)
include_directories(src/common)

add_executable(rdmatools_0_3_ssh
            src/common/optparse.hpp
            src/common/rdma_rand.hpp
            src/common/rdma_context.hpp
            src/common/rdma_utils.hpp
            src/common/rdma_config.hpp
            src/common/rdma_endpoint.hpp
            src/common/rdma_rand.hpp

            src/common/tcpip_client.hpp
            main.cpp src/common/rdma_track.hpp src/common/rdma_crc.hpp)

target_link_libraries(rdmatools_0_3_ssh pthread ibverbs z)
