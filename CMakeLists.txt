cmake_minimum_required(VERSION 3.16)
project(minikafka CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

add_compile_options(-Wall -Wextra)

include(FetchContent)
FetchContent_Declare(googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.14.0)
FetchContent_MakeAvailable(googletest)

# common — crc32 + big-endian codec + murmur2, shared by storage/protocol/CLI
add_library(common src/common/crc32.cpp src/common/codec.cpp src/common/murmur2.cpp)
target_include_directories(common PUBLIC src)

# M1 — storage
add_library(storage
    src/storage/record.cpp
    src/storage/segment.cpp
    src/storage/partition.cpp)
target_include_directories(storage PUBLIC src)
target_link_libraries(storage PUBLIC common)

# M2 — protocol (pure encode/decode, no I/O)
add_library(protocol src/protocol/requests.cpp)
target_include_directories(protocol PUBLIC src)
target_link_libraries(protocol PUBLIC common storage)

# M2 — network
add_library(net src/net/socket.cpp src/net/server.cpp)
target_include_directories(net PUBLIC src)
target_link_libraries(net PUBLIC common pthread)

# M3 — group coordinator
add_library(group_lib src/group/coordinator.cpp)
target_include_directories(group_lib PUBLIC src)
target_link_libraries(group_lib PUBLIC protocol)

# M4 — cluster membership + Raft-inspired controller election
add_library(cluster_lib src/cluster/membership.cpp src/cluster/controller.cpp)
target_include_directories(cluster_lib PUBLIC src)
target_link_libraries(cluster_lib PUBLIC net protocol)

# M5 — replication (leader-side ISR tracking + follower puller)
add_library(replication_lib src/replication/isr_manager.cpp src/replication/follower.cpp)
target_include_directories(replication_lib PUBLIC src)
target_link_libraries(replication_lib PUBLIC storage net protocol)

# M2 — broker
add_library(broker_lib src/broker/broker.cpp)
target_include_directories(broker_lib PUBLIC src)
target_link_libraries(broker_lib PUBLIC storage net protocol group_lib cluster_lib replication_lib)

# CLI binaries
foreach(tool mk-broker mk-produce mk-consume mk-admin)
    add_executable(${tool} cli/${tool}.cpp)
    target_include_directories(${tool} PRIVATE src)
    target_link_libraries(${tool} PRIVATE broker_lib)
endforeach()

# tests/ is excluded from the pushed repo (see .gitignore) — guard so a fresh
# clone still configures and builds the CLI binaries cleanly without it.
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/CMakeLists.txt")
    enable_testing()
    add_subdirectory(tests)
endif()
