cmake_minimum_required(VERSION 3.16.3)
project(mysql-dds)

# Sketch serialization is dependent on endianness (float serialization in particular)
include (TestBigEndian)
TEST_BIG_ENDIAN(IS_BIG_ENDIAN)
if(IS_BIG_ENDIAN)
    message(FATAL_ERROR "Cannot compile sketch functions on big endian architecture")
endif()

set(CMAKE_CXX_STANDARD 17)

add_library(mysql-dds SHARED src/dds.cc)

execute_process(COMMAND bash -c "mysql_config --include | cut -c 3-"
  OUTPUT_VARIABLE MYSQL_INCLUDE OUTPUT_STRIP_TRAILING_WHITESPACE
)

target_include_directories(mysql-dds PRIVATE ${MYSQL_INCLUDE})
target_compile_options(mysql-dds PRIVATE -O3 -fno-omit-frame-pointer -ftls-model=local-exec -Wall -Wextra -Werror -Wformat-security -Wvla -Wundef -Wmissing-format-attribute -Woverloaded-virtual -Wcast-qual -Wno-null-conversion -Wno-unused-private-field -Wdeprecated -Wextra-semi -Wnon-virtual-dtor)

# GoogleTest framework
include(FetchContent)
FetchContent_Declare(
        googletest
        DOWNLOAD_EXTRACT_TIMESTAMP true
        URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
FetchContent_MakeAvailable(googletest)
enable_testing()
add_executable(
        dds_test
        src/dds_test.cc
        src/dds.cc
)
target_link_libraries(
        dds_test
        GTest::gtest_main
)
target_include_directories(dds_test PRIVATE ${MYSQL_INCLUDE})

include(GoogleTest)
gtest_discover_tests(dds_test)
