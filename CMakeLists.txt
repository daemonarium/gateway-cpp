cmake_minimum_required(VERSION 3.17)
project(waosGateway)

set(CMAKE_CXX_STANDARD 26)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Native loopback tests (Boost.UT). WASM trees cannot link Boost::ut executables.
if(NOT WASM)
  aux_source_directory(${CMAKE_SOURCE_DIR}/cpp-rpc/src/io IO_FILES)
  aux_source_directory(${CMAKE_SOURCE_DIR}/cpp-rpc/src/socket SOCKET_FILES)

  add_executable(gateway_session_rpc_ut
    src/test/session_rpc_ut.cpp
    ${IO_FILES}
    ${SOCKET_FILES}
  )
  target_include_directories(gateway_session_rpc_ut PRIVATE
    include
    ${CMAKE_SOURCE_DIR}/cpp-rpc/src
    ${CMAKE_SOURCE_DIR}/cpp-rpc/src/test
  )
  target_link_libraries(gateway_session_rpc_ut PRIVATE Boost::ut socket_test_stub)
  target_compile_definitions(gateway_session_rpc_ut PRIVATE
    GATEWAY_TESTDATA_DIR="${CMAKE_SOURCE_DIR}/gateway/testdata"
  )
  set_target_properties(gateway_session_rpc_ut PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/tests
  )

  add_test(NAME gateway_session_rpc_ut COMMAND gateway_session_rpc_ut WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/tests)

  add_executable(gateway_session_mux_ut
    src/test/session_mux_ut.cpp
    ${IO_FILES}
    ${SOCKET_FILES}
  )
  target_include_directories(gateway_session_mux_ut PRIVATE
    include
    ${CMAKE_SOURCE_DIR}/cpp-rpc/src
    ${CMAKE_SOURCE_DIR}/cpp-rpc/src/test
  )
  target_link_libraries(gateway_session_mux_ut PRIVATE Boost::ut socket_test_stub)
  target_compile_definitions(gateway_session_mux_ut PRIVATE
    GATEWAY_TESTDATA_DIR="${CMAKE_SOURCE_DIR}/gateway/testdata"
  )
  set_target_properties(gateway_session_mux_ut PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/tests
  )

  add_test(NAME gateway_session_mux_ut COMMAND gateway_session_mux_ut WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/tests)
endif()
