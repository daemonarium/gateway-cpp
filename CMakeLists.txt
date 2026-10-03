cmake_minimum_required(VERSION 3.17)
project(waosGateway)

set(CMAKE_CXX_STANDARD 26)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

aux_source_directory(${CMAKE_SOURCE_DIR}/cpp-rpc/src/io IO_FILES)
aux_source_directory(${CMAKE_SOURCE_DIR}/cpp-rpc/src/socket SOCKET_FILES)
set(WASM_FILES ${CMAKE_SOURCE_DIR}/cpp-rpc/src/utils/socket_handlers.cpp)

# Native loopback tests (Boost.UT). WASM trees cannot link Boost::ut executables.
if(NOT WASM)
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

if(WASM)
  add_executable(gateway_session_wasm
    src/module_main.cpp
    ${WASM_FILES}
    ${IO_FILES}
    ${SOCKET_FILES}
  )
  target_compile_options(gateway_session_wasm PRIVATE
    -fwasm-exceptions
  )
  target_link_options(gateway_session_wasm PRIVATE
    -s STANDALONE_WASM=1
    -fwasm-exceptions
    -mbulk-memory
    -Wl,--import-memory
    --no-entry
    -sALLOW_MEMORY_GROWTH=1
    -sINITIAL_MEMORY=2MB
    -sTOTAL_STACK=64000
    -sEXPORTED_FUNCTIONS=_rpc_description,_start,_malloc,_waos_socket_data,_waos_socket_output_drain
    -sERROR_ON_UNDEFINED_SYMBOLS=0
    -Wl,--allow-undefined-file=${CMAKE_SOURCE_DIR}/cpp-rpc/src/examples/modules/allowed-undefined-symbols.syms
  )
  target_include_directories(gateway_session_wasm PRIVATE
    include
    ${CMAKE_SOURCE_DIR}/cpp-rpc/src
  )
  target_compile_definitions(gateway_session_wasm
    PRIVATE
      RPC_SERVER
      RPC_TYPE_DESCRIPTION
  )
  set_target_properties(gateway_session_wasm PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
    OUTPUT_NAME gateway-session
    SUFFIX ".wasm"
  )
endif()
