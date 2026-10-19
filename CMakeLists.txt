cmake_minimum_required(VERSION 3.17)
project(gateway_cpp LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 26)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT TARGET cpp_rpc)
  if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/extern/rpc-cpp/CMakeLists.txt)
    add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/extern/rpc-cpp cpp_rpc)
  else()
    message(FATAL_ERROR
      "cpp_rpc not found. Add rpc-cpp at extern/rpc-cpp before configuring gateway-cpp.")
  endif()
endif()

add_library(gateway_cpp STATIC
  src/module_main.cpp
  ${cpp_rpc_SOURCE_DIR}/src/utils/socket_handlers.cpp
)
target_include_directories(gateway_cpp PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(gateway_cpp PUBLIC cpp_rpc)
target_compile_definitions(gateway_cpp PRIVATE RPC_SERVER RPC_TYPE_DESCRIPTION)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  include(FetchContent)
  FetchContent_Declare(ut
    GIT_REPOSITORY https://github.com/boost-ext/ut.git
    GIT_TAG v2.0.0
  )
  FetchContent_MakeAvailable(ut)

  if(NOT WASM)
    add_library(gateway_socket_test_stub STATIC
      ${cpp_rpc_SOURCE_DIR}/src/test/socket_flush_stub.c
    )
    target_include_directories(gateway_socket_test_stub PRIVATE
      ${cpp_rpc_SOURCE_DIR}/src
    )

    foreach(name session_rpc session_mux session_stream pipe)
      add_executable(gateway_${name}_ut src/test/${name}_ut.cpp)
      target_include_directories(gateway_${name}_ut PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${cpp_rpc_SOURCE_DIR}/src/test
      )
      target_link_libraries(gateway_${name}_ut PRIVATE
        Boost::ut cpp_rpc gateway_socket_test_stub
      )
      target_compile_definitions(gateway_${name}_ut PRIVATE
        GATEWAY_TESTDATA_DIR="${CMAKE_CURRENT_SOURCE_DIR}/testdata"
      )
      set_target_properties(gateway_${name}_ut PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/tests
      )
      add_test(NAME gateway_${name}_ut
        COMMAND gateway_${name}_ut
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/tests
      )
    endforeach()
  endif()

  if(WASM)
    add_executable(gateway_session_wasm
      src/module_main.cpp
      ${cpp_rpc_SOURCE_DIR}/src/utils/socket_handlers.cpp
    )
    target_link_libraries(gateway_session_wasm PRIVATE cpp_rpc)
    target_include_directories(gateway_session_wasm PRIVATE
      ${CMAKE_CURRENT_SOURCE_DIR}/include
    )
    target_compile_definitions(gateway_session_wasm PRIVATE
      RPC_SERVER RPC_TYPE_DESCRIPTION
    )
    target_compile_options(gateway_session_wasm PRIVATE -fwasm-exceptions)
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
      -Wl,--allow-undefined-file=${cpp_rpc_SOURCE_DIR}/src/examples/modules/allowed-undefined-symbols.syms
    )
    set_target_properties(gateway_session_wasm PROPERTIES
      RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
      OUTPUT_NAME gateway-session
      SUFFIX ".wasm"
    )
  endif()

  enable_testing()
endif()
