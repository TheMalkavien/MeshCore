set(component_sources "")

if(DEFINED MESHCORE_BUILD_PROFILE AND
    MESHCORE_BUILD_PROFILE STREQUAL "heltec_v3_companion_radio_ble_idf")
  file(GLOB meshcore_core_sources
    "${CMAKE_CURRENT_LIST_DIR}/*.cpp"
  )
  file(GLOB meshcore_helper_sources
    "${CMAKE_CURRENT_LIST_DIR}/helpers/*.cpp"
  )
  file(GLOB meshcore_radiolib_sources
    "${CMAKE_CURRENT_LIST_DIR}/helpers/radiolib/*.cpp"
  )
  file(GLOB meshcore_esp32_sources
    "${CMAKE_CURRENT_LIST_DIR}/helpers/esp32/*.cpp"
  )
  file(GLOB meshcore_companion_sources
    "${CMAKE_CURRENT_LIST_DIR}/../examples/companion_radio/*.cpp"
    "${CMAKE_CURRENT_LIST_DIR}/../examples/companion_radio/ui-new/*.cpp"
  )
  file(GLOB meshcore_ed25519_sources
    "${CMAKE_CURRENT_LIST_DIR}/../lib/ed25519/*.c"
  )

  set(component_sources
    ${meshcore_core_sources}
    ${meshcore_helper_sources}
    ${meshcore_radiolib_sources}
    ${meshcore_esp32_sources}
    ${meshcore_companion_sources}
    ${meshcore_ed25519_sources}
    "${CMAKE_CURRENT_LIST_DIR}/helpers/bridges/BridgeBase.cpp"
    "${CMAKE_CURRENT_LIST_DIR}/helpers/sensors/EnvironmentSensorManager.cpp"
    "${CMAKE_CURRENT_LIST_DIR}/helpers/ui/MomentaryButton.cpp"
    "${CMAKE_CURRENT_LIST_DIR}/helpers/ui/SSD1306Display.cpp"
    "${CMAKE_CURRENT_LIST_DIR}/../variants/heltec_v3/target.cpp"
  )
else()
  file(GLOB_RECURSE component_sources
    "${CMAKE_CURRENT_LIST_DIR}/*.c"
    "${CMAKE_CURRENT_LIST_DIR}/*.cpp"
  )
endif()

list(REMOVE_DUPLICATES component_sources)

idf_component_register(
  SRCS ${component_sources}
  INCLUDE_DIRS
    "."
    "${CMAKE_CURRENT_LIST_DIR}/../lib/ed25519"
    "${CMAKE_CURRENT_LIST_DIR}/../variants/heltec_v3"
    "${CMAKE_CURRENT_LIST_DIR}/../examples/companion_radio/ui-new"
  REQUIRES
    bt
)
