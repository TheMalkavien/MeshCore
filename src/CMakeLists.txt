# ESP-IDF ignores PlatformIO src_filter. Build only files relevant to the
# requested companion radio profile.
if(NOT DEFINED MESHCORE_BUILD_PROFILE)
  set(MESHCORE_BUILD_PROFILE "heltec_v4_companion_radio_ble_idf")
endif()

if(MESHCORE_BUILD_PROFILE STREQUAL "heltec_v3_companion_radio_ble_idf")
  set(MESHCORE_VARIANT_DIR "variants/heltec_v3")
elseif(MESHCORE_BUILD_PROFILE STREQUAL "heltec_v4_companion_radio_ble_idf")
  set(MESHCORE_VARIANT_DIR "variants/heltec_v4")
else()
  message(FATAL_ERROR "Unsupported MESHCORE_BUILD_PROFILE='${MESHCORE_BUILD_PROFILE}'")
endif()

file(GLOB app_sources
  "${CMAKE_SOURCE_DIR}/src/*.cpp"
  "${CMAKE_SOURCE_DIR}/src/helpers/*.cpp"
  "${CMAKE_SOURCE_DIR}/src/helpers/radiolib/*.cpp"
  "${CMAKE_SOURCE_DIR}/src/helpers/sensors/*.cpp"
  "${CMAKE_SOURCE_DIR}/src/helpers/esp32/*.cpp"
  "${CMAKE_SOURCE_DIR}/src/helpers/bridges/BridgeBase.cpp"
  "${CMAKE_SOURCE_DIR}/src/helpers/ui/MomentaryButton.cpp"
  "${CMAKE_SOURCE_DIR}/src/helpers/ui/SSD1306Display.cpp"
  "${CMAKE_SOURCE_DIR}/${MESHCORE_VARIANT_DIR}/*.cpp"
  "${CMAKE_SOURCE_DIR}/examples/companion_radio/*.cpp"
  "${CMAKE_SOURCE_DIR}/examples/companion_radio/ui-new/*.cpp"
)

list(SORT app_sources)

idf_component_register(
  SRCS ${app_sources}
  INCLUDE_DIRS
    "${CMAKE_SOURCE_DIR}/include"
    "${CMAKE_SOURCE_DIR}/src"
    "${CMAKE_SOURCE_DIR}/${MESHCORE_VARIANT_DIR}"
    "${CMAKE_SOURCE_DIR}/examples/companion_radio"
    "${CMAKE_SOURCE_DIR}/examples/companion_radio/ui-new"
)
