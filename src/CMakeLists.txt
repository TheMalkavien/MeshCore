# ESP-IDF ignores PlatformIO src_filter. Keep this source list aligned
# with heltec_v4_companion_radio_ble_idf so only relevant files are built.
file(GLOB app_sources
  "${CMAKE_SOURCE_DIR}/src/*.cpp"
  "${CMAKE_SOURCE_DIR}/src/helpers/*.cpp"
  "${CMAKE_SOURCE_DIR}/src/helpers/radiolib/*.cpp"
  "${CMAKE_SOURCE_DIR}/src/helpers/sensors/*.cpp"
  "${CMAKE_SOURCE_DIR}/src/helpers/esp32/*.cpp"
  "${CMAKE_SOURCE_DIR}/src/helpers/bridges/BridgeBase.cpp"
  "${CMAKE_SOURCE_DIR}/src/helpers/ui/MomentaryButton.cpp"
  "${CMAKE_SOURCE_DIR}/src/helpers/ui/SSD1306Display.cpp"
  "${CMAKE_SOURCE_DIR}/variants/heltec_v4/*.cpp"
  "${CMAKE_SOURCE_DIR}/examples/companion_radio/*.cpp"
  "${CMAKE_SOURCE_DIR}/examples/companion_radio/ui-new/*.cpp"
)

list(SORT app_sources)

idf_component_register(
  SRCS ${app_sources}
  INCLUDE_DIRS
    "${CMAKE_SOURCE_DIR}/include"
    "${CMAKE_SOURCE_DIR}/src"
    "${CMAKE_SOURCE_DIR}/variants/heltec_v4"
    "${CMAKE_SOURCE_DIR}/examples/companion_radio"
    "${CMAKE_SOURCE_DIR}/examples/companion_radio/ui-new"
)
