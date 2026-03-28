cmake_minimum_required(VERSION 3.16)
project(mi_band10_viewer C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

find_package(PkgConfig REQUIRED)
find_package(OpenSSL REQUIRED)
pkg_check_modules(GTK REQUIRED IMPORTED_TARGET gtk4)
pkg_check_modules(ADW REQUIRED IMPORTED_TARGET libadwaita-1)
pkg_check_modules(GLIB REQUIRED IMPORTED_TARGET glib-2.0 gio-2.0)
pkg_check_modules(PROTOBUF_C REQUIRED IMPORTED_TARGET libprotobuf-c)
pkg_check_modules(SQLITE3 REQUIRED IMPORTED_TARGET sqlite3)
find_library(BLUETOOTH_LIB bluetooth REQUIRED)

add_executable(libreband
    src/main.c
    src/mib10_crypto.c
    src/mib10_activity.c
    src/mib10_bluez.c
    src/mib10_wire.c
    src/mib10_ui.c
    src/gb_db.c
    src/xiaomi.pb-c.c
)

target_link_libraries(libreband PRIVATE
    PkgConfig::GTK
    PkgConfig::ADW
    PkgConfig::GLIB
    PkgConfig::PROTOBUF_C
    PkgConfig::SQLITE3
    ${BLUETOOTH_LIB}
    OpenSSL::Crypto
    m
)
