# ==============================================================================
# CMakeLists.txt
#
# Copyright (C) 2019  xcp-ng-xapi-storage
# Copyright (C) 2019  Vates SAS
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# ==============================================================================

cmake_minimum_required(VERSION 3.13.1)

# ------------------------------------------------------------------------------

project(xcp-ng-xapi-storage VERSION 1.0.2 LANGUAGES)

find_package(Python2 COMPONENTS Interpreter REQUIRED)

# ------------------------------------------------------------------------------
# Config.
# ------------------------------------------------------------------------------

set(DATAPATH_PLUGINS
  qdisk
  tapdisk
)

set(VOLUME_PLUGINS
  org.xen.xapi.storage.zfs-vol
  org.xen.xapi.storage.ext4-ng
  org.xen.xapi.storage.filebased
  org.xen.xapi.storage.nfs-ng
  org.xen.xapi.storage.raw-device
)

set(LIBS_TASKS
  libcow/coalesce.py
  libcow/db_backup.py
)

# ------------------------------------------------------------------------------

set(STORAGE_LIBS_TARGET "${CMAKE_CURRENT_BINARY_DIR}/xcp-ng-xapi-storage-libs.timestamp")
set(STORAGE_LIBS "${CMAKE_CURRENT_BINARY_DIR}/setup_py")

file(GLOB_RECURSE STORAGE_LIBS_SOURCES "xapi/*.py")
add_custom_command(OUTPUT ${STORAGE_LIBS_TARGET}
  COMMAND ${Python2_EXECUTABLE} setup.py install --root "${STORAGE_LIBS}"
  COMMAND ${CMAKE_COMMAND} -E touch ${STORAGE_LIBS_TARGET}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  DEPENDS setup.py ${STORAGE_LIBS_SOURCES}
)

add_custom_target(target ALL DEPENDS ${STORAGE_LIBS_TARGET})

# ------------------------------------------------------------------------------
# Install.
# ------------------------------------------------------------------------------

# Not sexy but in our case it's ok. Install in lib, not lib64!
set(CMAKE_SIZEOF_VOID_P 4)
include(GNUInstallDirs)

# Install libs and tasks.
list(JOIN LIBS_TASKS "|" LIBS_TASKS_REGEX)
set(LIBS_TASKS_REGEX "(${LIBS_TASKS_REGEX})$")

install(DIRECTORY "${STORAGE_LIBS}/usr/"
  DESTINATION .
  PATTERN REGEX ${LIBS_TASKS_REGEX} EXCLUDE
)

install(DIRECTORY "${STORAGE_LIBS}/usr/"
  DESTINATION .
  FILE_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  FILES_MATCHING
  PATTERN REGEX ${LIBS_TASKS_REGEX}
)

# Install daemons.
install(FILES daemons/qemuback/qemuback.py
  DESTINATION ${CMAKE_INSTALL_BINDIR}
  PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
)
install(FILES daemons/qemuback/qemuback.service
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/systemd/system
)

# Install plugins.
set(PLUGINS_INSTALL_PATH "${CMAKE_INSTALL_FULL_LIBEXECDIR}/xapi-storage-script")
# Use an absolute path to create correctly symlinks.
# When cmake is called like this: `/usr/bin/cmake -DCMAKE_INSTALL_PREFIX:PATH=toto`, the prefix path
# is relative. It is absolute when cmake is called: `/usr/bin/cmake -DCMAKE_INSTALL_PREFIX=toto`.
get_filename_component(PLUGINS_INSTALL_PATH ${PLUGINS_INSTALL_PATH} ABSOLUTE BASE_DIR ${CMAKE_CURRENT_BINARY_DIR})

function (install_plugin PLUGIN_NAME IN OUT TYPE)
  set(IN "${IN}/${PLUGIN_NAME}")
  set(OUT "${OUT}/${PLUGIN_NAME}")

  set(BIN_REGEX)
  if (TYPE STREQUAL "DATAPATH")
    set(BIN_REGEX "${IN}/(datapath|plugin).py$")
  else ()
    set(BIN_REGEX "${IN}/(sr|volume|plugin).py$")
  endif ()

  install(DIRECTORY "${IN}/"
    DESTINATION ${OUT}
    PATTERN REGEX ${BIN_REGEX} EXCLUDE
  )

  install(DIRECTORY "${IN}/"
    DESTINATION ${OUT}
    FILE_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
    FILES_MATCHING
    PATTERN REGEX ${BIN_REGEX}
  )
endfunction ()

function (create_plugin_symlink PLUGIN_NAME TARGET LINK_NAME PLUGINS_DIR)
  set(RESULT_SYMLINK RESULT_SYMLINK_${PLUGIN_NAME}_${LINK_NAME})

  install(CODE "
    # Use DESTDIR if necessary!
    set(WORKING_DIRECTORY \"\$ENV{DESTDIR}/${PLUGINS_DIR}/${PLUGIN_NAME}\")
    message(\"Create symlink: ${TARGET} ${LINK_NAME} in \${WORKING_DIRECTORY}\")
    execute_process(
      COMMAND ${CMAKE_COMMAND} -E create_symlink ${TARGET} ${LINK_NAME}
      WORKING_DIRECTORY \${WORKING_DIRECTORY}
      RESULT_VARIABLE ${RESULT_SYMLINK}
    )
    if (NOT \"\${${RESULT_SYMLINK}}\" STREQUAL \"0\")
      message(FATAL_ERROR \"Failed to create symlink: \${${RESULT_SYMLINK}}...\")
    endif ()
  ")
endfunction ()

foreach (PLUGIN_NAME ${DATAPATH_PLUGINS})
  set(IN_PLUGINS_DIR "plugins/datapath")
  set(OUT_PLUGINS_DIR "${PLUGINS_INSTALL_PATH}/datapath")

  install_plugin(${PLUGIN_NAME} ${IN_PLUGINS_DIR} ${OUT_PLUGINS_DIR} "DATAPATH")

  foreach (LINK_NAME activate attach close deactivate detach open)
    create_plugin_symlink(${PLUGIN_NAME} "datapath.py" "Datapath.${LINK_NAME}" ${OUT_PLUGINS_DIR})
  endforeach ()
  create_plugin_symlink(${PLUGIN_NAME} "plugin.py" "Plugin.Query" ${OUT_PLUGINS_DIR})
endforeach ()

foreach (PLUGIN_NAME ${VOLUME_PLUGINS})
  set(IN_PLUGINS_DIR "plugins/volume")
  set(OUT_PLUGINS_DIR "${PLUGINS_INSTALL_PATH}/volume")

  install_plugin(${PLUGIN_NAME} ${IN_PLUGINS_DIR} ${OUT_PLUGINS_DIR} "VOLUME")

  foreach (LINK_NAME clone create destroy resize set set_description set_name snapshot stat unset)
    create_plugin_symlink(${PLUGIN_NAME} "volume.py" "Volume.${LINK_NAME}" ${OUT_PLUGINS_DIR})
  endforeach ()
  foreach (LINK_NAME attach create destroy detach ls probe set_description set_name stat)
    create_plugin_symlink(${PLUGIN_NAME} "sr.py" "SR.${LINK_NAME}" ${OUT_PLUGINS_DIR})
  endforeach ()
  foreach (LINK_NAME Query diagnostics)
    create_plugin_symlink(${PLUGIN_NAME} "plugin.py" "Plugin.${LINK_NAME}" ${OUT_PLUGINS_DIR})
  endforeach ()
endforeach ()

# Install doc.
install(FILES LICENSE README.md
  DESTINATION ${CMAKE_INSTALL_DOCDIR}
)
