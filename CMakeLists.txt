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

project(xcp-ng-xapi-storage VERSION 1.0.0 LANGUAGES)

find_package(Python2 COMPONENTS Interpreter REQUIRED)

# ------------------------------------------------------------------------------
# Config.
# ------------------------------------------------------------------------------

set(DATAPATH_PLUGINS
  qdisk
  tapdisk
)

set(VOLUME_PLUGINS
  org.xen.xapi.storage.ext4-ng
  org.xen.xapi.storage.filebased
  org.xen.xapi.storage.nfs-ng
)

set(LIBS_TASKS
  libcow/coalesce.py
  libcow/db_backup.py
)

# ------------------------------------------------------------------------------

set(STORAGE_LIBS_BUILD "${CMAKE_CURRENT_BINARY_DIR}/xcp-ng-xapi-storage-libs.timestamp")

file(GLOB_RECURSE STORAGE_LIBS_SOURCES "xapi/*.py")
add_custom_command(OUTPUT ${STORAGE_LIBS_BUILD}
  COMMAND ${Python2_EXECUTABLE} setup.py install --root ${CMAKE_CURRENT_BINARY_DIR}
  COMMAND ${CMAKE_COMMAND} -E touch ${STORAGE_LIBS_BUILD}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  DEPENDS ${STORAGE_LIBS_SOURCES}
)

add_custom_target(target ALL DEPENDS ${STORAGE_LIBS_BUILD})

# ------------------------------------------------------------------------------
# Install.
# ------------------------------------------------------------------------------

# Install libs and tasks.
list(JOIN LIBS_TASKS "|" LIBS_TASKS_REGEX)
set(LIBS_TASKS_REGEX "(${LIBS_TASKS_REGEX})$")

install(DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/usr"
  DESTINATION .
  PATTERN REGEX ${LIBS_TASKS_REGEX} EXCLUDE
)

install(DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/usr"
  DESTINATION .
  FILE_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  FILES_MATCHING
  PATTERN REGEX ${LIBS_TASKS_REGEX}
)

# Install daemons.
install(FILES daemons/qemuback/qemuback.py
  DESTINATION bin
  PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
)
install(FILES daemons/qemuback/qemuback.service
  DESTINATION lib/systemd/system
)

# Install plugins.
set(PLUGINS_INSTALL_PATH libexec/xapi-storage-script)

function (install_plugin IN OUT TYPE)
  set(BIN_REGEX)
  if (TYPE STREQUAL "DATAPATH")
    set(BIN_REGEX "${IN}/(datapath|plugin).py$")
  else ()
    set(BIN_REGEX "${IN}/(sr|volume|plugin).py$")
  endif ()

  install(DIRECTORY ${IN}
    DESTINATION ${OUT}
    PATTERN REGEX ${BIN_REGEX} EXCLUDE
  )

  install(DIRECTORY ${IN}
    DESTINATION ${OUT}
    FILE_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
    FILES_MATCHING
    PATTERN REGEX ${BIN_REGEX}
  )
endfunction ()

function (create_symlink TARGET LINK_NAME WORKING_DIRECTORY)
  install(CODE "EXECUTE_PROCESS( \
    COMMAND ${CMAKE_COMMAND} -E create_symlink ${TARGET} ${LINK_NAME} \
    WORKING_DIRECTORY ${WORKING_DIRECTORY}
  )")
endfunction ()

foreach (PLUGIN_NAME ${DATAPATH_PLUGINS})
  set(IN_PLUGIN_DIR "plugins/datapath/${PLUGIN_NAME}")
  set(OUT_PLUGIN_DIR "${CMAKE_INSTALL_PREFIX}/${PLUGINS_INSTALL_PATH}/datapath/${PLUGIN_NAME}")

  install_plugin(${IN_PLUGIN_DIR} ${OUT_PLUGIN_DIR} "DATAPATH")

  foreach (LINK_NAME activate attach close deactivate detach open)
    create_symlink("datapath.py" "Datapath.${LINK_NAME}" ${OUT_PLUGIN_DIR})
  endforeach ()
  create_symlink("plugin.py" "Plugin.Query" ${OUT_PLUGIN_DIR})
endforeach ()

foreach (PLUGIN_NAME ${VOLUME_PLUGINS})
  set(IN_PLUGIN_DIR "plugins/volume/${PLUGIN_NAME}")
  set(OUT_PLUGIN_DIR "${CMAKE_INSTALL_PREFIX}/${PLUGINS_INSTALL_PATH}/volume/${PLUGIN_NAME}")

  install_plugin(${IN_PLUGIN_DIR} ${OUT_PLUGIN_DIR} "VOLUME")

  foreach (LINK_NAME clone create destroy resize set set_description set_name snapshot stat unset)
    create_symlink("volume.py" "Volume.${LINK_NAME}" ${OUT_PLUGIN_DIR})
  endforeach ()
  foreach (LINK_NAME attach create destroy detach ls probe set_description set_name stat)
    create_symlink("sr.py" "SR.${LINK_NAME}" ${OUT_PLUGIN_DIR})
  endforeach ()
  foreach (LINK_NAME Query diagnostics)
    create_symlink("plugin.py" "Plugin.${LINK_NAME}" ${OUT_PLUGIN_DIR})
  endforeach ()
endforeach ()

# Install doc.
install(FILES LICENSE README.md
  DESTINATION "share/doc/${CMAKE_PROJECT_NAME}-${PROJECT_VERSION}"
)
