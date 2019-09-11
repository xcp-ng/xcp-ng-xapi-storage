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

function (create_symlink TARGET LINK_NAME WORKING_DIRECTORY)
  install(CODE "EXECUTE_PROCESS( \
    COMMAND ${CMAKE_COMMAND} -E create_symlink ${TARGET} ${LINK_NAME} \
    WORKING_DIRECTORY ${WORKING_DIRECTORY}
  )")
endfunction ()

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

# Install libs.
install(DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/usr/"
  DESTINATION ${CMAKE_INSTALL_PREFIX}
)

# Install daemons.
install(FILES daemons/qemuback/qemuback.py
  DESTINATION "${CMAKE_INSTALL_PREFIX}/bin"
)
install(FILES daemons/qemuback/qemuback.service
  DESTINATION "${CMAKE_INSTALL_PREFIX}/systemd/system"
)

# Install plugins.
install(DIRECTORY plugins/
  DESTINATION "${CMAKE_INSTALL_PREFIX}/libexec/xapi-storage-script"
)

set(PLUGINS_INSTALL_PATH "${CMAKE_INSTALL_PREFIX}/libexec/xapi-storage-script")

# Create symlinks for datapath plugins.
foreach (PLUGIN_NAME qdisk tapdisk)
  set(PLUGIN_DIR "${PLUGINS_INSTALL_PATH}/datapath/${PLUGIN_NAME}")
  foreach (LINK_NAME activate attach close deactivate detach open)
    create_symlink("datapath.py" "Datapath.${LINK_NAME}" "${PLUGIN_DIR}")
  endforeach ()
  create_symlink("plugin.py" "Plugin.Query" "${PLUGIN_DIR}")
endforeach ()

# Create symlinks for volume plugins.
foreach (PLUGIN_NAME ext4-ng filebased nfs-ng)
  set(PLUGIN_DIR "${PLUGINS_INSTALL_PATH}/volume/org.xen.xapi.storage.${PLUGIN_NAME}")
  foreach (LINK_NAME clone create destroy resize set set_description set_name snapshot stat unset)
    create_symlink("volume.py" "Volume.${LINK_NAME}" "${PLUGIN_DIR}")
  endforeach ()
  foreach (LINK_NAME attach create destroy detach ls probe set_description set_name stat)
    create_symlink("sr.py" "SR.${LINK_NAME}" "${PLUGIN_DIR}")
  endforeach ()
  foreach (LINK_NAME Query diagnostics)
    create_symlink("plugin.py" "Plugin.${LINK_NAME}" "${PLUGIN_DIR}")
  endforeach ()
endforeach ()

# Install doc.
install(FILES LICENSE README.md
  DESTINATION "${CMAKE_INSTALL_PREFIX}/share/doc/${CMAKE_PROJECT_NAME}-${PROJECT_VERSION}"
)
