"""Shared test setup for the DataCore SMAPIv3 plugin.

Stubs the `xapi.storage` modules the plugin imports at runtime (only
present on XCP-ng dom0) so tests can `import datacoreapi` / `import volume`
/ `import sr` / `import datapath` on any developer laptop with just
`pytest` and `requests` installed.

Also extends `sys.path` so the plugin modules resolve without their normal
`/usr/libexec/xapi-storage-script/...` install layout.
"""

import os
import sys
import types


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
VOLUME_PLUGIN_DIR = os.path.join(
    REPO_ROOT, "plugins/volume/org.xen.xapi.storage.datacore"
)
DATAPATH_PLUGIN_DIR = os.path.join(
    REPO_ROOT, "plugins/datapath/datacore-iscsi"
)


def _stub_xapi_storage():
    """Install minimal stand-ins for the xapi.storage.* modules the plugin
    imports. These are real Python packages on dom0; locally we only need
    enough structure that `from xapi.storage import log` and
    `class X(xapi.storage.api.v5.volume.SR_skeleton)` resolve."""

    if "xapi.storage" in sys.modules:
        return  # already stubbed in this pytest session

    xapi = types.ModuleType("xapi")
    storage = types.ModuleType("xapi.storage")
    api = types.ModuleType("xapi.storage.api")
    v5 = types.ModuleType("xapi.storage.api.v5")
    volume = types.ModuleType("xapi.storage.api.v5.volume")
    datapath = types.ModuleType("xapi.storage.api.v5.datapath")
    log = types.ModuleType("xapi.storage.log")

    class _Skeleton:
        pass

    class _NotFound(Exception):
        pass

    class _Unimpl(Exception):
        pass

    volume.Volume_skeleton = _Skeleton
    volume.SR_skeleton = _Skeleton
    volume.Volume_does_not_exist = _NotFound
    volume.Unimplemented = _Unimpl
    volume.Volume_commandline = lambda x: None
    volume.SR_commandline = lambda x: None

    datapath.Datapath_skeleton = _Skeleton
    datapath.Unimplemented = _Unimpl
    datapath.Datapath_commandline = lambda x: None

    def _noop(*a, **k):
        pass

    log.debug = log.info = log.warning = log.error = log.critical = _noop
    log.log_call_argv = _noop

    xapi.storage = storage
    storage.api = api
    api.v5 = v5
    v5.volume = volume
    v5.datapath = datapath
    storage.log = log

    sys.modules.update({
        "xapi": xapi,
        "xapi.storage": storage,
        "xapi.storage.api": api,
        "xapi.storage.api.v5": v5,
        "xapi.storage.api.v5.volume": volume,
        "xapi.storage.api.v5.datapath": datapath,
        "xapi.storage.log": log,
    })


_stub_xapi_storage()

# The plugin modules sit alongside each other in two directories. Put them
# on sys.path so plain `import datacoreapi` / `import volume` / `import sr`
# / `import datapath` work — same as how the deployed dom0 layout works.
for d in (VOLUME_PLUGIN_DIR, DATAPATH_PLUGIN_DIR):
    if d not in sys.path:
        sys.path.insert(0, d)
