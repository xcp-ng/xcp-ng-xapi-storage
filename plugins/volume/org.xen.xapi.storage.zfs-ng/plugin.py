#!/usr/bin/env python

import os
import sys
import xapi.storage.api.v5.plugin
from xapi.storage import log

class Implementation(xapi.storage.api.v5.plugin.Plugin_skeleton):

    def diagnostics(self, dbg):
        return "No diagnostic data to report"

    def query(self, dbg):
        return {
            "plugin": "zfs-ng",
            "name": "ZFS Volume plugin",
            "description": ("This plugin manages ZFS volumes"),
            "vendor": "None",
            "copyright": "(C) 2021-2024 Vates",
            "version": "3.0",
            "required_api_version": "5.0",
            "features": [
                "VDI_CREATE",
                "VDI_DESTROY",
                "VDI_RESIZE",
            ],
            "configuration": {},
            "required_cluster_stack": []}


if __name__ == "__main__":
    log.log_call_argv()
    cmd = xapi.storage.api.v5.plugin.Plugin_commandline(Implementation())
    base = os.path.basename(sys.argv[0])
    if base == 'Plugin.diagnostics':
        cmd.diagnostics()
    elif base == 'Plugin.Query':
        cmd.query()
    else:
        raise xapi.storage.api.v5.plugin.Unimplemented(base)
