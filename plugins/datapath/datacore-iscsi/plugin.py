#!/usr/bin/env python3

import os
import sys

import xapi.storage.api.v5.plugin
from xapi.storage import log


class Implementation(xapi.storage.api.v5.plugin.Plugin_skeleton):
    def diagnostics(self, dbg):
        return ""

    def query(self, dbg):
        return {
            "plugin": "datacore-iscsi",
            "name": "DataCore SANsymphony iSCSI Datapath",
            "description": (
                "Datapath for DataCore vDisks served over iSCSI. Calls Serve/Unserve "
                "on the array and waits for the corresponding /dev/disk/by-id/scsi-* "
                "symlink to appear in Dom0."
            ),
            "vendor": "Vates",
            "copyright": "(C) 2026 Vates",
            "version": "0.1",
            "required_api_version": "5.0",
            "features": [],
            "configuration": {},
            "required_cluster_stack": [],
        }


if __name__ == "__main__":
    log.log_call_argv()
    cmd = xapi.storage.api.v5.plugin.Plugin_commandline(Implementation())
    base = os.path.basename(sys.argv[0])
    if base == "Plugin.diagnostics":
        cmd.diagnostics()
    elif base == "Plugin.Query":
        cmd.query()
    else:
        raise xapi.storage.api.v5.plugin.Unimplemented(base)
