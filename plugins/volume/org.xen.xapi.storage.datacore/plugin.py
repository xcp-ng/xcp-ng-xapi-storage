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
            "plugin": "datacore",
            "name": "DataCore SANsymphony Volume plugin",
            "description": "Maps each VDI 1:1 to a DataCore vDisk via the SANsymphony REST API.",
            "vendor": "Vates",
            "copyright": "(C) 2026 Vates",
            "version": "0.1",
            "required_api_version": "5.0",
            "features": [
                "SR_ATTACH",
                "SR_DETACH",
                "SR_CREATE",
                "SR_PROBE",
                "SR_METADATA",
                "VDI_CREATE",
                "VDI_DESTROY",
                "VDI_ATTACH",
                "VDI_DETACH",
                "VDI_ACTIVATE",
                "VDI_DEACTIVATE",
                "VDI_RESIZE",
                "VDI_RESIZE_ONLINE",
                "VDI_SNAPSHOT",
                "VDI_CLONE",
                "VDI_UPDATE",
            ],
            "configuration": {
                "rest-endpoint": "DataCore REST base URL, e.g. https://datacore.example.com",
                "username": "DataCore admin username (Windows account on the SANsymphony server)",
                "password": "DataCore admin password (auto-stored as XAPI secret)",
                "first-pool": "Pool ID on server A: '{ServerA-Id}:{poolA-guid}'",
                "second-pool": "Pool ID on server B: '{ServerB-Id}:{poolB-guid}'",
                "iscsi-portals": "Comma-separated DataCore iSCSI portal IPs, e.g. '192.168.1.87,192.168.1.88'",
                "host-id": "DataCore host object ID for this XCP-ng host (initiator IQN will be auto-registered against it)",
                "tls-verify": "Validate TLS cert? Default 'false' (DataCore default install uses a self-signed cert)",
            },
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
