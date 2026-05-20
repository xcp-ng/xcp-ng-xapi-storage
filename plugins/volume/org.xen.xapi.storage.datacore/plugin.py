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
            # NOTE: VDI_MIRROR_IN is intentionally NOT advertised yet.
            # `xe vdi-pool-migrate` (live storage XenMotion) needs the
            # destination SR's plugin to expose an NBD endpoint
            # (/services/SM/nbd/MIR.../...) so XAPI's mover can stream
            # mirrored writes into the in-flight VDI. SMAPIv1 plugins
            # implement this via SM; an SMAPIv3 equivalent for our raw-
            # block-device datapath is unimplemented. Offline migration
            # via `xe vdi-copy` works in both directions today and is
            # the supported path. Advertising VDI_MIRROR_IN without the
            # NBD service makes XAPI start the migration then 500 on
            # the PUT, which is worse than a clean "not supported".
            "configuration": {
                "rest-endpoint": "DataCore REST base URL, e.g. https://datacore.example.com",
                "username": "DataCore admin username (Windows account on the SANsymphony server)",
                "password": "DataCore admin password (auto-stored as XAPI secret)",
                "first-pool": "Pool ID on server A: '{ServerA-Id}:{poolA-guid}'",
                "second-pool": "Pool ID on server B: '{ServerB-Id}:{poolB-guid}'",
                "iscsi-portals": "Comma-separated DataCore iSCSI portal IPs, e.g. '192.168.1.87,192.168.1.88'",
                "host-id": "DataCore host object ID for this XCP-ng host (optional after first attach: SR.attach resolves the host-id via /ports lookup once the IQN is registered)",
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
