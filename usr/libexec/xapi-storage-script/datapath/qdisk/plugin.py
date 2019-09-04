#!/usr/bin/env python

import os
import sys
import xapi.storage.api.v4.plugin
from xapi.storage import log


class Implementation(xapi.storage.api.v4.plugin.Plugin_skeleton):

    def query(self, dbg):
        return {
            "plugin": "qdisk",
            "name": "The QEMU qdisk user-space datapath plugin",
            "description": ("This plugin manages and configures qdisk"
                            " instances backend for qcow2 image format built"
                            " using libcow."),
            "vendor": "Citrix",
            "copyright": "(C) 2015 Citrix Inc",
            "version": "3.0",
            "required_api_version": "4.0",
            "features": [],
            "configuration": {},
            "required_cluster_stack": []}


if __name__ == "__main__":
    log.log_call_argv()
    CMD = xapi.storage.api.v4.plugin.Plugin_commandline(Implementation())
    CMD_BASE = os.path.basename(sys.argv[0])
    if CMD_BASE == "Plugin.Query":
        CMD.query()
    else:
        raise xapi.storage.api.v4.plugin.Unimplemented(CMD_BASE)
