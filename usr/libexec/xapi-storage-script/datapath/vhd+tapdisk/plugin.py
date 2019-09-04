#!/usr/bin/env python

import os
import sys
import xapi
import xapi.storage.api.v5.plugin
from xapi.storage import log


class Implementation(xapi.storage.api.v5.plugin.Plugin_skeleton):

    def query(self, dbg):
        return {
            "plugin": "vhd+tapdisk",
            "name": "The cow + tapdisk user-space datapath plugin",
            "description": ("This plugin manages and configures tapdisk"
                            " instances backend for cow image format built"
                            " using libcow, like file or lvm based Volume"
                            " plugins"),
            "vendor": "Citrix",
            "copyright": "(C) 2015 Citrix Inc",
            "version": "3.0",
            "required_api_version": "5.0",
            "features": [
                "NONPERSISTENT",  # Retire this one
                "VDI_NONPERSISTENT"],
            "configuration": {},
            "required_cluster_stack": []}


if __name__ == "__main__":
    log.log_call_argv()
    cmd = xapi.storage.api.v5.plugin.Plugin_commandline(Implementation())
    base = os.path.basename(sys.argv[0])
    if base == "Plugin.Query":
        cmd.query()
    else:
        raise xapi.storage.api.v5.plugin.Unimplemented(base)
