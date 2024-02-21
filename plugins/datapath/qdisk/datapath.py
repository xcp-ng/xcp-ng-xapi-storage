#!/usr/bin/env python
"""
Datapath for QEMU qdisk
"""

import urllib.parse
import os
import sys
import xapi
import xapi.storage.api.v5.datapath
import xapi.storage.api.v5.volume
import importlib
from xapi.storage.libs.libcow.datapath import QdiskDatapath
from xapi.storage import log


def get_sr_callbacks(dbg, uri):
    u = urllib.parse.urlparse(uri)
    sr = u.netloc
    sys.path.insert(
        0,
        '/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.' + sr)
    mod = importlib.import_module(sr)
    return mod.Callbacks()


class Implementation(xapi.storage.api.v5.datapath.Datapath_skeleton):
    """
    Datapath implementation
    """
    def activate(self, dbg, uri, domain):
        callbacks = get_sr_callbacks(dbg, uri)
        return QdiskDatapath.activate(dbg, uri, domain, callbacks)

    def attach(self, dbg, uri, domain):
        callbacks = get_sr_callbacks(dbg, uri)
        return QdiskDatapath.attach(dbg, uri, domain, callbacks)

    def deactivate(self, dbg, uri, domain):
        callbacks = get_sr_callbacks(dbg, uri)
        return QdiskDatapath.deactivate(dbg, uri, domain, callbacks)

    def detach(self, dbg, uri, domain):
        callbacks = get_sr_callbacks(dbg, uri)
        return QdiskDatapath.detach(dbg, uri, domain, callbacks)

    def open(self, dbg, uri, domain):
        callbacks = get_sr_callbacks(dbg, uri)
        return QdiskDatapath.epc_open(dbg, uri, domain, callbacks)

    def close(self, dbg, uri):
        callbacks = get_sr_callbacks(dbg, uri)
        return QdiskDatapath.epc_close(dbg, uri, callbacks)


if __name__ == "__main__":
    log.log_call_argv()
    CMD = xapi.storage.api.v5.datapath.Datapath_commandline(Implementation())
    CMD_BASE = os.path.basename(sys.argv[0])
    if CMD_BASE == "Datapath.activate":
        CMD.activate()
    elif CMD_BASE == "Datapath.attach":
        CMD.attach()
    elif CMD_BASE == "Datapath.close":
        CMD.close()
    elif CMD_BASE == "Datapath.deactivate":
        CMD.deactivate()
    elif CMD_BASE == "Datapath.detach":
        CMD.detach()
    elif CMD_BASE == "Datapath.open":
        CMD.open()
    else:
        raise xapi.storage.api.v5.datapath.Unimplemented(CMD_BASE)
