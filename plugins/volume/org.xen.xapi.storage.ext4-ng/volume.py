#!/usr/bin/env python

import os
import sys
import xapi.storage.api.v5.volume
from xapi.storage import log
from xapi.storage.libs.libcow.volume_implementation import Implementation
import importlib


def call_volume_command():
    """Parse the arguments and call the required command"""
    log.log_call_argv()
    ext4 = importlib.import_module("ext4-ng")
    cmd = xapi.storage.api.v5.volume.Volume_commandline(
        Implementation(ext4.Callbacks()))
    base = os.path.basename(sys.argv[0])
    if base == "Volume.clone":
        cmd.clone()
    elif base == "Volume.create":
        cmd.create()
    elif base == "Volume.destroy":
        cmd.destroy()
    elif base == "Volume.resize":
        cmd.resize()
    elif base == "Volume.set":
        cmd.set()
    elif base == "Volume.set_description":
        cmd.set_description()
    elif base == "Volume.set_name":
        cmd.set_name()
    elif base == "Volume.snapshot":
        cmd.snapshot()
    elif base == "Volume.stat":
        cmd.stat()
    elif base == "Volume.unset":
        cmd.unset()
    else:
        raise xapi.storage.api.v5.volume.Unimplemented(base)


if __name__ == "__main__":
    call_volume_command()
