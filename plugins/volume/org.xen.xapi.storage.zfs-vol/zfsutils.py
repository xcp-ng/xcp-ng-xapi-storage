import os

from xapi.storage.common import call

MOUNT_ROOT = '/var/run/sr-mount'

def pool_mountpoint(dbg, pool_name):
    cmd = "zfs get mountpoint -H -o value".split() + [ pool_name ]
    return call(dbg, cmd).strip()

def pool_create(dbg, pool_name, devs):
    cmd = ("zpool create".split() + [pool_name]
           + ['-R', MOUNT_ROOT]
           + devs)
    call(dbg, cmd)
