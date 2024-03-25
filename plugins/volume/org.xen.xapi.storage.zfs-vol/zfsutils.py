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

def pool_import(dbg, pool_name):
    cmd = ("zpool import".split()
           + ['-R', MOUNT_ROOT]    # -R ensure that <pool_name> is mounted
           + [pool_name])
    call(dbg, cmd)
