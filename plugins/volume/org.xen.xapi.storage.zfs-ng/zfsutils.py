import os

from xapi.storage.common import call

MOUNT_ROOT = '/var/run/sr-mount'

def zvol_path(pool_name, vol_id):
    return "{}/{}".format(pool_name, vol_id)

###

def pool_mountpoint(dbg, pool_name):
    cmd = "zfs get mountpoint -H -o value".split() + [ pool_name ]
    return call(dbg, cmd).strip()

def pool_create(dbg, pool_name, devs):
    cmd = ("zpool create".split() + [pool_name] # FIXME '-f' ?
           + ['-R', MOUNT_ROOT]
           + devs)
    call(dbg, cmd)

def pool_destroy(dbg, pool_name):
    cmd = "zpool destroy".split() + [pool_name]
    call(dbg, cmd)

def pool_get_size(dbg, sr_path):
    # size is returned in bytes
    cmd = "zpool get -Hp -o value size".split() + [ sr_path ]
    return int(call(dbg, cmd))

def pool_get_free_space(dbg, sr_path):
    # size is returned in bytes
    cmd = "zpool get -Hp -o value free".split() + [ sr_path ]
    return int(call(dbg, cmd))

###

def vol_get_used(dbg, vol_name):
    # size is returned in bytes
    cmd = "zfs get -Hp -o value used".split() + [ vol_name ]
    return int(call(dbg, cmd))

def vol_create(dbg, zvol_path, size_mib):
    cmd = ("zfs create".split() + [zvol_path]
           + ['-V', str(size_mib)]
           )
    call(dbg, cmd)
