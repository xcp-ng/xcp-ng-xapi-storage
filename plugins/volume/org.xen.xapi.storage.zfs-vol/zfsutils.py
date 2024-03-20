import os
import subprocess

import xapi
from xapi.storage import log

###

# FIXME: this should use zfs API to get a real error code to identify
# when to retry
def call(dbg, cmd_args, error=True, simple=True, expRc=0,
         ntries=1, retry_delay_sec=0.1):
    "Fork of xapi.storage call() with retry on busy."
    while ntries:
        log.debug('%s: Running cmd %s', dbg, cmd_args)
        proc = subprocess.Popen(
            cmd_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True)
        stdout, stderr = proc.communicate()
        if error and proc.returncode != expRc:
            log.error('%s: %s exitted with code %s: %s',
                      dbg, " ".join(cmd_args), proc.returncode, stderr)
            # returncode==2 is an invocation error, no retry needed
            if ntries > 1 and "is busy" in stderr and proc.returncode != 2:
                ntries -= 1
                log.debug("%s: busy detected, retrying %s times", dbg, ntries)
                continue
            raise xapi.InternalError('{} exitted with non-zero code {}: {}'.format(
                " ".join(cmd_args), proc.returncode, stderr))
        if simple:
            return stdout
        return stdout, stderr, proc.returncode

def call_retry(dbg, cmd_args, error=True, simple=True, expRc=0):
    return call(dbg, cmd_args, error=error, simple=simple, expRc=expRc, ntries=10)

###

MOUNT_ROOT = '/var/run/sr-mount'

def zpool_log_state(dbg, label, pool_name):
    cmd = "zfs list -t all -Hp -o name,origin".split()
    log.debug("%s: %s: %s", dbg, label, call(dbg, cmd))

def zvol_path(pool_name, vol_id):
    return "{}/{}".format(pool_name, vol_id)

def zvol_snap_path(pool_name, vol_id, snap_id):
    return "{}/{}@{}".format(pool_name, vol_id, snap_id)

# snapshot id is unique but full name will vary with "promote"
# operations, so we have to walk the full list to know its current
# name
def zvol_find_snap_path(dbg, pool_name, snap_id):
    cmd = "zfs list -t snapshot -Hp -o name".split()
    snap_id = str(snap_id)
    for this_snap_name in call(dbg, cmd).strip().splitlines():
        this_base, this_snap_id = this_snap_name.split("@")
        if this_snap_id == snap_id:
            return this_snap_name
    return None

def zvol_get_snaphots(dbg, vol_name):
    cmd = "zfs list -Hp -t snapshot -o name".split() + [vol_name]
    return call(dbg, cmd).strip().splitlines()

def zsnap_get_dependencies(dbg, snap_name):
    cmd = "zfs list -Hp -o name,origin".split()
    for entry in call(dbg, cmd).strip().splitlines():
        zvol, origin = entry.split("\t")
        if origin == snap_name:
            yield zvol

###

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

def pool_export(dbg, pool_name):
    cmd = "zpool export".split() + [ pool_name ]
    call(dbg, cmd)

def pool_destroy(dbg, pool_name):
    cmd = "zpool destroy".split() + [pool_name]
    call_retry(dbg, cmd)

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

def vol_get_size(dbg, vol_name):
    # size is returned in bytes
    cmd = "zfs get -Hp -o value volsize".split() + [ vol_name ]
    return int(call(dbg, cmd))

def vol_create(dbg, zvol_path, size_mib):
    cmd = ("zfs create -s".split() + [zvol_path]
           + ['-V', str(size_mib)]
           )
    call_retry(dbg, cmd)

def vol_destroy(dbg, zvol_path):
    cmd = "zfs destroy".split() + [zvol_path]
    call_retry(dbg, cmd)

def vol_promote(dbg, zvol_path):
    cmd = "zfs promote".split() + [zvol_path]
    call_retry(dbg, cmd)

def vol_resize(dbg, vol_path, new_size):
    cmd = "zfs set".split() + ['volsize={}'.format(new_size), vol_path]
    call_retry(dbg, cmd)

def vol_snapshot(dbg, snap_name):
    cmd = "zfs snapshot".split() + [snap_name]
    call_retry(dbg, cmd)

def vol_clone(dbg, snap_name, clone_name):
    cmd = "zfs clone".split() + [snap_name, clone_name]
    call_retry(dbg, cmd)

###

# this is really tied to the SR itself and not to ZFS itself, but
# needs to be shared because of SR.ls

def zfsvol_vdi_sanitize(vdi, db):
    """Sanitize vdi metadata object

    When retrieving vdi metadata from the database, it is possible
    that 'vsize' is 'None', if we crashed during a resize operation.
    In this case, query the underlying volume and update 'vsize', both
    in the object and the database
    """
    if vdi.volume.vsize is None:
        vdi.volume.vsize = vol_get_size(dbg, vol_name)
        db.update_volume_vsize(vdi.volume.id, vdi.volume.vsize)
