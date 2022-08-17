#!/usr/bin/env python

import importlib
import os
import sys
import urlparse
import uuid
import xapi.storage.api.v5.volume

from xapi.storage.common import call
from xapi.storage import log
from xapi.storage.libs import util
from xapi.storage.libs.libcow.callbacks import VolumeContext
from xapi.storage.libs.libcow.imageformat import ImageFormat
from xapi.storage.libs.libcow.lock import PollLock
from xapi.storage.libs.libcow.volume_implementation import Implementation as \
    DefaultImplementation

import volume

ZPOOL_BIN = 'zpool'
ZFS_BIN = 'zfs'

@util.decorate_all_routines(util.log_exceptions_in_function)
class Implementation(DefaultImplementation):
    def create(self, dbg, sr, name, description, size, sharable):
        str_size = str(size);
        with VolumeContext(self.callbacks, sr, 'w') as opq:
            image_type = ImageFormat.IMAGE_RAW
            image_format = ImageFormat.get_format(image_type)
            vdi_uuid = str(uuid.uuid4())

            with PollLock(opq, 'gl', self.callbacks, 0.5):
                with self.callbacks.db_context(opq) as db:
                    volume = db.insert_new_volume(size, image_type)
                    db.insert_vdi(
                        name, description, vdi_uuid, volume.id, sharable)
                    path = os.path.basename(sr) + '/'+ str(volume.id)
                    cmd = [
                        'zfs', 'create',
                        '-V', str_size, path
                    ]
                    call(dbg, cmd)

            vdi_uri = self.callbacks.getVolumeUriPrefix(opq) + vdi_uuid

        return {
            'key': vdi_uuid,
            'uuid': vdi_uuid,
            'name': name,
            'description': description,
            'read_write': True,
            'virtual_size': size,
            'physical_utilisation': size,
            'uri': [image_format.uri_prefix + vdi_uri],
            'sharable': False,
            'keys': {}
        }

    def destroy(self, dbg, sr, key):
        cb = self.callbacks
        with VolumeContext(cb, sr, 'w') as opq:
            with PollLock(opq, 'gl', cb, 0.5):
                with cb.db_context(opq) as db:
                    vdi = db.get_vdi_by_id(key)
                    is_snapshot = bool(vdi.volume.snap)
                    if is_snapshot:
                        path = os.path.basename(sr) + '/'+ str(vdi.volume.parent_id) + '@' + str(vdi.volume.id)
                        path_clone = os.path.basename(sr) + '/'+ str(vdi.volume.id)
                        # destroy clone first
                        cmd = [
                            'zfs', 'destroy',
                            path_clone
                        ]
                        call(dbg, cmd)
                    else:
                        path = os.path.basename(sr) + '/'+ str(vdi.volume.id)
                    cmd = [
                        'zfs', 'destroy',
                        path
                    ]
                    log.error('cmd= {}'.format(cmd))
                    # NOTE: by returning like this, xapi will think that the cmd successes
                    # and remove the vdi from its db.
                    # The revert-from-snap will never happen if this fails.
                    try:
                        call(dbg, cmd)
                    except Exception as e:
                        log.error('Command has failed, maybe it is because it still has children!')
                        return
                    db.delete_vdi(key)
                with cb.db_context(opq) as db:
                    cb.volumeDestroy(opq, str(vdi.volume.id))
                    db.delete_volume(vdi.volume.id)

    def stat(self, dbg, sr, key):
        image_format = None
        cb = self.callbacks
        with VolumeContext(cb, sr, 'r') as opq:
            with cb.db_context(opq) as db:
                vdi = db.get_vdi_by_id(key)
                image_format = ImageFormat.get_format(vdi.image_type)
                # TODO: handle this better
                #_vdi_sanitize(vdi, opq, db, cb)
                is_snapshot = bool(vdi.volume.snap)
                if is_snapshot:
                    path = os.path.basename(sr) + '/'+ str(vdi.volume.parent_id) + '@' + str(vdi.volume.id)
                else:
                    path = os.path.basename(sr) + '/'+ str(vdi.volume.id)
                custom_keys = db.get_vdi_custom_keys(vdi.uuid)
                vdi_uuid = vdi.uuid

        cmd = [
            ZFS_BIN, 'get',
            '-o', 'value', '-Hp', 'used,avail',
            path
        ]
        out = call(dbg, cmd).splitlines()
        if is_snapshot:
            psize = int(out[0])
        else:
            psize = int(out[0]) + int(out[1])
        vdi_uri = cb.getVolumeUriPrefix(opq) + vdi_uuid
        return {
            'uuid': vdi.uuid,
            'key': vdi.uuid,
            'name': vdi.name,
            'description': vdi.description,
            'read_write': not bool(vdi.volume.snap),
            'virtual_size': vdi.volume.vsize,
            'physical_utilisation': psize,
            'uri': [image_format.uri_prefix + vdi_uri],
            'keys': custom_keys,
            'sharable': False
        }

    def snapshot(self, dbg, sr, key):
        snap_uuid = str(uuid.uuid4())
        cb = self.callbacks
        with VolumeContext(cb, sr, 'w') as opq:
            result_volume_id = ''
            with PollLock(opq, 'gl', cb, 0.5):
                with cb.db_context(opq) as db:
                    vdi = db.get_vdi_by_id(key)
                    image_format = ImageFormat.get_format(vdi.image_type)
                    image_utils = image_format.image_utils

                    vol_id = (vdi.volume.id if vdi.volume.snap == 0 else
                              vdi.volume.parent_id)

                    vol_path = cb.volumeGetPath(opq, str(vol_id))
                    snap_volume = db.insert_child_volume(vol_id,
                                                         vdi.volume.vsize)
                    db.set_volume_as_snapshot(snap_volume.id)
                    db.insert_vdi(vdi.name, vdi.description,
                                  snap_uuid, snap_volume.id, False)
                    result_volume_id = str(snap_volume.id)
                    path = os.path.basename(sr) + '/'+ str(vdi.volume.id) + '@' + str(snap_volume.id)
                    cmd = [
                        ZFS_BIN, 'snapshot',
                        path
                    ]
                    log.error('snapshot: {}'.format(cmd))
                    call(dbg, cmd)
                    cmd = [
                        ZFS_BIN, 'clone',
                        path, os.path.basename(sr) + '/' + str(snap_volume.id)
                    ]
                    log.error('clone: {}'.format(cmd))
                    call(dbg, cmd)
        psize = 0
        snap_uri = cb.getVolumeUriPrefix(opq) + snap_uuid
        return {
            'uuid': snap_uuid,
            'key': snap_uuid,
            'name': result_volume_id,
            'description': vdi.description,
            'read_write': False,
            'virtual_size': vdi.volume.vsize,
            'physical_utilisation': psize,
            'uri': [image_format.uri_prefix + snap_uri],
            'keys': {},
            'sharable': False
        }

    # clone only works on snapshots
    # fails otherwise
    def clone(self, dbg, sr, key):
        snap_uuid = str(uuid.uuid4())
        cb = self.callbacks
        with VolumeContext(cb, sr, 'w') as opq:
            result_volume_id = ''
            with PollLock(opq, 'gl', cb, 0.5):
                with cb.db_context(opq) as db:
                    vdi = db.get_vdi_by_id(key)
                    if vdi.volume.snap == 0:
                        log.error('Only snapshots can be cloned!')
                        raise
                    image_format = ImageFormat.get_format(vdi.image_type)
                    image_utils = image_format.image_utils
                    parent_vol_id = vdi.volume.parent_id
                    cloned_volume = db.insert_new_volume(vdi.volume.vsize, vdi.image_type)
                    db.insert_vdi(
                        vdi.name, vdi.description, snap_uuid, cloned_volume.id, False)
                    result_volume_id = str(cloned_volume.id)
                    snap_path = os.path.basename(sr) + '/'+ str(parent_vol_id) + '@' + str(vdi.volume.id)
                    clone_path = os.path.basename(sr) + '/'+ result_volume_id
                    cmd = [
                        ZFS_BIN, 'clone',
                        snap_path, clone_path
                    ]
                    log.error('cmd: {}'.format(cmd))
                    call(dbg, cmd)
                    # promote clone
                    cmd = [
                        ZFS_BIN, 'promote',
                        clone_path
                    ]
                    log.error('cmd: {}'.format(cmd))
                    call(dbg, cmd)
                    children = db.get_children(parent_vol_id)
                    for child in children:
                         db.update_volume_parent(child.id, cloned_volume.id)
                    parent_vdi = db.get_vdi_for_volume(parent_vol_id)
                    # remove parent from db
                    db.delete_vdi(parent_vdi.uuid)
                    cb.volumeDestroy(opq, str(parent_vdi.volume.id))
                    db.delete_volume(parent_vdi.volume.id)
                    # destroy parent
                    parent_path = os.path.basename(sr) + '/'+ str(parent_vol_id)
                    cmd = [
                        ZFS_BIN, 'destroy',
                        parent_path
                    ]
                    log.error('cmd: {}'.format(cmd))
                    call(dbg, cmd)
        psize = 0
        snap_uri = cb.getVolumeUriPrefix(opq) + snap_uuid
        return {
            'uuid': snap_uuid,
            'key': snap_uuid,
            'name': result_volume_id,
            'description': vdi.description,
            'read_write': True,
            'virtual_size': vdi.volume.vsize,
            'physical_utilisation': psize,
            'uri': [image_format.uri_prefix + snap_uri],
            'keys': {},
            'sharable': False
        }

def call_volume_command():
    """Parse the arguments and call the required command"""
    log.log_call_argv()
    fsp = importlib.import_module("zfs-ng")
    cmd = xapi.storage.api.v5.volume.Volume_commandline(
        Implementation(fsp.Callbacks()))
    base = os.path.basename(sys.argv[0])
    if base == "Volume.create":
        cmd.create()
    elif base == "Volume.clone":
        cmd.clone()
    elif base == "Volume.destroy":
        cmd.destroy()
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
