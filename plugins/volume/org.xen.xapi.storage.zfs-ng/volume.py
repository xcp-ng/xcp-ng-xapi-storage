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

import zfsutils

@util.decorate_all_routines(util.log_exceptions_in_function)
class Implementation(DefaultImplementation):
    "Volume driver to provide volumes from zvol's"

    def create(self, dbg, sr, name, description, size, sharable):
        meta = util.get_sr_metadata(dbg, 'file://' + sr)
        pool_name = meta["zpool"]

        with VolumeContext(self.callbacks, sr, 'w') as opq:
            # FIXME how should we choose image format?
            image_type = ImageFormat.IMAGE_RAW
            image_format = ImageFormat.get_format(image_type)
            vdi_uuid = str(uuid.uuid4())

            with PollLock(opq, 'gl', self.callbacks, 0.5):
                with self.callbacks.db_context(opq) as db:
                    volume = db.insert_new_volume(size, image_type)
                    db.insert_vdi(
                        name, description, vdi_uuid, volume.id, sharable)
                    path = zfsutils.zvol_path(pool_name, volume.id)
                    zfsutils.vol_create(dbg, path, size)

            vdi_uri = self.callbacks.getVolumeUriPrefix(opq) + vdi_uuid

        return {
            'key': vdi_uuid,    # FIXME check this
            'uuid': vdi_uuid,
            'name': name,
            'description': description,
            'read_write': True,
            'virtual_size': size,
            'physical_utilisation': size, # FIXME - incidently psize gets null in the db
            'uri': [image_format.uri_prefix + vdi_uri],
            'sharable': False,
            'keys': {}
        }

    def destroy(self, dbg, sr, key):
        meta = util.get_sr_metadata(dbg, 'file://' + sr)
        pool_name = meta["zpool"]

        cb = self.callbacks
        need_destroy_clone = False
        with VolumeContext(cb, sr, 'w') as opq:
            with PollLock(opq, 'gl', cb, 0.5):
                with cb.db_context(opq) as db:
                    vdi = db.get_vdi_by_id(key)
                    is_snapshot = vdi.volume.snap
                    if is_snapshot:
                        snap_name = zfsutils.zvol_find_snap_path(dbg, pool_name, vdi.volume.id)
                        zfsutils.zpool_log_state(dbg, "before destroy {}".format(snap_name), pool_name)
                        if snap_name is None:
                            raise Exception("zfs snapshot object %s/*@%s not found on disk" %
                                            (pool_name, vdi.volume.id))
                        if tuple(zfsutils.zsnap_get_dependencies(dbg, snap_name)):
                            raise Exception("zfs snapshot %s destruction blocked by clones" %
                                            (snap_name,))

                        zfsutils.vol_destroy(dbg, snap_name)
                    else:
                        vol_name = zfsutils.zvol_path(pool_name, vdi.volume.id)
                        zfsutils.zpool_log_state(dbg, "before destroy {}".format(vol_name), pool_name)
                        # for each snapshot select a clone
                        vol_dependencies = []
                        for vol_snap in zfsutils.zvol_get_snaphots(dbg, vol_name):
                            snap_dependencies = tuple(zfsutils.zsnap_get_dependencies(dbg, vol_snap))
                            if snap_dependencies:
                                vol_dependencies.append(snap_dependencies[0])
                            else:
                                raise Exception(
                                    "zfs volume %s destruction blocked by uncloned snapshot %s" %
                                    (vol_name, vol_snap))
                        if vol_dependencies:
                            for dep in vol_dependencies:
                                zfsutils.vol_promote(dbg, dep)
                            zfsutils.zpool_log_state(dbg, "after promotions", pool_name)
                        zfsutils.vol_destroy(dbg, vol_name)

                    db.delete_vdi(key)

                    cb.volumeDestroy(opq, str(vdi.volume.id))
                    db.delete_volume(vdi.volume.id)

    def resize(self, dbg, sr, key, new_size):
        meta = util.get_sr_metadata(dbg, 'file://' + sr)
        pool_name = meta["zpool"]

        cb = self.callbacks
        with VolumeContext(cb, sr, 'r') as opq:
            with cb.db_context(opq) as db:
                vdi = db.get_vdi_by_id(key)
                vol_name = zfsutils.zvol_path(pool_name, vdi.volume.id)
                db.update_volume_vsize(vdi.volume.id, new_size)
                zfsutils.vol_resize(dbg, vol_name, new_size)

    def stat(self, dbg, sr, key):
        meta = util.get_sr_metadata(dbg, 'file://' + sr)
        pool_name = meta["zpool"]

        image_format = None
        cb = self.callbacks
        with VolumeContext(cb, sr, 'r') as opq:
            with cb.db_context(opq) as db:
                vdi = db.get_vdi_by_id(key)
                image_format = ImageFormat.get_format(vdi.image_type)
                # TODO: handle this better
                # _vdi_sanitize(vdi, opq, db, cb)
                is_snapshot = vdi.volume.snap
                if is_snapshot:
                    vol_name = zfsutils.zvol_find_snap_path(dbg, pool_name, vdi.volume.id)
                    if vol_name is None:
                        raise Exception("snapshot volume %s not found on disk" % (vdi.volume.id))
                else:
                    vol_name = zfsutils.zvol_path(pool_name, vdi.volume.id)
                custom_keys = db.get_vdi_custom_keys(vdi.uuid)

        psize = zfsutils.vol_get_used(dbg, vol_name) # FIXME check
        vdi_uri = cb.getVolumeUriPrefix(opq) + vdi.uuid

        return {
            'uuid': vdi.uuid,
            'key': vdi.uuid,    # FIXME check this
            'name': vdi.name,
            'description': vdi.description,
            'read_write': not is_snapshot,
            'virtual_size': vdi.volume.vsize,
            'physical_utilisation': psize,
            'uri': [image_format.uri_prefix + vdi_uri],
            'keys': custom_keys,
            'sharable': False
        }

    def snapshot(self, dbg, sr, key):
        meta = util.get_sr_metadata(dbg, 'file://' + sr)
        pool_name = meta["zpool"]

        snap_uuid = str(uuid.uuid4())
        cb = self.callbacks
        with VolumeContext(cb, sr, 'w') as opq:
            with PollLock(opq, 'gl', cb, 0.5):
                with cb.db_context(opq) as db:
                    vdi = db.get_vdi_by_id(key)
                    image_format = ImageFormat.get_format(vdi.image_type)

                    vol_id = (vdi.volume.id if vdi.volume.snap == 0 else
                              vdi.volume.parent_id)

                    snap_volume = db.insert_child_volume(vol_id, vdi.volume.vsize,
                                                         is_snapshot=True)
                    snap_name = zfsutils.zvol_snap_path(pool_name, vol_id, snap_volume.id)

                    zfsutils.vol_snapshot(dbg, snap_name)

                    db.insert_vdi(vdi.name, vdi.description,
                                  snap_uuid, snap_volume.id, vdi.sharable)

        psize = zfsutils.vol_get_used(dbg, snap_name) # FIXME check
        snap_uri = cb.getVolumeUriPrefix(opq) + snap_uuid
        return {
            'uuid': snap_uuid,
            'key': snap_uuid,
            'name': str(snap_volume.id),
            'description': vdi.description,
            'read_write': False,
            'virtual_size': vdi.volume.vsize,
            'physical_utilisation': psize,
            'uri': [image_format.uri_prefix + snap_uri],
            'keys': {},
            'sharable': False
        }


    def clone(self, dbg, sr, key):
        meta = util.get_sr_metadata(dbg, 'file://' + sr)
        pool_name = meta["zpool"]

        clone_uuid = str(uuid.uuid4())
        cb = self.callbacks
        with VolumeContext(cb, sr, 'w') as opq:
            with PollLock(opq, 'gl', cb, 0.5):
                with cb.db_context(opq) as db:
                    vdi = db.get_vdi_by_id(key)
                    if not vdi.volume.snap:
                        raise Exception('Only snapshots can be cloned!')
                    snap_name = zfsutils.zvol_find_snap_path(dbg, pool_name, vdi.volume.id)

                    image_format = ImageFormat.get_format(vdi.image_type)
                    image_utils = image_format.image_utils

                    # (if this is a snapshot) we could want to take
                    # the snap's parent_id as clone's parent, but it
                    # might as well be destroyed already
                    cloned_volume = db.insert_child_volume(vdi.volume.id, vdi.volume.vsize)

                    clone_path = zfsutils.zvol_path(pool_name, cloned_volume.id)
                    zfsutils.vol_clone(dbg, snap_name, clone_path)

                    db.insert_vdi(vdi.name, vdi.description,
                                  clone_uuid, cloned_volume.id, vdi.sharable)

        psize = zfsutils.vol_get_used(dbg, snap_name) # FIXME check
        clone_uri = cb.getVolumeUriPrefix(opq) + clone_uuid
        return {
            'uuid': clone_uuid,
            'key': clone_uuid,
            'name': str(cloned_volume.id),
            'description': vdi.description,
            'read_write': True,
            'virtual_size': vdi.volume.vsize,
            'physical_utilisation': psize,
            'uri': [image_format.uri_prefix + clone_uri],
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
    elif base == "Volume.destroy":
        cmd.destroy()
    elif base == "Volume.resize":
        cmd.resize()
    elif base == "Volume.snapshot":
        cmd.snapshot()
    elif base == "Volume.clone":
        cmd.clone()
    elif base == "Volume.stat":
        cmd.stat()
    elif base == "Volume.set":
        cmd.set()
    elif base == "Volume.unset":
        cmd.unset()
    elif base == "Volume.set_name":
        cmd.set_name()
    elif base == "Volume.set_description":
        cmd.set_description()
    else:
        raise xapi.storage.api.v5.volume.Unimplemented(base)

if __name__ == "__main__":
    call_volume_command()
