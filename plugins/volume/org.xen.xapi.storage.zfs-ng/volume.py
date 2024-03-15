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
                    assert not is_snapshot, "snapshots not implemented yet"

                    vol_name = zfsutils.zvol_path(pool_name, vdi.volume.id)
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
                assert not is_snapshot, "snapshots not implemented yet"
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
