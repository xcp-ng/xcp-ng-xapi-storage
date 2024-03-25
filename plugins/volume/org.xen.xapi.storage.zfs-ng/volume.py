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

def call_volume_command():
    """Parse the arguments and call the required command"""
    log.log_call_argv()
    fsp = importlib.import_module("zfs-ng")
    cmd = xapi.storage.api.v5.volume.Volume_commandline(
        Implementation(fsp.Callbacks()))
    base = os.path.basename(sys.argv[0])
    if base == "Volume.create":
        cmd.create()
    elif base == "Volume.set":
        cmd.set()
    elif base == "Volume.unset":
        cmd.unset()
    else:
        raise xapi.storage.api.v5.volume.Unimplemented(base)

if __name__ == "__main__":
    call_volume_command()
