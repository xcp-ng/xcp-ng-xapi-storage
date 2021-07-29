#!/usr/bin/env python

import importlib
import os
import sys
import urlparse
import uuid
import xapi.storage.api.v5.volume

from xapi.storage import log
from xapi.storage.libs import util
from xapi.storage.libs.libcow.callbacks import VolumeContext
from xapi.storage.libs.libcow.imageformat import ImageFormat
from xapi.storage.libs.libcow.lock import PollLock
from xapi.storage.libs.libcow.volume_implementation import Implementation as \
    DefaultImplementation


@util.decorate_all_routines(util.log_exceptions_in_function)
class Implementation(DefaultImplementation):
    def create(self, dbg, sr, name, description, size, sharable):
        # WORKAROUND: For the moment we can't use a config param to forward the
        # shared_dir string. We use the name field instead.
        # We must open few PRs in the upstream repositories to support that:
        # - xenopsd
        # - xapi-storage-script
        #
        # TODO: The VDI size is useless. So it would be more interesting to add
        # a VDI.import method instead of using VDI.create.

        shared_dir = os.path.normpath(urlparse.urlparse(name).path)
        if not shared_dir or shared_dir == '.':
            raise ValueError('shared_dir param is empty')
        if not os.path.isdir(shared_dir):
            raise ValueError('shared_dir param is not a valid directory')

        statvfs = os.statvfs(os.path.realpath(shared_dir))
        psize = statvfs.f_blocks * statvfs.f_frsize

        with VolumeContext(self.callbacks, sr, 'w') as opq:
            image_type = ImageFormat.IMAGE_DIRECTORY
            image_format = ImageFormat.get_format(image_type)
            vdi_uuid = str(uuid.uuid4())

            with PollLock(opq, 'gl', self.callbacks, 0.5):
                with self.callbacks.db_context(opq) as db:
                    volume = db.insert_new_volume(psize, image_type)
                    db.insert_vdi(
                        name, description, vdi_uuid, volume.id, sharable)
                    volume_path = self.callbacks.volumeGetPath(
                        opq, str(volume.id))
            os.symlink(shared_dir, volume_path)

            vdi_uri = self.callbacks.getVolumeUriPrefix(opq) + vdi_uuid

        return {
            'key': vdi_uuid,
            'uuid': vdi_uuid,
            'name': name,
            'description': description,
            'read_write': True,
            'virtual_size': psize,
            'physical_utilisation': 0,
            'uri': [image_format.uri_prefix + vdi_uri],
            'sharable': sharable,
            'keys': {}
        }


def call_volume_command():
    """Parse the arguments and call the required command"""
    log.log_call_argv()
    fsp = importlib.import_module("fsp")
    cmd = xapi.storage.api.v5.volume.Volume_commandline(
        Implementation(fsp.Callbacks()))
    base = os.path.basename(sys.argv[0])
    if base == "Volume.create":
        cmd.create()
    elif base == "Volume.destroy":
        cmd.destroy()
    elif base == "Volume.set":
        cmd.set()
    elif base == "Volume.set_description":
        cmd.set_description()
    elif base == "Volume.set_name":
        cmd.set_name()
    elif base == "Volume.stat":
        cmd.stat()
    elif base == "Volume.unset":
        cmd.unset()
    else:
        raise xapi.storage.api.v5.volume.Unimplemented(base)


if __name__ == "__main__":
    call_volume_command()
