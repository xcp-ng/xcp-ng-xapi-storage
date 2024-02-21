#!/usr/bin/env python

import importlib
import os
import sys
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
        devices = util.get_sr_metadata(dbg, 'file://' + sr)['devices']
        devices = [os.path.normpath(x) for x in devices]

        with VolumeContext(self.callbacks, sr, 'w') as opq:
            image_type = ImageFormat.IMAGE_RAW
            image_format = ImageFormat.get_format(image_type)
            vdi_uuid = str(uuid.uuid4())

            with PollLock(opq, 'gl', self.callbacks, 0.5):
                with self.callbacks.db_context(opq) as db:
                    # List all used devices.
                    used_devices = [
                        os.path.realpath(self.callbacks.volumeGetPath(opq, str(vol.id)))
                        for vol in db.get_all_volumes()]

                    # Find first free device with the best size.
                    free_device = None
                    psize = sys.maxsize
                    for device in devices:
                        if os.path.realpath(device) not in used_devices:
                            device_size = util.get_physical_file_size(device)
                            if device_size >= size and device_size < psize:
                                free_device = device
                                psize = device_size

                    if not free_device:
                        # TODO: Maybe find a better exception.
                        raise ValueError('No free device found in config')

                    volume = db.insert_new_volume(psize, image_type)
                    db.insert_vdi(
                        name, description, vdi_uuid, volume.id, sharable)
                    volume_path = self.callbacks.volumeGetPath(
                        opq, str(volume.id))
            os.symlink(free_device, volume_path)

            vdi_uri = self.callbacks.getVolumeUriPrefix(opq) + vdi_uuid

        return {
            'key': vdi_uuid,
            'uuid': vdi_uuid,
            'name': name,
            'description': description,
            'read_write': True,
            'virtual_size': psize,
            'physical_utilisation': psize,
            'uri': [image_format.uri_prefix + vdi_uri],
            'sharable': sharable,
            'keys': {}
        }


def call_volume_command():
    """Parse the arguments and call the required command"""
    log.log_call_argv()
    cmd = xapi.storage.api.v5.volume.Volume_commandline(
        Implementation(importlib.import_module('raw-device').Callbacks()))
    base = os.path.basename(sys.argv[0])
    if base == "Volume.clone":
        cmd.clone()
    elif base == "Volume.create":
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
