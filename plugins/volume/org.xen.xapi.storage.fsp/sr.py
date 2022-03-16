#!/usr/bin/env python

import os
import os.path
import sys
import urlparse
import uuid

from xapi.storage import log
from xapi.storage.libs import util
from xapi.storage.libs.libcow.volume import COWVolume
from xapi.storage.libs.libcow.callbacks import VolumeContext
from xapi.storage.libs.libcow.imageformat import ImageFormat
from xapi.storage.libs.libcow.lock import PollLock
import xapi.storage.api.v5.volume

import importlib

DIRECTORY_KEY = 'directory'
DIRECTORIES_PATH = 'directories'

@util.decorate_all_routines(util.log_exceptions_in_function)
class Implementation(xapi.storage.api.v5.volume.SR_skeleton):
    def probe(self, dbg, configuration):
        return {
            'srs': [],
            'uris': []
        }

    def attach(self, dbg, configuration):
        uri = configuration['file-uri']
        log.debug('{}: SR.attach: config={}, uri={}'.format(
            dbg, configuration, uri))

        sr = urlparse.urlparse(uri).path
        return sr

    def create(self, dbg, sr_uuid, configuration, name, description):
        log.debug('{}: SR.create: config={}, sr_uuid={}'.format(
            dbg, configuration, sr_uuid))

        uri = configuration['file-uri']
        sr = urlparse.urlparse(uri).path
        log.debug('{}: SR.create: sr={}'.format(dbg, sr))

        # Create the metadata database
        importlib.import_module('fsp').Callbacks().create_database(sr)

        meta = {
            'name': name,
            'description': description,
            'uri': uri,
            'unique_id': sr_uuid,
            'read_caching': False,
            'keys': {}
        }
        util.update_sr_metadata(dbg, 'file://' + sr, meta)

        return configuration

    def destroy(self, dbg, sr):
        util.remove_folder_content(sr)

    def detach(self, dbg, sr):
        # Nothing todo.
        pass

    def ls(self, dbg, sr):
        self._check_paths(dbg, sr)
        # create new VDIs
        self._scan(dbg, sr)
        fsp = importlib.import_module('fsp')
        return COWVolume.ls(dbg, sr, fsp.Callbacks())

    def set_description(self, dbg, sr, new_description):
        util.update_sr_metadata(
            dbg, 'file://' + sr, {'description': new_description})

    def set_name(self, dbg, sr, new_name):
        util.update_sr_metadata(dbg, 'file://' + sr, {'name': new_name})

    def _create_vdi(self, sr, name, path):
        fsp = importlib.import_module("fsp")
        cb = fsp.Callbacks()
        description = ""
        sharable = False
        vdi_uuid = str(uuid.uuid4())
        with VolumeContext(cb, sr, 'w') as opq:
            image_type = ImageFormat.IMAGE_DIRECTORY
            image_format = ImageFormat.get_format(image_type)
            with PollLock(opq, 'gl', cb, 0.5):
                with cb.db_context(opq) as db:
                    volume = db.insert_new_volume(0, image_type)
                    db.insert_vdi(name, description, vdi_uuid, volume.id, sharable)
                    volume_path = cb.volumeGetPath(opq, str(volume.id))
                    db.set_vdi_custom_key(vdi_uuid, DIRECTORY_KEY, path, True)
            os.symlink(path, volume_path)
        return vdi_uuid

    def _check_paths(self, dbg, sr):
        all_vdis = {}
        fsp = importlib.import_module("fsp")
        cb = fsp.Callbacks()
        with VolumeContext(cb, sr, 'w') as opq:
            with PollLock(opq, 'gl', cb, 0.5):
                with cb.db_context(opq) as db:
                    # TODO: Filter on `DIRECTORY_KEY` instead of fetching all keys.
                    all_vdis = db.get_all_vdi_custom_keys(True)
        for vdi_uuid, vdi in all_vdis.iteritems():
            directory = vdi.get(DIRECTORY_KEY)
            if directory is not None and not os.path.exists(directory):
                COWVolume.destroy(dbg, sr, vdi_uuid, cb)
                log.debug('VDI destroyed due to broken symlink or missing directory {}: uuid={}'.format(
                    directory, vdi_uuid))

    def _get_all_volume_directories(self, sr):
        all_vdis = {}
        fsp = importlib.import_module("fsp")
        cb = fsp.Callbacks()
        with VolumeContext(cb, sr, 'w') as opq:
            with PollLock(opq, 'gl', cb, 0.5):
                with cb.db_context(opq) as db:
                    all_vdis = db.get_all_vdi_custom_keys(True)
        all_paths = set()
        for vdi in all_vdis.itervalues():
            directory = vdi.get(DIRECTORY_KEY)
            if directory is not None:
                all_paths.add(directory)
        return all_paths

    def _scan(self, dbg, sr):
        meta = util.get_sr_metadata(dbg, 'file://' + sr)
        # TODO: A future implementation should remove the DIRECTORIES_PATH directory
        sr_dir = meta['uri'] + '/' + DIRECTORIES_PATH
        all_paths = self._get_all_volume_directories(sr)
        if os.path.exists(sr_dir):
            for filename in os.listdir(sr_dir):
                path = os.path.join(sr_dir, filename)
                # isdir() supports both symlinks + directories
                if os.path.isdir(path):
                    if not path in all_paths:
                        vdi_uuid = self._create_vdi(sr, filename, path)
                        log.debug('New VDI with path {}: uuid={}'.format(filename, vdi_uuid))

    def stat(self, dbg, sr):
        if not os.path.isdir(sr):
            raise xapi.storage.api.v5.volume.Sr_not_attached(sr)

        # Just get the filesystem size because this SR is essentially a small
        # database + symlinks to used paths.
        # This is an approximation. `overprovision` is ignored.
        statvfs = os.statvfs(sr)
        psize = statvfs.f_blocks * statvfs.f_frsize
        fsize = statvfs.f_bfree * statvfs.f_frsize
        log.debug('{}: statvfs says psize = {}'.format(dbg, psize))

        meta = util.get_sr_metadata(dbg, 'file://' + sr)
        return {
            'sr': sr,
            'name': meta['name'],
            'description': meta['description'],
            'total_space': psize,
            'free_space': fsize,
            'uuid': meta['unique_id'],
            'overprovision': 0,
            'datasources': [],
            'clustered': True,
            'health': ['Healthy', '']
        }


if __name__ == '__main__':
    log.log_call_argv()
    cmd = xapi.storage.api.v5.volume.SR_commandline(Implementation())
    base = os.path.basename(sys.argv[0])
    if base == 'SR.probe':
        cmd.probe()
    elif base == 'SR.attach':
        cmd.attach()
    elif base == 'SR.create':
        cmd.create()
    elif base == 'SR.destroy':
        cmd.destroy()
    elif base == 'SR.detach':
        cmd.detach()
    elif base == 'SR.ls':
        cmd.ls()
    elif base == 'SR.set_description':
        cmd.set_description()
    elif base == 'SR.set_name':
        cmd.set_name()
    elif base == 'SR.stat':
        cmd.stat()
    else:
        raise xapi.storage.api.v5.volume.Unimplemented(base)
