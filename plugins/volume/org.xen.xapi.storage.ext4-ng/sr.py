#!/usr/bin/env python

import os
import os.path
import sys
import urllib.parse

from xapi.storage import log
from xapi.storage.common import call
from xapi.storage.libs import util
from xapi.storage.libs.libcow.coalesce import COWCoalesce
from xapi.storage.libs.libcow.volume import COWVolume
import xapi.storage.api.v5.volume

import importlib


def ext4_mount(dbg, dev_path, mnt_path):
    if not os.path.ismount(mnt_path):
        cmd = ['/usr/bin/mount', '-t', 'ext4', dev_path, mnt_path]
        call(dbg, cmd)


def ext4_unmount(dbg, mnt_path):
    cmd = ['/usr/bin/umount', mnt_path]
    call(dbg, cmd)


@util.decorate_all_routines(util.log_exceptions_in_function)
class Implementation(xapi.storage.api.v5.volume.SR_skeleton):
    MOUNTPOINT_ROOT = '/var/run/sr-mount/'

    @classmethod
    def _mount_path(cls, sr_uuid):
        return os.path.abspath(cls.MOUNTPOINT_ROOT + str(sr_uuid))

    def probe(self, dbg, configuration):
        return {
            'srs': [],
            'uris': []
        }

    def attach(self, dbg, configuration):
        uri = configuration['device']
        log.debug('{}: SR.attach: config={}, uri={}'.format(
            dbg, configuration, uri))

        dev_path = urllib.parse.urlparse(uri).path

        sr = self._mount_path(configuration['sr_uuid'])

        # SR.attach is idempotent
        if os.path.ismount(sr):
            log.debug('{}: SR.attach: already mounted on {}'.format(dbg, sr))
            return sr

        # Mount the ext4 filesystem
        util.mkdir_p(sr)
        ext4_mount(dbg, dev_path, sr)
        log.debug('{}: SR.attach: mounted on {}'.format(dbg, sr))

        # Start GC for this host
        COWCoalesce.start_gc(dbg, 'ext4-ng', sr)

        return sr

    def create(self, dbg, sr_uuid, configuration, name, description):
        log.debug('{}: SR.create: config={}, sr_uuid={}'.format(
            dbg, configuration, sr_uuid))

        uri = configuration['device']
        dev_path = urllib.parse.urlparse(uri).path
        log.debug('{}: SR.create: dev_path={}'.format(dbg, dev_path))

        # Make the filesystem
        cmd = ['/usr/sbin/mkfs.ext4', dev_path]
        call(dbg, cmd)

        # Temporarily mount the filesystem so we can write the SR metadata
        sr = self._mount_path(sr_uuid)
        util.mkdir_p(sr)
        ext4_mount(dbg, dev_path, sr)

        # Create the metadata database
        importlib.import_module('ext4-ng').Callbacks().create_database(sr)

        read_caching = True
        if 'read_caching' in configuration:
            if configuration['read_caching'] not in [
                    'true', 't', 'on', '1', 'yes']:
                read_caching = False

        meta = {
            'name': name,
            'description': description,
            'uri': uri,
            'unique_id': sr_uuid,
            'read_caching': read_caching,
            'keys': {}
        }
        util.update_sr_metadata(dbg, 'file://' + sr, meta)

        ext4_unmount(dbg, sr)

        configuration['sr_uuid'] = sr_uuid
        return configuration

    def _detach(self, dbg, sr):
        # stop GC
        try:
            COWCoalesce.stop_gc(dbg, 'ext4-ng', sr)
        except:
            log.debug('GC already stopped')

    def destroy(self, dbg, sr):
        self._detach(dbg, sr)
        try:
            util.remove_folder_content(sr)
        except:
            pass
        ext4_unmount(dbg, sr)
        os.rmdir(sr)

    def detach(self, dbg, sr):
        self._detach(dbg, sr)
        ext4_unmount(dbg, sr)
        os.rmdir(sr)

    def ls(self, dbg, sr):
        return COWVolume.ls(
            dbg, sr, importlib.import_module('ext4-ng').Callbacks())

    def set_description(self, dbg, sr, new_description):
        util.update_sr_metadata(
            dbg, 'file://' + sr, {'description': new_description})

    def set_name(self, dbg, sr, new_name):
        util.update_sr_metadata(dbg, 'file://' + sr, {'name': new_name})

    def stat(self, dbg, sr):
        if not os.path.isdir(sr):
            raise xapi.storage.api.v5.volume.Sr_not_attached(sr)

        # Get the filesystem size
        statvfs = os.statvfs(sr)
        psize = statvfs.f_blocks * statvfs.f_frsize
        fsize = statvfs.f_bfree * statvfs.f_frsize
        log.debug('{}: statvfs says psize = {}'.format(dbg, psize))

        overprovision = COWVolume.get_sr_provisioned_size(
            sr, importlib.import_module('ext4-ng').Callbacks()) / psize

        meta = util.get_sr_metadata(dbg, 'file://' + sr)
        return {
            'sr': sr,
            'name': meta['name'],
            'description': meta['description'],
            'total_space': psize,
            'free_space': fsize,
            'uuid': meta['unique_id'],
            'overprovision': overprovision,
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
