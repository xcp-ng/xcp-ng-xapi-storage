#!/usr/bin/env python

import os
import os.path
import sys
import errno
import urllib.parse

import xapi.storage.api.v5.volume
from xapi.storage import log
from xapi.storage.common import call
from xapi.storage.libs import util
from xapi.storage.libs.libcow.coalesce import COWCoalesce
from xapi.storage.libs.libcow.volume import COWVolume

import importlib


@util.decorate_all_routines(util.log_exceptions_in_function)
class Implementation(xapi.storage.api.v5.volume.SR_skeleton):
    # Base of SR mounts, SRs will mount in a folder with their GUID
    MOUNTPOINT_ROOT = '/var/run/sr-mount/'

    @classmethod
    def _mount_path(cls, sr_uuid):
        return os.path.abspath(cls.MOUNTPOINT_ROOT + str(sr_uuid))

    def _mount(self, dbg, nfs_path, sr_uuid):
        """Mount the NFS share to a temporary local folder"""
        mnt_path = self._mount_path(sr_uuid)
        try:
            os.makedirs(mnt_path)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(mnt_path):
                pass
            else:
                raise
        if not os.path.ismount(mnt_path):
            cmd = ['/usr/bin/mount', '-t', 'nfs', '-o',
                   'noatime,nodiratime', nfs_path, mnt_path]
            call(dbg, cmd)
        return mnt_path

    def _unmount(self, dbg, mnt_path):
        """Unmount the NFS share"""
        cmd = ['/usr/bin/umount', mnt_path]
        call(dbg, cmd)

    def probe(self, dbg, configuration):
        uri = configuration['uri']
        u = urllib.parse.urlparse(uri)
        if u.scheme is None:
            raise xapi.storage.api.v5.volume.SR_does_not_exist(
                'The SR URI is invalid')

        raise xapi.storage.api.v5.volume.Unimplemented('probe')

        return {
            'srs': [],
            'uris': []
        }

    def attach(self, dbg, configuration):
        uri = configuration['uri']
        log.debug('{}: SR.attach: config={}, uri={}'.format(
            dbg, configuration, uri))

        nfs_uri = urllib.parse.urlsplit(uri)
        if nfs_uri.scheme != 'nfs':
            raise ValueError('Incorrect URI scheme')

        nfs_server = '{0}:{1}'.format(nfs_uri.netloc, nfs_uri.path)

        sr_uuid = configuration['sr_uuid']
        mnt_path = self._mount_path(sr_uuid)
        sr_dir = os.path.join(mnt_path, sr_uuid)
        sr = urllib.parse.urlunsplit(('file', '', sr_dir, None, None))

        if os.path.exists(mnt_path) and os.path.ismount(mnt_path):
            log.debug("%s: SR.attach: uri=%s ALREADY ATTACHED" % (dbg, uri))
            return sr

        log.debug("%s: SR.attach: uri=%s NOT ATTACHED YET" % (dbg, uri))
        # Mount the file system
        mnt_path = self._mount(dbg, nfs_server, sr_uuid)

        if not os.path.exists(sr_dir) or not os.path.isdir(sr_dir):
            raise ValueError('SR directory doesn\'t exist')

        # Start GC for this host
        COWCoalesce.start_gc(dbg, 'nfs-ng', sr)

        return sr

    def create(self, dbg, sr_uuid, configuration, name, description):
        log.debug('{}: SR.create: config={}, sr_uuid={}'.format(
            dbg, configuration, sr_uuid))

        uri = configuration['uri']
        nfs_uri = urllib.parse.urlsplit(uri)
        if nfs_uri.scheme != 'nfs':
            raise ValueError('Incorrect URI scheme')

        nfs_server = '{0}:{1}'.format(nfs_uri.netloc, nfs_uri.path)

        # Temporarily mount the filesystem so we can write the SR metadata
        mnt_path = self._mount(dbg, nfs_server, sr_uuid)

        sr = os.path.join(mnt_path, str(sr_uuid))

        # Create SR folder based on name
        try:
            os.makedirs(sr)
        except OSError as exc:
            if exc.errno == errno.EEXIST:
                # Need a specifc error here, SR_already_exists.
                # raise ValueError('SR already exists'):
                raise
            else:
                raise

        # Create the metadata database
        importlib.import_module('nfs-ng').Callbacks().create_database(sr)

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

        self._unmount(dbg, mnt_path)

        configuration['sr_uuid'] = sr_uuid
        return configuration

    def destroy(self, dbg, sr):
        # Fixme: actually destroy the data
        return self.detach(dbg, sr)

    def detach(self, dbg, sr):
        # stop GC
        try:
            COWCoalesce.stop_gc(dbg, 'nfs-ng', sr)
        except:
            log.debug('GC already stopped')

        # Unmount the FS
        sr_path = urllib.parse.urlparse(sr).path
        mnt_path = os.path.dirname(sr_path)
        self._unmount(dbg, mnt_path)
        os.rmdir(mnt_path)

    def ls(self, dbg, sr):
        return COWVolume.ls(
            dbg, sr, importlib.import_module('nfs-ng').Callbacks())

    def set_description(self, dbg, sr, new_description):
        util.update_sr_metadata(
            dbg, 'file://' + sr, {'description': new_description})

    def set_name(self, dbg, sr, new_name):
        util.update_sr_metadata(dbg, 'file://' + sr, {'name': new_name})

    def stat(self, dbg, sr):
        if not os.path.isdir(sr) or not os.path.ismount(sr):
            raise xapi.storage.api.v5.volume.Sr_not_attached(sr)

        # Get the filesystem size
        statvfs = os.statvfs(sr)
        psize = statvfs.f_blocks * statvfs.f_frsize
        fsize = statvfs.f_bfree * statvfs.f_frsize
        log.debug('{}: statvfs says psize = {}'.format(dbg, psize))

        overprovision = COWVolume.get_sr_provisioned_size(
            sr, importlib.import_module('nfs-ng').Callbacks()) / psize

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
