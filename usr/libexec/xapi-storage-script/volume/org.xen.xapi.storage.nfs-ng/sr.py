#!/usr/bin/env python

from __future__ import division
import os
import os.path
import sys
import errno
import urlparse
import json

import xapi.storage.api.v5.volume
from xapi.storage.common import call
from xapi.storage.libs.libcow.volume import COWVolume
from xapi.storage.libs.libcow.coalesce import COWCoalesce
from xapi.storage import log

import importlib


class Implementation(xapi.storage.api.v5.volume.SR_skeleton):

    # Base of SR mounts, SRs will mount in a folder with their GUID
    MOUNTPOINT_ROOT = "/var/run/sr-mount/"

    def _mount_path(self, sr_uuid):
        return os.path.abspath(self.MOUNTPOINT_ROOT + str(sr_uuid))

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
            cmd = ["/usr/bin/mount", "-t", "nfs", "-o",
                   "noatime,nodiratime", nfs_path, mnt_path]
            call(dbg, cmd)
        return mnt_path

    def _umount(self, dbg, mnt_path):
        """Unmount the NFS share"""
        cmd = ["/usr/bin/umount", mnt_path]
        call(dbg, cmd)

    def probe(self, dbg, configuration):
        uris = []
        srs = []
        uri = configuration['uri']
        u = urlparse.urlparse(uri)
        if u.scheme is None:
            raise xapi.storage.api.v5.volume.SR_does_not_exist(
                "The SR URI is invalid")

        raise xapi.storage.api.v5.volume.Unimplemented("probe")

        return {
            "srs": srs,
            "uris": uris
        }

    def attach(self, dbg, configuration):
        uri = configuration['uri']
        sr_uuid = configuration['sr_uuid']

        log.debug("%s: SR.attach: uri=%s" % (dbg, uri))

        nfs_uri = urlparse.urlsplit(uri)
        if nfs_uri.scheme != 'nfs':
            raise ValueError('Incorrect URI scheme')

        nfs_server = '{0}:{1}'.format(nfs_uri.netloc, nfs_uri.path)

        mnt_path = self._mount_path(sr_uuid)
        sr_dir = os.path.join(mnt_path, sr_uuid)
        sr = urlparse.urlunsplit(('file', '', sr_dir, None, None))

        if os.path.exists(mnt_path) and os.path.ismount(mnt_path):
            log.debug("%s: SR.attach: uri=%s ALREADY ATTACHED" % (dbg, uri))
            return sr

        log.debug("%s: SR.attach: uri=%s NOT ATTACHED YET" % (dbg, uri))
        # Mount the file system
        mnt_path = self._mount(dbg, nfs_server, sr_uuid)

        if not os.path.exists(sr_dir) or not os.path.isdir(sr_dir):
            raise ValueError('SR directory doesn\'t exist')

        # Start GC for this host
        COWCoalesce.start_gc(dbg, "nfs-ng", sr)

        return sr

    def create(self, dbg, sr_uuid, configuration, name, description):
        log.debug("%s: SR.create: config=%s" %
                  (dbg, configuration))

        uri = configuration['uri']
        nfs_uri = urlparse.urlsplit(uri)
        if nfs_uri.scheme != 'nfs':
            raise ValueError('Incorrect URI scheme')

        nfs_server = '{0}:{1}'.format(nfs_uri.netloc, nfs_uri.path)

        # Temporarily mount the filesystem so we can write the SR metadata
        mnt_path = self._mount(dbg, nfs_server, sr_uuid)

        sr_path = os.path.join(mnt_path, str(sr_uuid))

        # Create SR folder based on name
        try:
            os.makedirs(sr_path)
        except OSError as exc:
            if exc.errno == errno.EEXIST:
                # Need a specifc error here, SR_already_exists.
                # raise ValueError('SR already exists'):
                raise
            else:
                raise

        # Create the metadata database
        COWVolume.create_metabase(sr_path + "/sqlite3-metadata.db")

        read_caching = True
        if 'read_caching' in configuration:
            if configuration['read_caching'] not in [
                    'true', 't', 'on', '1', 'yes']:
                read_caching = False

        configuration['sr_uuid'] = sr_uuid

        meta = {
            "name": name,
            "description": description,
            "uri": uri,
            "unique_id": sr_uuid,
            "read_caching": read_caching,
            "keys": {}
        }
        metapath = sr_path + "/meta.json"
        log.debug("%s: dumping metadata to %s: %s" % (dbg, metapath, meta))

        with open(metapath, "w") as json_fp:
            json.dump(meta, json_fp)
            json_fp.write("\n")

        self._umount(dbg, mnt_path)

        return configuration

    def destroy(self, dbg, sr):
        # Fixme: actually destroy the data
        return self.detach(dbg, sr)

    def detach(self, dbg, sr):
        # stop GC
        try:
            COWCoalesce.stop_gc(dbg, "nfs-ng", sr)
        except:
            log.debug("GC already stopped")

        # Unmount the FS
        sr_path = urlparse.urlparse(sr).path
        mnt_path = os.path.dirname(sr_path)
        self._umount(dbg, mnt_path)
        os.rmdir(mnt_path)

    def ls(self, dbg, sr):
        raise xapi.storage.api.v5.volume.Unimplemented("ls")

    def stat(self, dbg, sr):
        # SR path (sr) is file://<mnt_path>
        # Get mnt_path by dropping url scheme
        sr_path = urlparse.urlparse(sr).path
        mnt_path = os.path.dirname(sr_path)

        if not(os.path.isdir(mnt_path)) or not os.path.ismount(mnt_path):
            raise xapi.storage.api.v5.volume.Sr_not_attached(mnt_path)

        # Get the filesystem size
        statvfs = os.statvfs(mnt_path)
        psize = statvfs.f_blocks * statvfs.f_frsize
        fsize = statvfs.f_bfree * statvfs.f_frsize
        log.debug("%s: statvfs says psize = %Ld" % (dbg, psize))

        nfs = importlib.import_module("nfs-ng")

        overprovision = COWVolume.get_sr_provisioned_size(
            sr, nfs.Callbacks()) / psize

        return {
            "sr": sr,
            "name": "SR Name",
            "description": "NFS SR",
            "uuid": os.path.basename(sr_path),
            "total_space": psize,
            "free_space": fsize,
            "overprovision": overprovision,
            "datasources": [],
            "clustered": True,
            "health": ["Healthy", ""]
        }


def call_sr_command():
    """Process the command line arguments and call the required operation"""
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
    elif base == 'SR.stat':
        cmd.stat()
    else:
        raise xapi.storage.api.v5.volume.Unimplemented(base)


if __name__ == "__main__":
    call_sr_command()
