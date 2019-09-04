#!/usr/bin/env python

from __future__ import division
import os
import os.path
import sys
import urlparse
import json

import xapi.storage.api.v5.volume
from xapi.storage.common import call
from xapi.storage.libs.libcow.volume import COWVolume
from xapi.storage.libs.libcow.coalesce import COWCoalesce
from xapi.storage import log

import filebased

# For a block device /a/b/c, we will mount it at <mountpoint_root>/a/b/c
mountpoint_root = "/var/run/sr-mount/"


class Implementation(xapi.storage.api.v5.volume.SR_skeleton):

    def probe(self, dbg, configuration):
        uris = []
        srs = []
        return {
            "srs": srs,
            "uris": uris
        }

    def attach(self, dbg, configuration):
        uri = configuration['uri']

        sr = urlparse.urlparse(uri).path

        # Start GC for this host
        COWCoalesce.start_gc(dbg, "filebased", sr)

        return sr

    def create(self, dbg, uuid, configuration, name, description):
        log.debug("%s: SR.create: config=%s, uuid=%s" %
                  (dbg, configuration, uuid))

        uri = configuration['uri']

        sr_path = urlparse.urlparse(uri).path
        log.debug("%s: SR.create: sr_path = %s" % (dbg, sr_path))
        sr_id = uuid
        log.debug("%s: SR.create: sr_id = %s" % (dbg, sr_id))

        # Create the metadata database
        COWVolume.create_metabase(sr_path + "/sqlite3-metadata.db")

        read_caching = True
        if 'read_caching' in configuration:
            if configuration['read_caching'] not in [
                    'true', 't', 'on', '1', 'yes']:
                read_caching = False

        meta = {
            "name": name,
            "description": description,
            "uri": uri,
            "unique_id": sr_id,
            "fsname": sr_id,
            "read_caching": read_caching,
            "keys": {}
        }
        metapath = sr_path + "/meta.json"
        log.debug("%s: dumping metadata to %s: %s" % (dbg, metapath, meta))

        with open(metapath, "w") as json_fp:
            json.dump(meta, json_fp)
            json_fp.write("\n")

        log.debug("%s: finished" % (dbg))
        return configuration

    def destroy(self, dbg, sr):
        # stop GC
        try:
            COWCoalesce.stop_gc(dbg, "filebased", sr)
        except:
            log.debug("GC already stopped")
        call("dbg", ["/usr/bin/rm", "-rf", sr + "/*"])
        return self.detach(dbg, sr)

    def detach(self, dbg, sr):
        # stop GC
        try:
            COWCoalesce.stop_gc(dbg, "filebased", sr)
        except:
            log.debug("GC already stopped")
        return

    def ls(self, dbg, sr):
        return COWVolume.ls(dbg, sr, filebased.Callbacks())

    def stat(self, dbg, sr):
        # SR path (sr) is file://<mnt_path>
        # Get mnt_path by dropping url scheme
        mnt_path = sr

        if not(os.path.isdir(mnt_path)):
            raise xapi.storage.api.v5.volume.Sr_not_attached(mnt_path)

        # Get the filesystem size
        statvfs = os.statvfs(mnt_path)
        psize = statvfs.f_blocks * statvfs.f_frsize
        fsize = statvfs.f_bfree * statvfs.f_frsize
        log.debug("%s: statvfs says psize = %Ld" % (dbg, psize))

        overprovision = COWVolume.get_sr_provisioned_size(
            sr, filebased.Callbacks()) / psize

        return {
            "sr": sr,
            "uuid": "xxx",
            "name": "SR Name",
            "description": "FILEBASED SR",
            "total_space": psize,
            "free_space": fsize,
            "overprovision": overprovision,
            "datasources": [],
            "clustered": True,
            "health": ["Healthy", ""]
        }


if __name__ == "__main__":
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
