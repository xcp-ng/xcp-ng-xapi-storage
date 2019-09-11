#!/usr/bin/env python

import os
import os.path
import sys
import urlparse

from xapi.storage import log
from xapi.storage.libs import util
from xapi.storage.libs.libcow.coalesce import COWCoalesce
from xapi.storage.libs.libcow.volume import COWVolume
import xapi.storage.api.v5.volume

import filebased


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

        # Start GC for this host
        COWCoalesce.start_gc(dbg, 'filebased', sr)

        return sr

    def create(self, dbg, sr_uuid, configuration, name, description):
        log.debug('{}: SR.create: config={}, sr_uuid={}'.format(
            dbg, configuration, sr_uuid))

        uri = configuration['file-uri']
        sr = urlparse.urlparse(uri).path
        log.debug('{}: SR.create: sr={}'.format(dbg, sr))

        # Create the metadata database
        filebased.Callbacks().create_database(sr)

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

        return configuration

    def destroy(self, dbg, sr):
        self.detach(dbg, sr)
        util.remove_folder_content(sr)

    def detach(self, dbg, sr):
        # stop GC
        try:
            COWCoalesce.stop_gc(dbg, 'filebased', sr)
        except:
            log.debug('GC already stopped')

    def ls(self, dbg, sr):
        return COWVolume.ls(dbg, sr, filebased.Callbacks())

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
            sr, filebased.Callbacks()) / psize

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
