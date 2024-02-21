#!/usr/bin/env python

import importlib
import os
import os.path
import sys
import urlparse

from xapi.storage import log
from xapi.storage.libs import util
from xapi.storage.libs.libcow.coalesce import COWCoalesce
from xapi.storage.libs.libcow.volume import COWVolume
import xapi.storage.api.v5.volume


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
        COWCoalesce.start_gc(dbg, 'raw-device', sr)

        return sr

    def create(self, dbg, sr_uuid, configuration, name, description):
        log.debug('{}: SR.create: config={}, sr_uuid={}'.format(
            dbg, configuration, sr_uuid))

        uri = configuration['file-uri']
        sr = urlparse.urlparse(uri).path
        log.debug('{}: SR.create: sr={}'.format(dbg, sr))

        # Create the metadata database
        importlib.import_module('raw-device').Callbacks().create_database(sr)

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
            'keys': {},
            'devices': [x.strip() for x in configuration['devices'].split(',')]
        }
        util.update_sr_metadata(dbg, 'file://' + sr, meta)

        return configuration

    def destroy(self, dbg, sr):
        self.detach(dbg, sr)
        util.remove_folder_content(sr)

    def detach(self, dbg, sr):
        # stop GC
        try:
            COWCoalesce.stop_gc(dbg, 'raw-device', sr)
        except:
            log.debug('GC already stopped')

    def ls(self, dbg, sr):
        return COWVolume.ls(
            dbg, sr, importlib.import_module('raw-device').Callbacks())

    def set_description(self, dbg, sr, new_description):
        util.update_sr_metadata(
            dbg, 'file://' + sr, {'description': new_description})

    def set_name(self, dbg, sr, new_name):
        util.update_sr_metadata(dbg, 'file://' + sr, {'name': new_name})

    def stat(self, dbg, sr):
        if not os.path.isdir(sr):
            raise xapi.storage.api.v5.volume.Sr_not_attached(sr)

        devices = util.get_sr_metadata(dbg, 'file://' + sr)['devices']
        total_size = 0
        for device in (os.path.realpath(x) for x in devices):
            total_size += util.get_physical_file_size(device)

        used_size = COWVolume.get_sr_provisioned_size(
            sr, importlib.import_module('raw-device').Callbacks())

        meta = util.get_sr_metadata(dbg, 'file://' + sr)
        return {
            'sr': sr,
            'name': meta['name'],
            'description': meta['description'],
            'total_space': total_size,
            'free_space': total_size - used_size,
            'uuid': meta['unique_id'],
            'overprovision': used_size,
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
