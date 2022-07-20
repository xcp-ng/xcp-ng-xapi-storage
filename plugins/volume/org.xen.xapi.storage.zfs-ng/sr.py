#!/usr/bin/env python

import os
import os.path
import sys
import urlparse

from xapi.storage import log
from xapi.storage.libs import util
from xapi.storage.common import call
from xapi.storage.libs.libcow.volume import COWVolume
import xapi.storage.api.v5.volume

import importlib

ZPOOL_BIN = 'zpool'
ZFS_BIN = 'zfs'

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
        dev = urlparse.urlparse(uri).path

        # zfs mount the new fs in root by using the name
        sr = '/' + name
        cmd = [
            ZPOOL_BIN, 'create',
            name, dev
        ]

        try:
            call(dbg, cmd)
        except:
            log.debug('error creating the pool')

        log.debug('{}: SR.create: sr={}'.format(dbg, sr))

        # Create the metadata database
        importlib.import_module('zfs-ng').Callbacks().create_database(sr)

        meta = {
            'name': name,
            'description': description,
            'uri': uri,
            'unique_id': sr_uuid,
            'read_caching': False,
            'keys': {}
        }
        util.update_sr_metadata(dbg, 'file://' + sr, meta)

        configuration['file-uri'] = sr

        return configuration

    def destroy(self, dbg, sr):
        name = os.path.basename(sr)
        cmd = [
            ZPOOL_BIN, 'destroy',
            name
        ]

        log.debug('cmd={}'.format(cmd))

        try:
            call(dbg, cmd)
        except:
            log.debug('error destroying the pool the pool')

    def detach(self, dbg, sr):
        # Nothing todo.
        pass

    def ls(self, dbg, sr):
        # TODO: reimplement ls
        return COWVolume.ls(
            dbg, sr, importlib.import_module('zfs-ng').Callbacks())

    def set_description(self, dbg, sr, new_description):
        util.update_sr_metadata(
            dbg, 'file://' + sr, {'description': new_description})

    def set_name(self, dbg, sr, new_name):
        util.update_sr_metadata(dbg, 'file://' + sr, {'name': new_name})

    def stat(self, dbg, sr):
        # TODO: replace this with a check if it is a device
        #if not os.path.isdir(sr):
        #    raise xapi.storage.api.v5.volume.Sr_not_attached(sr)
        meta = util.get_sr_metadata(dbg, 'file://' + sr)

        cmd = [
            ZFS_BIN, 'get',
            '-o', 'value', '-Hp', 'used,avail',
            meta['name']
        ]

        try:
            out = call(dbg, cmd).splitlines()
        except:
            log.debug('error querying the size of the pool')

        # TODO: rewrite this
        psize = int(out[0]) + int(out[1])
        fsize = int(out[1])

        return {
            'sr': sr,
            'name': meta['name'],
            'description': meta['description'],
            'total_space': psize,
            'free_space': fsize,
            'uuid': meta['unique_id'],
            'datasources': [],
            'clustered': False,
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
