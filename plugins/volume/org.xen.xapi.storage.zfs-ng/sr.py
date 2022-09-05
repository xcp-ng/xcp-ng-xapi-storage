#!/usr/bin/env python

import os
import os.path
import sys
import urlparse

from xapi.storage import log
from xapi.storage.libs import util
from xapi.storage.common import call
from xapi.storage.libs.libcow.imageformat import ImageFormat
from xapi.storage.libs.libcow.volume import COWVolume
from xapi.storage.libs.libcow.callbacks import VolumeContext
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
        uri = configuration['mountpoint']
        log.debug('{}: SR.attach: config={}, uri={}'.format(
            dbg, configuration, uri))

        sr = urlparse.urlparse(uri).path
        return sr

    def create(self, dbg, sr_uuid, configuration, name, description):
        log.debug('{}: SR.create: config={}, sr_uuid={}'.format(
            dbg, configuration, sr_uuid))

        if 'devices' not in configuration:
            log.error('devices parameter is missed')
            raise
        devs = configuration['devices'].split(',')

        mountpoint = '/' + name
        if 'mountpoint' in configuration:
            mountpoint = configuration['mountpoint']

        compression = False
        if 'compression' in configuration:
            if configuration['compression'] in [
                    'true', 't', 'on', '1', 'yes']:
                compression = True

        cmd = [
            ZPOOL_BIN, 'create', '-f',
            name, '-m', mountpoint
        ]

        if 'mode' in configuration:
            if configuration['mode'] not in [
                    'N', 'M', 'R']:
                log.error('mode can only be N(default), M(mirror) or R(raidz)')
                raise
            if configuration['mode'] in ['M']:
                if len(devs) < 2:
                    log.error('mirror mode requires at least two devices')
                    raise
                cmd.append('mirror')
            if configuration['mode'] in ['R']:
                if len(devs) < 2:
                    log.error('raidz mode requires at least two devices')
                    raise
                cmd.append('raidz')

        cmd.extend(devs)

        try:
            call(dbg, cmd)
        except:
            log.error('error creating the pool')
            raise

        if compression:
            cmd = [
                ZFS_BIN, 'set', 'compression=on', name
            ]
            call(dbg, cmd)

        log.debug('{}: SR.create: sr={}'.format(dbg, mountpoint))

        importlib.import_module('zfs-ng').Callbacks().create_database(mountpoint)

        meta = {
            'name': name,
            'description': description,
            'uri': mountpoint,
            'mountpoint': mountpoint,
            'unique_id': sr_uuid,
            'read_caching': False,
            'keys': {}
        }
        util.update_sr_metadata(dbg, 'file://' + mountpoint, meta)

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
        results = []
        cb = importlib.import_module('zfs-ng').Callbacks()
        with VolumeContext(cb, sr, 'r') as opq:
            with cb.db_context(opq) as db:
                vdis = db.get_all_vdis()
                all_custom_keys = db.get_all_vdi_custom_keys()

            for vdi in vdis:
                # TODO: handle this better
                # _vdi_sanitize(vdi, opq, db, cb)

                image_format = ImageFormat.get_format(vdi.image_type)
                is_snapshot = bool(vdi.volume.snap)
                if is_snapshot:
                    path = os.path.basename(sr) + '/'+ str(vdi.volume.parent_id) + '@' + str(vdi.volume.id)
                else:
                    path = os.path.basename(sr) + '/'+ str(vdi.volume.id)
                cmd = [
                    ZFS_BIN, 'get',
                    '-o', 'value', '-Hp', 'used,avail',
                    path
                ]
                out = call(dbg, cmd).splitlines()
                if is_snapshot:
                    psize = int(out[0])
                else:
                    psize = int(out[0]) + int(out[1])

                vdi_uri = cb.getVolumeUriPrefix(opq) + vdi.uuid
                custom_keys = {}
                if vdi.uuid in all_custom_keys:
                    custom_keys = all_custom_keys[vdi.uuid]

                results.append({
                    'uuid': vdi.uuid,
                    'key': vdi.uuid,
                    'name': vdi.name,
                    'description': vdi.description,
                    'read_write': not is_snapshot,
                    'virtual_size': vdi.volume.vsize,
                    'physical_utilisation': psize,
                    'uri': [image_format.uri_prefix + vdi_uri],
                    'keys': custom_keys,
                    'sharable': bool(vdi.sharable)
                })

        return results

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
