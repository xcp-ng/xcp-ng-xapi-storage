#!/usr/bin/env python

import importlib
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

import zfsutils


@util.decorate_all_routines(util.log_exceptions_in_function)
class Implementation(xapi.storage.api.v5.volume.SR_skeleton):
    "SR driver to provide volumes from zvol's"

    def create(self, dbg, sr_uuid, configuration, name, description):
        log.debug('{}: SR.create: config={}, sr_uuid={}'.format(
            dbg, configuration, sr_uuid))

        # 2 ways to create a SR:
        # - from an existing zpool (manually created for complex configs)
        # - from a "device" config string (comma-separated list of devices)

        if 'zpool' in configuration:
            if 'device' in configuration:
                log.error('"zpool" specified, "device" should not be used')
                raise Exception('"zpool" specified, "device" should not be used')

            # FIXME validate existence of pool first?
            pool_name = configuration['zpool']

        elif 'device' in configuration:
            devs = configuration['device'].split(',')

            pool_name = "sr-{}".format(sr_uuid)
            zfsutils.pool_create(dbg, pool_name, devs)

            # "device" is only used once to create the zpool, which
            # then becomes the sole way to designate the SR
            configuration["orig-device"] = configuration['device']
            del configuration['device']
            configuration["zpool"] = pool_name

        else:
            log.error('devices config must have "zpool" or "device"')
            raise Exception('devices config must have "zpool" or "device"')

        # FIXME this assumes zpool is mounted/attached
        mountpoint = zfsutils.pool_mountpoint(dbg, pool_name)
        importlib.import_module('zfs-vol').Callbacks().create_database(mountpoint)

        meta = {
            # mandatory elements we need everywhere
            'name': name,
            'description': description,
            'uuid': sr_uuid,
            # pool name may not always be derived from mountpoint or
            # sr_uuid, esp. when creating with "zpool=$PREBUILT_POOL"
            'zpool': pool_name,
        }
        util.update_sr_metadata(dbg, 'file://' + mountpoint, meta)

        log.debug('{}: SR.create: sr={}'.format(dbg, mountpoint))
        return configuration

    def destroy(self, dbg, sr):
        meta = util.get_sr_metadata(dbg, 'file://' + sr)
        zfsutils.pool_destroy(dbg, meta["zpool"])

    def attach(self, dbg, configuration):
        log.debug('{}: SR.attach: config={}'.format(dbg, configuration))

        pool_name = configuration["zpool"]
        try:
            mountpoint = zfsutils.pool_mountpoint(dbg, pool_name)
        except Exception:
            log.debug('{}: SR.attach: mountpoint for {} not found, import it'.format(dbg, pool_name))
        else:
            # SR.attach should be idempotent. So if the mountpoint is already
            # mounted just return it.
            return mountpoint

        zfsutils.pool_import(dbg, pool_name)
        return zfsutils.pool_mountpoint(dbg, configuration["zpool"])

    def detach(self, dbg, sr):
        meta = util.get_sr_metadata(dbg, 'file://' + sr)
        # When the pool is exported it is also unmounted.
        zfsutils.pool_export(dbg, meta["zpool"])

    def ls(self, dbg, sr):
        results = []
        meta = util.get_sr_metadata(dbg, 'file://' + sr)
        pool_name = meta["zpool"]
        cb = importlib.import_module('zfs-vol').Callbacks()
        with VolumeContext(cb, sr, 'r') as opq:
            with cb.db_context(opq) as db:
                vdis = db.get_all_vdis()
                all_custom_keys = db.get_all_vdi_custom_keys()

            for vdi in vdis:
                # TODO: handle this better
                # _vdi_sanitize(vdi, opq, db, cb)

                image_format = ImageFormat.get_format(vdi.image_type)
                is_snapshot = bool(vdi.volume.snap)
                assert not is_snapshot, "snapshots not implemented yet"
                vol_name = zfsutils.zvol_path(pool_name, vdi.volume.id)
                psize = zfsutils.vol_get_used(dbg, vol_name)

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

    def stat(self, dbg, sr):
        if not os.path.isdir(sr):
            raise xapi.storage.api.v5.volume.Sr_not_attached(sr)
        meta = util.get_sr_metadata(dbg, 'file://' + sr)
        pool_name = meta["zpool"]

        return {
            'sr': sr,
            'name': meta['name'],
            'description': meta['description'],
            'total_space': zfsutils.pool_get_size(dbg, pool_name),
            'free_space': zfsutils.pool_get_free_space(dbg, pool_name),
            'uuid': meta['uuid'],
            'datasources': [],
            'clustered': False,
            'health': ['Healthy', ''] # could be "Recovering", when does it make sense?
        }

    def set_name(self, dbg, sr, new_name):
        util.update_sr_metadata(dbg, 'file://' + sr, {'name': new_name})

    def set_description(self, dbg, sr, new_description):
        util.update_sr_metadata(
            dbg, 'file://' + sr, {'description': new_description})


if __name__ == '__main__':
    log.log_call_argv()
    cmd = xapi.storage.api.v5.volume.SR_commandline(Implementation())

    call("zfs-vol.sr", ['modprobe', 'zfs'])

    base = os.path.basename(sys.argv[0])
    if base == 'SR.create':
        cmd.create()
    elif base == 'SR.attach':
        cmd.attach()
    elif base == 'SR.destroy':
        cmd.destroy()
    elif base == 'SR.detach':
        cmd.detach()
    elif base == 'SR.ls':
        cmd.ls()
    elif base == 'SR.stat':
        cmd.stat()
    elif base == 'SR.set_name':
        cmd.set_name()
    elif base == 'SR.set_description':
        cmd.set_description()
    else:
        raise xapi.storage.api.v5.volume.Unimplemented(base)
