#!/usr/bin/env python

import importlib
import os
import os.path
import sys
import urlparse

from xapi.storage import log
from xapi.storage.libs import util
from xapi.storage.common import call
from xapi.storage.libs.libcow.volume import COWVolume
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
        # - from a "devices" config string (comma-separated list of devices)

        if 'zpool' in configuration:
            if 'devices' in configuration:
                log.error('"zpool" specified, "devices" should not be used')
                raise Exception('"zpool" specified, "devices" should not be used')

            # FIXME do we reject pools not under MOUNT_ROOT?

            # FIXME validate existence of pool first?
            pool_name = configuration['zpool']

        elif 'devices' in configuration:
            devs = configuration['devices'].split(',')

            pool_name = "sr-{}".format(sr_uuid)
            zfsutils.pool_create(dbg, pool_name, devs)

            # "devices" is only used once to create the zpool, which
            # then becomes the sole way to designate the SR
            configuration["orig-devices"] = configuration['devices']
            del configuration['devices']
            configuration["zpool"] = pool_name

        else:
            log.error('devices config must have "zpool" or "devices"')
            raise Exception('devices config must have "zpool" or "devices"')

        # FIXME this assumes zpool is mounted/attached
        mountpoint = zfsutils.pool_mountpoint(dbg, pool_name)
        importlib.import_module('zfs-ng').Callbacks().create_database(mountpoint)

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

        # ZFS automagically attaches a pool to a mountpoint on
        # create/boot/etc, so we basically do nothing here but find
        # this mountpoint

        # FIXME: study how pools are mounted on boot and see if we
        # could do real a/detach

        return zfsutils.pool_mountpoint(dbg, configuration["zpool"])

    def detach(self, dbg, sr):
        # Nothing to unmount for now.
        pass

    def stat(self, dbg, sr):
        # TODO: replace this with a check if it is a device
        #if not os.path.isdir(sr):
        #    raise xapi.storage.api.v5.volume.Sr_not_attached(sr)
        meta = util.get_sr_metadata(dbg, 'file://' + sr)
        pool_name = meta["zpool"]

        # FIXME a single zpool-get call would be faster
        psize = zfsutils.pool_get_size(dbg, pool_name)
        fsize = zfsutils.pool_get_free_space(dbg, pool_name) # FIXME is that the expected datapoint?

        return {
            'sr': sr,
            'name': meta['name'],
            'description': meta['description'],
            'total_space': psize,
            'free_space': fsize,
            'uuid': meta['uuid'],
            'datasources': [],  # FIXME doublecheck
            'clustered': False,
            'health': ['Healthy', ''] # FIXME
        }


if __name__ == '__main__':
    log.log_call_argv()
    cmd = xapi.storage.api.v5.volume.SR_commandline(Implementation())
    base = os.path.basename(sys.argv[0])
    if base == 'SR.create':
        cmd.create()
    elif base == 'SR.attach':
        cmd.attach()
    elif base == 'SR.destroy':
        cmd.destroy()
    elif base == 'SR.detach':
        cmd.detach()
    elif base == 'SR.stat':
        cmd.stat()
    else:
        raise xapi.storage.api.v5.volume.Unimplemented(base)
