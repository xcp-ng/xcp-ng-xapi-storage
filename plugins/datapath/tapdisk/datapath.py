#!/usr/bin/env python

import urlparse
import os
import sys
import xapi
import xapi.storage.api.v5.datapath
import xapi.storage.api.v5.volume
import importlib
from xapi.storage.libs.libcow.datapath import COWDatapath
from xapi.storage.libs.libcow.intellicache import IntelliCache
from xapi.storage.libs import tapdisk
from xapi.storage import log

vdi_enable_intellicache = False


def get_sr_callbacks(dbg, uri):
    u = urlparse.urlparse(uri)
    sr = u.netloc
    sys.path.insert(
        0,
        '/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.' + sr)
    mod = importlib.import_module(sr)
    return mod.Callbacks()


class TapdiskDatapath(COWDatapath):
    """
    Datapath handler for tapdisk
    """

    @staticmethod
    def parse_uri(uri):
        # uri will be like:
        # "tapdisk://<sr-type>/<sr-mount-or-volume-group>|<volume-name>"
        mount_or_vg, name = urlparse.urlparse(uri).path.split('|')
        return ('vhd:///' + mount_or_vg, name)

    @staticmethod
    def attach_internal(dbg, opq, vdi, vol_path, cb):
        if vdi.volume.parent_id is not None and vdi_enable_intellicache:
            parent_cow_path = cb.volumeGetPath(opq, str(vdi.volume.parent_id))
            IntelliCache.attach(
                dbg,
                vol_path,
                parent_cow_path
            )
        else:
            tap = tapdisk.create(dbg)
            tapdisk.save_tapdisk_metadata(
                dbg, cb.get_data_metadata_path(opq, vdi.uuid), tap)

        return [
            ['XenDisk', {
                'backend_type': 'vbd3',
                'params': tap.block_device(),
                'extra': {}
            }],
            ['BlockDevice', {
                'path': tap.block_device()
            }]
        ]

    @staticmethod
    def activate_internal(dbg, opq, vdi, img, cb):
        if vdi.volume.parent_id is not None and vdi_enable_intellicache:
            parent_cow_path = cb.volumeGetPath(
                opq,
                str(vdi.volume.parent_id)
            )

            IntelliCache.activate(
                img.path,
                parent_cow_path,
                vdi.nonpersistent
            )
        else:
            vdi_meta_path = cb.get_data_metadata_path(opq, vdi.uuid)
            tap = tapdisk.load_tapdisk_metadata(
                dbg, vdi_meta_path)
            # enable read caching by default since this is
            # goint to be used from licensed SRs
            tap.open(dbg, img, False)
            tapdisk.save_tapdisk_metadata(
                dbg, vdi_meta_path, tap)

    @staticmethod
    def deactivate_internal(dbg, opq, vdi, img, cb):
        """
        Do the tapdisk specific deactivate
        """
        if vdi.volume.parent_id is not None and vdi_enable_intellicache:
            parent_cow_path = cb.volumeGetPath(
                opq, str(vdi.volume.parent_id))
            IntelliCache.deactivate(
                dbg, img.path, parent_cow_path)
        else:
            tap = tapdisk.load_tapdisk_metadata(
                dbg, cb.get_data_metadata_path(opq, vdi.uuid))
            tap.close(dbg)

    @staticmethod
    def detach_internal(dbg, opq, vdi, cb):
        if vdi.volume.parent_id is not None and vdi_enable_intellicache:
            parent_cow_path = cb.volumeGetPath(
                opq, str(vdi.volume.parent_id))
            vol_path = cb.volumeGetPath(opq, str(vdi.volume.id))
            IntelliCache.detach(dbg, vol_path, parent_cow_path)
        else:
            vdi_meta_path = cb.get_data_metadata_path(opq, vdi.uuid)
            tap = tapdisk.load_tapdisk_metadata(dbg, vdi_meta_path)
            tap.destroy(dbg)
            tapdisk.forget_tapdisk_metadata(dbg, vdi_meta_path)


class Implementation(xapi.storage.api.v5.datapath.Datapath_skeleton):

    def activate(self, dbg, uri, domain):
        cb = get_sr_callbacks(dbg, uri)
        TapdiskDatapath.activate(dbg, uri, domain, cb)

    def attach(self, dbg, uri, domain):
        cb = get_sr_callbacks(dbg, uri)
        return TapdiskDatapath.attach(dbg, uri, domain, cb)

    def detach(self, dbg, uri, domain):
        cb = get_sr_callbacks(dbg, uri)
        TapdiskDatapath.detach(dbg, uri, domain, cb)

    def deactivate(self, dbg, uri, domain):
        cb = get_sr_callbacks(dbg, uri)
        TapdiskDatapath.deactivate(dbg, uri, domain, cb)

    def open(self, dbg, uri, persistent):
        cb = get_sr_callbacks(dbg, uri)
        TapdiskDatapath.epc_open(dbg, uri, persistent, cb)
        return None

    def close(self, dbg, uri):
        cb = get_sr_callbacks(dbg, uri)
        TapdiskDatapath.epc_close(dbg, uri, cb)
        return None


if __name__ == "__main__":
    log.log_call_argv()
    cmd = xapi.storage.api.v5.datapath.Datapath_commandline(Implementation())
    base = os.path.basename(sys.argv[0])
    if base == "Datapath.activate":
        cmd.activate()
    elif base == "Datapath.attach":
        cmd.attach()
    elif base == "Datapath.close":
        cmd.close()
    elif base == "Datapath.deactivate":
        cmd.deactivate()
    elif base == "Datapath.detach":
        cmd.detach()
    elif base == "Datapath.open":
        cmd.open()
    else:
        raise xapi.storage.api.v5.datapath.Unimplemented(base)
