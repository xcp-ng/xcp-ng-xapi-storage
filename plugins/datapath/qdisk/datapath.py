#!/usr/bin/env python
"""
Datapath for QEMU qdisk
"""

import urlparse
import os
import sys
import xapi
import xapi.storage.api.v5.datapath
import xapi.storage.api.v5.volume
import importlib
from xapi.storage.libs.libcow.datapath import COWDatapath
from xapi.storage.libs import qemudisk
from xapi.storage import log


def get_sr_callbacks(dbg, uri):
    u = urlparse.urlparse(uri)
    sr = u.netloc
    sys.path.insert(
        0,
        '/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.' + sr)
    mod = importlib.import_module(sr)
    return mod.Callbacks()


class QdiskDatapath(COWDatapath):
    """
    Datapath handler for qdisk
    """

    @staticmethod
    def parse_uri(uri):
        # uri will be like:
        # "qdisk://<sr-type>/<sr-mount-or-volume-group>|<volume-name>"
        mount_or_vg, name = urlparse.urlparse(uri).path.split('|')
        return ('qcow2:///' + mount_or_vg, name)

    @staticmethod
    def attach_internal(dbg, opq, vdi, vol_path, cb):
        log.debug("attach: doing qcow2 attach")
        # spawn an upstream qemu as a standalone backend
        qemu_be = qemudisk.create(dbg, vdi.uuid)
        log.debug("attach: created %s" % qemu_be)
        data_metadata_path = cb.get_data_metadata_path(opq, vdi.uuid)
        qemudisk.save_qemudisk_metadata(dbg,
                                        data_metadata_path,
                                        qemu_be)
        log.debug("attach: saved metadata with %s, %s" %
                  (cb.get_data_metadata_path(opq, vdi.uuid), qemu_be))

        return [
            ['XenDisk', {
                'backend_type': 'qdisk',
                'params': "vdi:{}".format(vdi.uuid),
                'extra': {}
            }],
            ['Nbd', {
                'uri': 'nbd:unix:{}:exportname={}'.format(
                    qemu_be.nbd_unix_sock, qemudisk.LEAF_NODE_NAME
                )
            }]
        ]

    @staticmethod
    def activate_internal(dbg, opq, vdi, img, cb):
        log.debug(
            "activate: doing qcow2 activate with img '%s'"
            % (img))
        vdi_meta_path = cb.get_data_metadata_path(opq, vdi.uuid)
        qemu_be = qemudisk.load_qemudisk_metadata(
            dbg, vdi_meta_path)
        qemu_be.open(dbg, vdi.uuid, img)
        qemudisk.save_qemudisk_metadata(dbg,
                                        vdi_meta_path,
                                        qemu_be)

    @staticmethod
    def deactivate_internal(dbg, opq, vdi, img, cb):
        """
        Do the qdisk specific deactivate
        """
        log.debug(
            "deactivate: doing qcow2 deactivate with img '%s'"
            % (img))
        qemu_be = qemudisk.load_qemudisk_metadata(
            dbg, cb.get_data_metadata_path(opq, vdi.uuid))
        qemu_be.close(dbg, vdi.uuid, img)
        metadata_path = cb.get_data_metadata_path(opq, vdi.uuid)
        qemudisk.save_qemudisk_metadata(dbg,
                                        metadata_path,
                                        qemu_be)

    @staticmethod
    def detach_internal(dbg, opq, vdi, cb):
        log.debug("detach: find and kill the qemu")
        vdi_meta_path = cb.get_data_metadata_path(opq, vdi.uuid)
        qemu_be = qemudisk.load_qemudisk_metadata(dbg, vdi_meta_path)
        qemu_be.quit(dbg, vdi.uuid)


class Implementation(xapi.storage.api.v5.datapath.Datapath_skeleton):
    """
    Datapath implementation
    """
    def activate(self, dbg, uri, domain):
        callbacks = get_sr_callbacks(dbg, uri)
        return QdiskDatapath.activate(dbg, uri, domain, callbacks)

    def attach(self, dbg, uri, domain):
        callbacks = get_sr_callbacks(dbg, uri)
        return QdiskDatapath.attach(dbg, uri, domain, callbacks)

    def deactivate(self, dbg, uri, domain):
        callbacks = get_sr_callbacks(dbg, uri)
        return QdiskDatapath.deactivate(dbg, uri, domain, callbacks)

    def detach(self, dbg, uri, domain):
        callbacks = get_sr_callbacks(dbg, uri)
        return QdiskDatapath.detach(dbg, uri, domain, callbacks)

    def open(self, dbg, uri, domain):
        callbacks = get_sr_callbacks(dbg, uri)
        return QdiskDatapath.epc_open(dbg, uri, domain, callbacks)

    def close(self, dbg, uri):
        callbacks = get_sr_callbacks(dbg, uri)
        return QdiskDatapath.epc_close(dbg, uri, callbacks)


if __name__ == "__main__":
    log.log_call_argv()
    CMD = xapi.storage.api.v5.datapath.Datapath_commandline(Implementation())
    CMD_BASE = os.path.basename(sys.argv[0])
    if CMD_BASE == "Datapath.activate":
        CMD.activate()
    elif CMD_BASE == "Datapath.attach":
        CMD.attach()
    elif CMD_BASE == "Datapath.close":
        CMD.close()
    elif CMD_BASE == "Datapath.deactivate":
        CMD.deactivate()
    elif CMD_BASE == "Datapath.detach":
        CMD.detach()
    elif CMD_BASE == "Datapath.open":
        CMD.open()
    else:
        raise xapi.storage.api.v5.datapath.Unimplemented(CMD_BASE)
