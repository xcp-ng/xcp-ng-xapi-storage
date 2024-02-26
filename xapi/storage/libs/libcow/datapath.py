from __future__ import absolute_import
import sys
import urlparse

from xapi.storage import log
from xapi.storage.libs import tapdisk, image, qemudisk, util

from .callbacks import VolumeContext
from .imageformat import ImageFormat
from .intellicache import IntelliCache
from .lock import Lock

vdi_enable_intellicache = False


class COWDatapath(object):
    @staticmethod
    def parse_uri(uri):
        raise NotImplementedError('Override in dp specifc class')

    @staticmethod
    def attach_internal(dbg, opq, vdi, vol_path, cb):
        raise NotImplementedError('Override in dp specifc class')

    @classmethod
    def attach(cls, dbg, uri, domain, cb):
        sr, key = cls.parse_uri(uri)
        with VolumeContext(cb, sr, 'r') as opq:
            with cb.db_context(opq) as db:
                vdi = db.get_vdi_by_id(key)
            # activate LVs chain here
            vol_path = cb.volumeGetPath(opq, str(vdi.volume.id))

        return {
            'implementations': cls.attach_internal(dbg, opq, vdi, vol_path, cb)
        }

    @staticmethod
    def activate_internal(dbg, opq, vdi, img, cb):
        raise NotImplementedError('Override in dp specifc class')

    @staticmethod
    def _get_image_from_vdi(vdi, vol_path):
        if vdi.sharable or util.is_block_device(vol_path):
            return image.Raw(vol_path)
        return image.Cow(vol_path)

    @classmethod
    def activate(cls, dbg, uri, domain, cb):
        this_host_label = cb.get_current_host()
        sr, key = cls.parse_uri(uri)
        with VolumeContext(cb, sr, 'w') as opq:
            with Lock(opq, 'gl', cb):
                with cb.db_context(opq) as db:
                    vdi = db.get_vdi_by_id(key)
                    # Raise Storage Error VDIInUse - 24
                    if vdi.active_on:
                        raise util.create_storage_error(
                            "SR_BACKEND_FAILURE_24",
                            ["VDIInUse", "The VDI is currently in use"])
                    vol_path = cb.volumeGetPath(opq, str(vdi.volume.id))
                    img = cls._get_image_from_vdi(vdi, vol_path)
                    if not vdi.sharable:
                        db.update_vdi_active_on(vdi.uuid, this_host_label)

                    try:
                        cls.activate_internal(dbg, opq, vdi, img, cb)
                    except Exception as e:
                        log.error('{}: activate_internal failed: {}'.format(dbg, e))
                        raise

    @staticmethod
    def deactivate_internal(dbg, opq, vdi, img, cb):
        raise NotImplementedError('Override in dp specifc class')

    @classmethod
    def deactivate(cls, dbg, uri, domain, cb):
        sr, key = cls.parse_uri(uri)
        with VolumeContext(cb, sr, 'w') as opq:
            with Lock(opq, 'gl', cb):
                with cb.db_context(opq) as db:
                    vdi = db.get_vdi_by_id(key)
                    vol_path = cb.volumeGetPath(opq, str(vdi.volume.id))
                    img = cls._get_image_from_vdi(vdi, vol_path)
                    if not vdi.sharable:
                        db.update_vdi_active_on(vdi.uuid, None)

                    try:
                        cls.deactivate_internal(dbg, opq, vdi, img, cb)
                    except Exception as e:
                        log.error('{}: deactivate_internal failed: {}'.format(dbg, e))

    @staticmethod
    def detach_internal(dbg, opq, vdi, cb):
        raise NotImplementedError('Override in dp specifc class')

    @classmethod
    def detach(cls, dbg, uri, domain, cb):
        sr, key = cls.parse_uri(uri)
        with VolumeContext(cb, sr, 'r') as opq:
            with cb.db_context(opq) as db:
                vdi = db.get_vdi_by_id(key)
            try:
                # deactivate LVs chain here
                cls.detach_internal(dbg, opq, vdi, cb)
            except Exception as e:
                log.error('{}: detach_internal failed: {}'.format(dbg, e))

    @classmethod
    def create_single_clone(cls, db, sr, key, cb):
        pass

    @classmethod
    def epc_open(cls, dbg, uri, persistent, cb):
        log.debug("{}: Datapath.epc_open: uri == {}".format(dbg, uri))
        sr, key = cls.parse_uri(uri)
        with VolumeContext(cb, sr, 'w') as opq:
            with Lock(opq, 'gl', cb):
                try:
                    with cb.db_context(opq) as db:
                        vdi = db.get_vdi_by_id(key)
                        vol_path = cb.volumeGetPath(
                            opq, str(vdi.volume.id))
                        image_utils = ImageFormat.get_format(
                            vdi.image_type).image_utils
                        if (persistent):
                            log.debug(
                                ("{}: Datapath.epc_open: "
                                 "{} is persistent").format(dbg, vol_path)
                            )
                            if vdi.nonpersistent:
                                # Truncate, etc
                                image_utils.reset(dbg, vol_path)
                                db.update_vdi_nonpersistent(vdi.uuid, 1)
                        elif vdi.nonpersistent:
                            log.debug(
                                ("{}: Datapath.epc_open: {} already "
                                 "marked non-persistent").format(dbg,
                                                                 vol_path)
                            )
                            # truncate
                            image_utils.reset(dbg, vol_path)
                        else:
                            log.debug(
                                ("{}: Datapath.epc_open: {} is "
                                 "non-persistent").format(dbg, vol_path)
                            )
                            db.update_vdi_nonpersistent(vdi.uuid, 1)
                            if not image_utils.is_empty(dbg, vol_path):
                                # Create single clone
                                COWDatapath.create_single_clone(
                                    db, sr, key, cb)
                except Exception as e:
                    log.error("{}: Datapath.epc_open: failed to complete open, {}"
                              .format(dbg, e))
                    raise
        return None

    @classmethod
    def epc_close(cls, dbg, uri, cb):
        log.debug("{}: Datapath.epc_close: uri == {}".format(dbg, uri))
        sr, key = cls.parse_uri(uri)
        with VolumeContext(cb, sr, 'w') as opq:
            try:
                with Lock(opq, 'gl', cb):
                    with cb.db_context(opq) as db:
                        vdi = db.get_vdi_by_id(key)
                        vol_path = cb.volumeGetPath(opq, str(vdi.volume.id))
                        image_utils = ImageFormat.get_format(
                            vdi.image_type).image_utils
                        if vdi.nonpersistent:
                            # truncate
                            image_utils.reset(dbg, vol_path)
                            db.update_vdi_nonpersistent(vdi.uuid, None)
            except Exception as e:
                log.error("{}: Datapath.epc_close: failed to complete close, {}"
                          .format(dbg, e))
                raise
        return None


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
