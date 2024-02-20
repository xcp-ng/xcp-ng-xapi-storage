from __future__ import absolute_import
import sys

from xapi.storage import log
from xapi.storage.libs import image, util

from .callbacks import VolumeContext
from .imageformat import ImageFormat
from .lock import Lock


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
