from __future__ import absolute_import, division
import uuid

from xapi.storage import log
from xapi.storage.libs import util
import xapi.storage.api.v5.volume

from .callbacks import VolumeContext
from .imageformat import ImageFormat
from .lock import Lock, PollLock

MEBIBYTE = 2**20


def _vdi_sanitize(vdi, opq, db, cb):
    """Sanitize vdi metadata object

    When retrieving vdi metadata from the database, it is possible
    that 'vsize' is 'None', if we crashed during a resize operation.
    In this case, query the underlying volume and update 'vsize', both
    in the object and the database
    """
    if vdi.volume.vsize is None:
        image_format = ImageFormat.get_format(vdi.image_type)
        vdi.volume.vsize = image_format.image_utils.get_vsize(
            "", cb.volumeGetPath(opq, str(vdi.volume.id))
        )

        db.update_volume_vsize(vdi.volume.id, vdi.volume.vsize)


def _set_property(dbg, sr, key, field, value, cb):
    with VolumeContext(cb, sr, 'w') as opq:
        with cb.db_context(opq) as db:
            vdi = db.get_vdi_by_id(key)
            if field == 'name':
                db.update_vdi_name(vdi.uuid, value)
            elif field == 'description':
                db.update_vdi_description(vdi.uuid, value)


def _get_size_mib_and_vsize(size):
    # Calculate virtual size (round up size to nearest MiB)
    size_mib = (int(size) - 1) // MEBIBYTE + 1
    vsize = size_mib * MEBIBYTE
    return size_mib, vsize


class COWVolume(object):
    @staticmethod
    def create(dbg, sr, name, description, size, sharable, cb):
        size_mib, vsize = _get_size_mib_and_vsize(size)

        image_format = None

        with VolumeContext(cb, sr, 'w') as opq:
            image_type = ImageFormat.IMAGE_RAW # Support for QCOW2 is disabled until fixed
            image_format = ImageFormat.get_format(image_type)
            vdi_uuid = str(uuid.uuid4())

            with PollLock(opq, 'gl', cb, 0.5):
                with cb.db_context(opq) as db:
                    volume = db.insert_new_volume(vsize, image_type)
                    db.insert_vdi(name, description, vdi_uuid,
                                  volume.id, sharable)
                    volume_path = cb.volumeCreate(opq, str(volume.id), vsize)
                    image_format.image_utils.create(
                        dbg, volume_path, size_mib)

            psize = cb.volumeGetPhysSize(opq, str(volume.id))
            vdi_uri = cb.getVolumeUriPrefix(opq) + vdi_uuid

        return {
            'key': vdi_uuid,
            'uuid': vdi_uuid,
            'name': name,
            'description': description,
            'read_write': True,
            'virtual_size': vsize,
            'physical_utilisation': psize,
            'uri': [image_format.uri_prefix + vdi_uri],
            'sharable': sharable,
            'keys': {}
        }

    @staticmethod
    def destroy(dbg, sr, key, cb):
        with VolumeContext(cb, sr, 'w') as opq:
            with Lock(opq, 'gl', cb):
                with cb.db_context(opq) as db:
                    vdi = db.get_vdi_by_id(key)
                    db.delete_vdi(key)
                with cb.db_context(opq) as db:
                    cb.volumeDestroy(opq, str(vdi.volume.id))
                    db.delete_volume(vdi.volume.id)

    @staticmethod
    def resize(dbg, sr, key, new_size, cb):
        size_mib, vsize = _get_size_mib_and_vsize(new_size)

        with VolumeContext(cb, sr, 'w') as opq:
            image_format = None
            with cb.db_context(opq) as db:
                vdi = db.get_vdi_by_id(key)
                image_format = ImageFormat.get_format(vdi.image_type)
                if vdi.sharable:
                    # TODO: Report Storage error here.
                    raise NotImplementedError(
                        "Sharable VDIs cannot be resized")

                if new_size < vdi.volume.vsize:
                    log.error("Volume cannot be shrunk from {} to {}".
                              format(vdi.volume.vsize, new_size))
                    raise util.create_storage_error("SR_BACKEND_FAILURE_79",
                                                    ["VDI Invalid size",
                                                     "shrinking not allowed"])

                db.update_volume_vsize(vdi.volume.id, None)
            with cb.db_context(opq) as db:
                cb.volumeResize(opq, str(vdi.volume.id), vsize)
                vol_path = cb.volumeGetPath(opq, str(vdi.volume.id))
                if (util.is_block_device(vol_path)):
                    raise util.create_storage_error(
                        'SR_BACKEND_FAILURE_110',
                        ['Cannot resize block device', ''])
                image_format.image_utils.resize(dbg, vol_path, size_mib)
                db.update_volume_vsize(vdi.volume.id, vsize)

    @staticmethod
    def _check_clone(vdi, db, callbacks, image_utils, is_snapshot):
        if vdi.sharable:
            # TODO: Report storage error
            raise NotImplementedError("Sharable VDIs cannot be cloned")

        if db.get_vdi_chain_height(vdi.uuid) >= (
                image_utils.get_max_chain_height()):
            raise util.create_storage_error(
                "SR_BACKEND_FAILURE_109",
                ["The snapshot chain is too long", ""])

        if vdi.active_on:
            if not is_snapshot:
                raise util.create_storage_error(
                    'SR_BACKEND_FAILURE_24',
                    ['The VDI is currently in use', ''])
            current_host = callbacks.get_current_host()
            if vdi.active_on != current_host:
                log.debug("{} can not snapshot a vdi already"
                          " active on {}".format(
                              current_host, vdi.active_on))
                raise xapi.storage.api.v5.volume.Activated_on_another_host(
                    vdi.active_on)

    @staticmethod
    def _clone(dbg, sr, key, cb, is_snapshot):
        snap_uuid = str(uuid.uuid4())
        need_extra_snap = False

        with VolumeContext(cb, sr, 'w') as opq:
            result_volume_id = ''
            with PollLock(opq, 'gl', cb, 0.5):
                with cb.db_context(opq) as db:
                    vdi = db.get_vdi_by_id(key)
                    image_format = ImageFormat.get_format(vdi.image_type)
                    image_utils = image_format.image_utils

                    COWVolume._check_clone(
                        vdi, db, cb, image_utils, is_snapshot)

                    vol_id = (vdi.volume.id if vdi.volume.snap == 0 else
                              vdi.volume.parent_id)

                    vol_path = cb.volumeGetPath(opq, str(vol_id))
                    if (util.is_block_device(vol_path)):
                        raise util.create_storage_error(
                            'SR_BACKEND_FAILURE_82',
                            ['Cannot clone or snapshot block device', ''])

                    snap_volume = db.insert_child_volume(vol_id,
                                                         vdi.volume.vsize)
                    snap_path = cb.volumeCreate(opq, str(snap_volume.id),
                                                vdi.volume.vsize)
                    if vdi.active_on:
                        image_utils.online_snapshot(
                            dbg, snap_path, vol_path, False)
                    else:
                        image_utils.offline_snapshot(
                            dbg, snap_path, vol_path, False)

                    # NB. As an optimisation, "vhd-util snapshot A->B" will
                    #     check if "A" is empty. If it is, it will set
                    #     "B.parent" to "A.parent" instead of "A" (provided
                    #     "A" has a parent) and we are done.
                    #     If "B.parent" still points to "A", we need to
                    #     rebase "A".
                    need_extra_snap = vdi.volume.snap == 0 and (
                        vdi.active_on or image_utils
                        .is_parent_pointing_to_path(dbg, snap_path, vol_path)
                    )
                    if need_extra_snap:
                        db.update_vdi_volume_id(vdi.uuid, snap_volume.id)
                    else:
                        if not vdi.volume.snap:
                            db.update_volume_parent(
                                snap_volume.id, vdi.volume.parent_id)
                        if is_snapshot:
                            db.set_volume_as_snapshot(snap_volume.id)
                        db.insert_vdi(vdi.name, vdi.description,
                                      snap_uuid, snap_volume.id, vdi.sharable)
                        result_volume_id = str(snap_volume.id)

                if need_extra_snap:
                    log.debug("Need extra snap")
                    if vdi.active_on:
                        with cb.db_context(opq) as db:
                            image_utils.refresh_datapath_clone(
                                "Volume.snapshot",
                                cb.get_data_metadata_path(opq, vdi.uuid),
                                snap_path)
                    with cb.db_context(opq) as db:
                        db.update_volume_psize(vdi.volume.id,
                                               cb.volumeGetPhysSize(
                                                   opq, str(vdi.volume.id)))
                        snap_2_volume = db.insert_child_volume(
                            vdi.volume.id, vdi.volume.vsize, is_snapshot)
                        snap_2_path = cb.volumeCreate(opq,
                                                      str(snap_2_volume.id),
                                                      vdi.volume.vsize)
                        if vdi.active_on:
                            image_utils.online_snapshot(
                                dbg, snap_2_path, vol_path, False)
                        else:
                            image_utils.offline_snapshot(
                                dbg, snap_2_path, vol_path, False)
                        db.insert_vdi(vdi.name, vdi.description, snap_uuid,
                                      snap_2_volume.id, vdi.sharable)
                    result_volume_id = str(snap_2_volume.id)

            psize = cb.volumeGetPhysSize(opq, result_volume_id)

            snap_uri = cb.getVolumeUriPrefix(opq) + snap_uuid

        return {
            'uuid': snap_uuid,
            'key': snap_uuid,
            'name': vdi.name,
            'description': vdi.description,
            'read_write': not is_snapshot,
            'virtual_size': vdi.volume.vsize,
            'physical_utilisation': psize,
            'uri': [image_format.uri_prefix + snap_uri],
            'keys': {},
            'sharable': False
        }

    @staticmethod
    def clone(dbg, sr, key, cb):
        return COWVolume._clone(dbg, sr, key, cb, False)

    @staticmethod
    def snapshot(dbg, sr, key, cb):
        return COWVolume._clone(dbg, sr, key, cb, True)

    @staticmethod
    def stat(dbg, sr, key, cb):
        image_format = None

        with VolumeContext(cb, sr, 'r') as opq:
            with cb.db_context(opq) as db:
                vdi = db.get_vdi_by_id(key)
                image_format = ImageFormat.get_format(vdi.image_type)
                _vdi_sanitize(vdi, opq, db, cb)
                custom_keys = db.get_vdi_custom_keys(vdi.uuid)

            psize = cb.volumeGetPhysSize(opq, str(vdi.volume.id))
            vdi_uri = cb.getVolumeUriPrefix(opq) + vdi.uuid

        return {
            'uuid': vdi.uuid,
            'key': vdi.uuid,
            'name': vdi.name,
            'description': vdi.description,
            'read_write': True,
            'virtual_size': vdi.volume.vsize,
            'physical_utilisation': psize,
            'uri': [image_format.uri_prefix + vdi_uri],
            'keys': custom_keys,
            'sharable': bool(vdi.sharable)
        }

    @staticmethod
    def ls(dbg, sr, cb):
        results = []
        with VolumeContext(cb, sr, 'r') as opq:
            with cb.db_context(opq) as db:
                vdis = db.get_all_vdis()
                all_custom_keys = db.get_all_vdi_custom_keys()

            for vdi in vdis:
                _vdi_sanitize(vdi, opq, db, cb)

                image_format = ImageFormat.get_format(vdi.image_type)

                psize = cb.volumeGetPhysSize(opq, str(vdi.volume.id))
                vdi_uri = cb.getVolumeUriPrefix(opq) + vdi.uuid
                custom_keys = {}
                if vdi.uuid in all_custom_keys:
                    custom_keys = all_custom_keys[vdi.uuid]

                results.append({
                    'uuid': vdi.uuid,
                    'key': vdi.uuid,
                    'name': vdi.name,
                    'description': vdi.description,
                    'read_write': True,
                    'virtual_size': vdi.volume.vsize,
                    'physical_utilisation': psize,
                    'uri': [image_format.uri_prefix + vdi_uri],
                    'keys': custom_keys,
                    'sharable': bool(vdi.sharable)
                })

        return results

    @staticmethod
    def set(dbg, sr, key, custom_key, value, cb):
        with VolumeContext(cb, sr, 'r') as opq:
            with cb.db_context(opq) as db:
                db.set_vdi_custom_key(key, custom_key, value)

    @staticmethod
    def unset(dbg, sr, key, custom_key, cb):
        with VolumeContext(cb, sr, 'r') as opq:
            with cb.db_context(opq) as db:
                db.delete_vdi_custom_key(key, custom_key)

    @staticmethod
    def set_name(dbg, sr, key, new_name, cb):
        _set_property(dbg, sr, key, 'name', new_name, cb)

    @staticmethod
    def set_description(dbg, sr, key, new_description, cb):
        _set_property(dbg, sr, key, 'description', new_description, cb)

    @staticmethod
    def get_sr_provisioned_size(sr, cb):
        """Returns tha max space the SR could end up using.

        This is the sum of the physical size of all snapshots,
        plus the virtual size of all VDIs.
        """
        with VolumeContext(cb, sr, 'w') as opq:
            with cb.db_context(opq) as db:
                provisioned_size = db.get_non_leaf_total_psize()
                for vdi in db.get_all_vdis():
                    _vdi_sanitize(vdi, opq, db, cb)
                    provisioned_size += vdi.volume.vsize
        return provisioned_size
