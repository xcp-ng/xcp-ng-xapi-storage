#!/usr/bin/env python3

import os
import sys
import uuid as uuidlib

import xapi.storage.api.v5.volume
from xapi.storage import log

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import datacoreapi
import sr as sr_mod  # for _read_stash


def _reencode_metadata(d, **overrides):
    """Rebuild a vDisk's xcp-ng JSON metadata blob with the given fields overridden.

    Reads the current Description, parses xcp-ng:* keys, merges overrides
    (None values delete keys), and returns the new JSON string suitable for
    `PUT /virtualdisks/{id}`. Preserves unknown keys so we don't lose state
    written by a future plugin version.
    """
    meta = datacoreapi.parse_metadata(d.get("Description"))
    for k, v in overrides.items():
        if v is None:
            meta.pop(k, None)
        else:
            meta[k] = v
    import json
    out = json.dumps(meta, separators=(",", ":"))
    if len(out) > 1024:
        raise datacoreapi.DataCoreError(
            "Metadata blob exceeds 1024 chars after update")
    return out


class Implementation(xapi.storage.api.v5.volume.Volume_skeleton):
    def create(self, dbg, sr, name, description, size, sharable):
        log.debug("{}: Volume.create sr={} name={!r} size={}".format(dbg, sr, name, size))
        cfg = sr_mod._read_stash(sr)
        client = datacoreapi.DataCoreClient.from_sr_config(cfg)

        # DataCore rejects vDisks smaller than the pool's ChunkSize
        # (typically 128 MiB) with HTTP 400 "the disk size provided is
        # less than the storage allocation unit size". XAPI happily
        # asks for sub-chunk VDIs (XO CloudConfigDrive is 10 MiB), so
        # we round up to the pool boundary silently. Same shape as how
        # LVM-backed SRs round to PE boundaries.
        chunk = client.pool_chunk_size_bytes(cfg["first-pool"])
        aligned = client.align_size_to_chunk(size, chunk)
        if aligned != size:
            log.info("{}: Volume.create: rounded size {} -> {} (pool chunk={})".format(
                dbg, size, aligned, chunk))
            size = aligned

        vdi_uuid = str(uuidlib.uuid4())
        vdisk_name = "{}{}".format(datacoreapi.vdisk_prefix(sr), vdi_uuid)
        meta = datacoreapi.encode_metadata(
            vdi_uuid=vdi_uuid,
            sr_uuid=sr,
            vdi_name=name,
            description=description,
            sharable=sharable,
            read_write=True,
        )

        d = client.create_mirrored_vdisk(
            vdisk_name,
            cfg["first-pool"],
            cfg["second-pool"],
            size,
            description=meta,
        )
        log.debug("{}: Volume.create -> Id={} (waiting for mirror sync)".format(dbg, d["Id"]))
        # Block until DiskStatus=Online so an immediately-following snapshot
        # or clone doesn't hit DataCore's "not up-to-date" 400.
        d = client.wait_for_vdisk_online(d["Id"])
        log.info("{}: Volume.create: vdisk={} VDI={} size={} ready".format(
            dbg, d["Id"], vdi_uuid, size))
        return datacoreapi.vdisk_to_vdi_info(d, sr)

    def destroy(self, dbg, sr, key):
        log.debug("{}: Volume.destroy sr={} key={}".format(dbg, sr, key))
        cfg = sr_mod._read_stash(sr)
        client = datacoreapi.DataCoreClient.from_sr_config(cfg)
        d = datacoreapi.find_vdisk_by_vdi_uuid(client, sr, key)
        if d is None:
            log.debug("{}: Volume.destroy: VDI {} not found, treating as already destroyed".format(dbg, key))
            return
        # Clones produced by Volume.clone are differential-snapshot
        # destinations: a snapshot RECORD on the array points at one of
        # this vDisk's logical disks. DataCore refuses DELETE on the vDisk
        # while that snapshot record exists. Find the record (if any) and
        # delete it first — that cascades to delete the vDisk itself.
        snap_record = datacoreapi.find_snapshot_record_by_dest_vdisk(client, d)
        if snap_record:
            log.info("{}: Volume.destroy: deleting snapshot record {} (cascades to vDisk)".format(
                dbg, snap_record["Id"]))
            try:
                datacoreapi.delete_snapshot_record(client, snap_record["Id"])
                log.info("{}: Volume.destroy: snapshot+vdisk cascade-deleted".format(dbg))
                return
            except Exception as e:
                log.debug("{}: Volume.destroy: snapshot record delete failed: {} (falling through to direct delete)".format(dbg, e))
        # Best-effort Unserve before delete. DataCore refuses DELETE on a
        # vDisk that is still Served to any host ("is served to one or
        # multiple hosts and cannot be deleted"). The usual lifecycle has
        # Datapath.detach call Unserve first, but XAPI's rollback path for
        # a failed Datapath.attach skips Datapath.detach and goes straight
        # to Volume.destroy. Calling Unserve here (and ignoring its result)
        # makes Volume.destroy safe to invoke at any point.
        host_id = cfg.get("host-id")
        if host_id and d.get("IsServed"):
            try:
                client.unserve_vdisk(d["Id"], host_id)
            except datacoreapi.DataCoreError as e:
                log.debug("{}: Volume.destroy: Unserve failed (continuing): {}".format(
                    dbg, e))
        client.delete_vdisk(d["Id"])
        log.info("{}: Volume.destroy: deleted vdisk={} (VDI {})".format(dbg, d["Id"], key))

    def resize(self, dbg, sr, key, new_size):
        log.debug("{}: Volume.resize sr={} key={} new_size={}".format(dbg, sr, key, new_size))
        cfg = sr_mod._read_stash(sr)
        client = datacoreapi.DataCoreClient.from_sr_config(cfg)
        d = datacoreapi.find_vdisk_by_vdi_uuid(client, sr, key)
        if d is None:
            raise xapi.storage.api.v5.volume.Volume_does_not_exist(key)
        current = d["Size"]["Value"]
        if new_size < current:
            raise Exception(
                "Volume.resize: shrinking not supported (current={} new={})".format(
                    current, new_size))
        if new_size == current:
            log.debug("{}: Volume.resize: already at {} bytes, no-op".format(dbg, current))
            return
        try:
            client.resize_vdisk(d["Id"], new_size)
            # Grow triggers a brief mirror re-sync (DiskStatus 0 -> 1 -> 0).
            # Block until Online so an immediately-following snapshot/clone
            # doesn't race the "not up-to-date" check the same way the
            # post-create wait protects against.
            client.wait_for_vdisk_online(d["Id"])
            log.info("{}: Volume.resize: vdisk={} {} -> {} bytes".format(
                dbg, d["Id"], current, new_size))
            return
        except datacoreapi.DataCoreError as e:
            if "snapshots attached" not in str(e).lower() \
                    and "cannot be resized" not in str(e).lower():
                raise
            # The vDisk is itself a differential-snapshot destination (a
            # fast clone). DataCore won't resize it — the geometry is tied
            # to its parent. Promote to an independent mirrored vDisk at
            # the new size; the clone becomes detached from the template.
            #
            # This is the hybrid path: fast clones stay fast in the common
            # case (same-size as template), and only pay the copy cost
            # when something actually asks them to grow.
            log.info(
                "{}: Volume.resize: clone {} can't be resized in place "
                "(differential snapshot of a smaller source); promoting "
                "to an independent vDisk at {} bytes".format(
                    dbg, d["Id"], new_size))
            datacoreapi.promote_to_independent(client, cfg, sr, d, new_size, dbg)

    def stat(self, dbg, sr, key):
        log.debug("{}: Volume.stat sr={} key={}".format(dbg, sr, key))
        cfg = sr_mod._read_stash(sr)
        client = datacoreapi.DataCoreClient.from_sr_config(cfg)
        d = datacoreapi.find_vdisk_by_vdi_uuid(client, sr, key)
        if d is None:
            raise xapi.storage.api.v5.volume.Volume_does_not_exist(key)
        return datacoreapi.vdisk_to_vdi_info(d, sr)

    def snapshot(self, dbg, sr, key):
        """Take a crash-consistent point-in-time snapshot of `key`.

        DataCore Type=1 differential snapshot: instant, COW on the array,
        dependent on the source. Use this for backup checkpoints and the
        cutover-delta machinery in outbound migration — anywhere you want
        "a frozen view of the source as it was at this instant".
        """
        log.debug("{}: Volume.snapshot sr={} key={}".format(dbg, sr, key))
        cfg = sr_mod._read_stash(sr)
        client = datacoreapi.DataCoreClient.from_sr_config(cfg)
        src = datacoreapi.find_vdisk_by_vdi_uuid(client, sr, key)
        if src is None:
            raise xapi.storage.api.v5.volume.Volume_does_not_exist(key)

        # DataCore Type=1 snapshots are always single-pool ("The snapshot can
        # only exist on one server"). Per the docs' best practice, place the
        # snapshot on the non-preferred side of the source's mirror so capacity
        # balances across both servers. Single-server failure of the chosen
        # pool will still lose the snapshot — that's a hard DataCore limit,
        # documented as a HA gap in the plugin README.
        dest_pool = datacoreapi.pick_snapshot_pool(cfg, src)
        src_meta = datacoreapi.parse_metadata(src.get("Description"))
        new_uuid = str(uuidlib.uuid4())
        vdisk_name = "{}{}".format(datacoreapi.vdisk_prefix(sr), new_uuid)
        log.info("{}: Volume.snapshot src={} dest_pool={} new_uuid={}".format(
            dbg, src["Id"], dest_pool, new_uuid))

        d = client.create_differential_snapshot(
            source_vdisk_id=src["Id"],
            name=vdisk_name,
            destination_pool=dest_pool,
        )
        meta = datacoreapi.encode_metadata(
            vdi_uuid=new_uuid,
            sr_uuid=sr,
            vdi_name=src_meta.get("xcp-ng:vdi-name", ""),
            description=src_meta.get("xcp-ng:vdi-description", ""),
            sharable=bool(src_meta.get("xcp-ng:sharable", False)),
            read_write=False,
            is_snapshot=True,
            parent_vdi_uuid=key,
        )
        client.update_description(d["Id"], meta)
        d = client.find_vdisk_by_id(d["Id"]) or d
        d["Description"] = meta
        return datacoreapi.vdisk_to_vdi_info(d, sr)

    def clone(self, dbg, sr, key):
        """Create an INDEPENDENT writable copy of `key`.

        Why full-copy instead of a Type=1 differential snapshot:

        DataCore's Type=1 differential snapshot is instant and COW-cheap,
        but the resulting vDisk is a snapshot DESTINATION (Type=0). DataCore
        then refuses several downstream operations on Type=0 vDisks:
          * `xe vdi-resize` larger than source — "has snapshots attached"
            (geometry tied to parent). Breaks XAPI's vm-install resize step.
          * `xe vm-snapshot` on the resulting VM — "cannot create a snapshot
            for virtual disk because it is a snapshot destination". Breaks
            backup/checkpoint workflows.
          * Other chained-dependency complications.

        We briefly tried a hybrid (fast clone + lazy promote-on-resize), but
        the snapshot-of-snapshot wall is unreachable without leaving XAPI:
        a running VM whose boot disk is Type=0 cannot be promoted in-flight
        because we can't take a crash-consistent snapshot of it to read from.

        Going full-copy by default trades ~50 s per clone (host-side qemu-img
        convert through dom0) for a Type=2 mirrored vDisk that supports every
        downstream operation. The cost is a one-time provisioning hit; the
        alternative regressed every snapshot workflow thereafter.
        """
        log.debug("{}: Volume.clone sr={} key={}".format(dbg, sr, key))
        cfg = sr_mod._read_stash(sr)
        host_id = cfg.get("host-id")
        if not host_id:
            raise Exception("Volume.clone: SR config missing 'host-id'")
        client = datacoreapi.DataCoreClient.from_sr_config(cfg)
        src = datacoreapi.find_vdisk_by_vdi_uuid(client, sr, key)
        if src is None:
            raise xapi.storage.api.v5.volume.Volume_does_not_exist(key)

        src_meta = datacoreapi.parse_metadata(src.get("Description"))
        new_uuid = str(uuidlib.uuid4())
        new_vdisk_name = "{}{}".format(datacoreapi.vdisk_prefix(sr), new_uuid)
        size = int(src["Size"]["Value"])

        # 1. Take a Type=1 snapshot of the source so the qemu-img convert
        #    reads a crash-consistent point-in-time, even if the source is
        #    currently attached to a running VM. We delete the snapshot at
        #    the end of the clone.
        snap_pool = datacoreapi.pick_snapshot_pool(cfg, src)
        snap_name = "{}clone-src-{}".format(
            datacoreapi.vdisk_prefix(sr), new_uuid[:8])
        log.info("{}: Volume.clone: snapshotting src={} for consistent read".format(
            dbg, src["Id"]))
        snap = client.create_differential_snapshot(
            src["Id"], snap_name, snap_pool,
            description="xcp:clone-source-snapshot")
        snap_id = snap["Id"]
        client.wait_for_vdisk_online(snap_id)
        snap = client.find_vdisk_by_id(snap_id) or snap
        snap_wwn = snap["ScsiDeviceIdString"].lower()

        # 2. Provision the destination as a fresh Type=2 mirrored vDisk at
        #    the source's size. Independent from creation — supports resize,
        #    snapshot, every subsequent operation.
        meta = datacoreapi.encode_metadata(
            vdi_uuid=new_uuid,
            sr_uuid=sr,
            vdi_name=src_meta.get("xcp-ng:vdi-name", ""),
            description=src_meta.get("xcp-ng:vdi-description", ""),
            sharable=bool(src_meta.get("xcp-ng:sharable", False)),
            read_write=True,
            is_snapshot=False,
            parent_vdi_uuid=key,
        )
        log.info("{}: Volume.clone: creating dest vDisk {} size={}".format(
            dbg, new_vdisk_name, size))
        new_d = client.create_mirrored_vdisk(
            new_vdisk_name,
            cfg["first-pool"], cfg["second-pool"],
            size,
            description=meta,
        )
        new_d = client.wait_for_vdisk_online(new_d["Id"])
        new_wwn = new_d["ScsiDeviceIdString"].lower()

        # 3. Serve both to dom0 and copy via qemu-img convert.
        try:
            datacoreapi.copy_vdisk_data(
                client, host_id, snap_id, snap_wwn, new_d["Id"], new_wwn, dbg)
        finally:
            # 4. Tear down everything except the new vDisk (XAPI will Serve
            #    the new one later via Datapath.attach when a VBD plugs in).
            datacoreapi.unserve_and_evict(client, host_id, snap_id, snap_wwn, dbg)
            datacoreapi.unserve_and_evict(client, host_id, new_d["Id"], new_wwn, dbg)
            try:
                client.delete_vdisk(snap_id)
            except Exception as e:
                log.error("{}: clone: snap delete failed: {}".format(dbg, e))

        log.info("{}: Volume.clone: new_uuid={} done".format(dbg, new_uuid))
        return datacoreapi.vdisk_to_vdi_info(new_d, sr)

    def set_description(self, dbg, sr, key, new_description):
        log.debug("{}: Volume.set_description sr={} key={}".format(dbg, sr, key))
        self._update_metadata(sr, key, **{"xcp-ng:vdi-description": (new_description or "")[:200]})

    def set_name(self, dbg, sr, key, new_name):
        log.debug("{}: Volume.set_name sr={} key={}".format(dbg, sr, key))
        self._update_metadata(sr, key, **{"xcp-ng:vdi-name": (new_name or "")[:200]})

    def set(self, dbg, sr, key, k, v):
        log.debug("{}: Volume.set sr={} key={} {}={!r}".format(dbg, sr, key, k, v))
        self._update_metadata(sr, key, **{"xcp-ng:custom:{}".format(k): v})

    def unset(self, dbg, sr, key, k):
        log.debug("{}: Volume.unset sr={} key={} k={}".format(dbg, sr, key, k))
        self._update_metadata(sr, key, **{"xcp-ng:custom:{}".format(k): None})

    def _update_metadata(self, sr, key, **overrides):
        cfg = sr_mod._read_stash(sr)
        client = datacoreapi.DataCoreClient.from_sr_config(cfg)
        d = datacoreapi.find_vdisk_by_vdi_uuid(client, sr, key)
        if d is None:
            raise xapi.storage.api.v5.volume.Volume_does_not_exist(key)
        new_desc = _reencode_metadata(d, **overrides)
        client.update_description(d["Id"], new_desc)


if __name__ == "__main__":
    log.log_call_argv()
    cmd = xapi.storage.api.v5.volume.Volume_commandline(Implementation())
    base = os.path.basename(sys.argv[0])
    if base == "Volume.create":
        cmd.create()
    elif base == "Volume.destroy":
        cmd.destroy()
    elif base == "Volume.resize":
        cmd.resize()
    elif base == "Volume.stat":
        cmd.stat()
    elif base == "Volume.snapshot":
        cmd.snapshot()
    elif base == "Volume.clone":
        cmd.clone()
    elif base == "Volume.set_description":
        cmd.set_description()
    elif base == "Volume.set_name":
        cmd.set_name()
    elif base == "Volume.set":
        cmd.set()
    elif base == "Volume.unset":
        cmd.unset()
    else:
        raise xapi.storage.api.v5.volume.Unimplemented(base)
