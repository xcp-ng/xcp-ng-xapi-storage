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
        client.resize_vdisk(d["Id"], new_size)
        # Grow triggers a brief mirror re-sync (DiskStatus 0 -> 1 -> 0).
        # Block until Online so an immediately-following snapshot/clone
        # doesn't race the "not up-to-date" check the same way the
        # post-create wait protects against.
        client.wait_for_vdisk_online(d["Id"])
        log.info("{}: Volume.resize: vdisk={} {} -> {} bytes".format(
            dbg, d["Id"], current, new_size))

    def stat(self, dbg, sr, key):
        log.debug("{}: Volume.stat sr={} key={}".format(dbg, sr, key))
        cfg = sr_mod._read_stash(sr)
        client = datacoreapi.DataCoreClient.from_sr_config(cfg)
        d = datacoreapi.find_vdisk_by_vdi_uuid(client, sr, key)
        if d is None:
            raise xapi.storage.api.v5.volume.Volume_does_not_exist(key)
        return datacoreapi.vdisk_to_vdi_info(d, sr)

    def snapshot(self, dbg, sr, key):
        return self._snapshot_or_clone(dbg, sr, key, is_snapshot=True)

    def clone(self, dbg, sr, key):
        return self._snapshot_or_clone(dbg, sr, key, is_snapshot=False)

    def _snapshot_or_clone(self, dbg, sr, key, is_snapshot):
        op = "snapshot" if is_snapshot else "clone"
        log.debug("{}: Volume.{} sr={} key={}".format(dbg, op, sr, key))
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
        # Inherit source's display name unless it was empty.
        src_name = src_meta.get("xcp-ng:vdi-name", "")
        src_desc = src_meta.get("xcp-ng:vdi-description", "")

        new_uuid = str(uuidlib.uuid4())
        vdisk_name = "{}{}".format(datacoreapi.vdisk_prefix(sr), new_uuid)
        log.info("{}: Volume.{} src={} dest_pool={} new_uuid={}".format(
            dbg, op, src["Id"], dest_pool, new_uuid))

        d = client.create_differential_snapshot(
            source_vdisk_id=src["Id"],
            name=vdisk_name,
            destination_pool=dest_pool,
        )

        # Description does NOT propagate from source to snapshot — must PUT
        # explicitly. Snapshots are returned read_write=False (XAPI semantic);
        # clones are returned read_write=True. The DataCore array itself
        # accepts writes on a Type=1 snapshot regardless.
        meta = datacoreapi.encode_metadata(
            vdi_uuid=new_uuid,
            sr_uuid=sr,
            vdi_name=src_name,
            description=src_desc,
            sharable=bool(src_meta.get("xcp-ng:sharable", False)),
            read_write=(not is_snapshot),
            is_snapshot=is_snapshot,
            parent_vdi_uuid=key,
        )
        client.update_description(d["Id"], meta)
        # Re-fetch so vdi_info reflects the new Description.
        d = client.find_vdisk_by_id(d["Id"]) or d
        d["Description"] = meta  # ensure parse_metadata sees the new blob
        return datacoreapi.vdisk_to_vdi_info(d, sr)

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
