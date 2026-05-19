#!/usr/bin/env python3

import os
import sys
import uuid as uuidlib

import xapi.storage.api.v5.volume
from xapi.storage import log

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import datacoreapi
import sr as sr_mod  # for _read_stash


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
        log.debug("{}: Volume.create -> Id={}".format(dbg, d["Id"]))
        return datacoreapi.vdisk_to_vdi_info(d, sr)

    def destroy(self, dbg, sr, key):
        log.debug("{}: Volume.destroy sr={} key={}".format(dbg, sr, key))
        cfg = sr_mod._read_stash(sr)
        client = datacoreapi.DataCoreClient.from_sr_config(cfg)
        d = datacoreapi.find_vdisk_by_vdi_uuid(client, sr, key)
        if d is None:
            log.debug("{}: Volume.destroy: VDI {} not found, treating as already destroyed".format(dbg, key))
            return
        client.delete_vdisk(d["Id"])

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

    def stat(self, dbg, sr, key):
        log.debug("{}: Volume.stat sr={} key={}".format(dbg, sr, key))
        cfg = sr_mod._read_stash(sr)
        client = datacoreapi.DataCoreClient.from_sr_config(cfg)
        d = datacoreapi.find_vdisk_by_vdi_uuid(client, sr, key)
        if d is None:
            raise xapi.storage.api.v5.volume.Volume_does_not_exist(key)
        return datacoreapi.vdisk_to_vdi_info(d, sr)

    def set_description(self, dbg, sr, key, new_description):
        # MVP: XAPI tracks name/description in its DB; we accept silently.
        log.debug("{}: Volume.set_description (no-op in MVP) sr={} key={}".format(dbg, sr, key))

    def set_name(self, dbg, sr, key, new_name):
        log.debug("{}: Volume.set_name (no-op in MVP) sr={} key={}".format(dbg, sr, key))

    def set(self, dbg, sr, key, k, v):
        # Custom KV pair (xe vdi-param-set). MVP: accept and discard.
        # Real impl would PUT a refreshed Description with xcp-ng:custom:{k}={v}.
        log.debug("{}: Volume.set (no-op in MVP) sr={} key={} {}={}".format(dbg, sr, key, k, v))

    def unset(self, dbg, sr, key, k):
        log.debug("{}: Volume.unset (no-op in MVP) sr={} key={} k={}".format(dbg, sr, key, k))


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
