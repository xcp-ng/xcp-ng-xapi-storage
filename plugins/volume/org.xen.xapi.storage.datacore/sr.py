#!/usr/bin/env python3

import json
import os
import subprocess
import sys

import xapi.storage.api.v5.volume
from xapi.storage import log

# Allow `import datacoreapi` whether invoked directly or via a SR.* symlink
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import datacoreapi


INITIATOR_FILE = "/etc/iscsi/initiatorname.iscsi"


def _host_iqn():
    with open(INITIATOR_FILE) as f:
        for line in f:
            if line.startswith("InitiatorName="):
                return line.strip().split("=", 1)[1]
    raise Exception("Could not read InitiatorName from {}".format(INITIATOR_FILE))


def _iscsi_setup(dbg, portals, iqn):
    """Discover + login on each portal. Idempotent — tolerates already-logged-in."""
    for portal in portals:
        portal = portal.strip()
        if not portal:
            continue
        log.debug("{}: iscsi discovery on {}".format(dbg, portal))
        subprocess.run(["iscsiadm", "-m", "discovery", "-t", "st", "-p", portal],
                       check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # `iscsiadm -m node --login` logs into every discovered node;
    # already-logged-in returns non-zero per-node but does no harm.
    subprocess.run(["iscsiadm", "-m", "node", "--login"], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def _iscsi_teardown(dbg):
    log.debug("{}: iscsi logout (all sessions)".format(dbg))
    subprocess.run(["iscsiadm", "-m", "node", "--logout"], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


STASH_DIR = "/run/datacore-sr"


def _stash_path(sr_uuid):
    return os.path.join(STASH_DIR, "{}.json".format(sr_uuid))


def _write_stash(sr_uuid, cfg):
    os.makedirs(STASH_DIR, exist_ok=True)
    tmp = _stash_path(sr_uuid) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(cfg, f)
    os.replace(tmp, _stash_path(sr_uuid))


def _read_stash(sr_handle):
    with open(_stash_path(sr_handle)) as f:
        return json.load(f)


class Implementation(xapi.storage.api.v5.volume.SR_skeleton):
    def probe(self, dbg, configuration):
        log.debug("{}: SR.probe".format(dbg))
        client = datacoreapi.DataCoreClient.from_sr_config(configuration)
        client.list_pools()
        return {"srs": [], "uris": []}

    def create(self, dbg, sr_uuid, configuration, name, description):
        log.debug("{}: SR.create uuid={} name={!r} cfg-keys={}".format(
            dbg, sr_uuid, name, sorted(configuration.keys())))
        client = datacoreapi.DataCoreClient.from_sr_config(configuration)
        client.list_pools()
        cfg = dict(configuration)
        cfg["sr-uuid"] = sr_uuid
        cfg["sr-name"] = name
        cfg["sr-description"] = description
        _write_stash(sr_uuid, cfg)
        # XAPI persists the returned configuration in device-config
        # We embed sr-uuid so SR.attach can rebuild the stash after a reboot.
        return cfg

    def attach(self, dbg, configuration):
        log.debug("{}: SR.attach cfg keys={}".format(dbg, list(configuration.keys())))
        sr_uuid = configuration.get("sr-uuid")
        if not sr_uuid:
            raise Exception("SR.attach: configuration missing 'sr-uuid' (was SR.create run?)")
        # Invalidate any prior password cache so a credential change on the
        # operator side (e.g. xe sr-param-set device-config:password=...) is
        # picked up. The XAPI lookup inside `from_sr_config` will repopulate.
        datacoreapi.clear_password_cache(sr_uuid)
        client = datacoreapi.DataCoreClient.from_sr_config(configuration)
        client.list_pools()
        _write_stash(sr_uuid, configuration)

        host_id = configuration.get("host-id")
        portals_raw = configuration.get("iscsi-portals", "")
        if host_id and portals_raw:
            iqn = _host_iqn()
            log.debug("{}: SR.attach register IQN {} against DataCore host {}".format(dbg, iqn, host_id))
            client.register_port_idempotent(host_id, iqn)
            portals = [p.strip() for p in portals_raw.split(",") if p.strip()]
            _iscsi_setup(dbg, portals, iqn)
        else:
            log.debug("{}: SR.attach skipping iscsi setup (no host-id or iscsi-portals)".format(dbg))
        return sr_uuid

    def detach(self, dbg, sr):
        log.debug("{}: SR.detach sr={}".format(dbg, sr))
        # NOTE: we deliberately do NOT log out iSCSI sessions here. Other SRs
        # (legacy lvmoiscsi, or another DataCore SR on the same array) may
        # share the same target IQNs; an unconditional `iscsiadm -m node
        # --logout` kills their live paths too. dm-multipath then has no
        # paths, `no_path_retry queue` blocks I/O, and any subsequent LV
        # deactivate / pbd-unplug hangs (observed; also explained the slow
        # host shutdown after SMAPIv3 + SMAPIv1 coexistence).
        # iSCSI sessions are cheap to leave open; the next SR.attach
        # re-logs in idempotently. Reference-counting is a future fix.
        try:
            os.unlink(_stash_path(sr))
        except FileNotFoundError:
            pass
        datacoreapi.clear_password_cache(sr)

    def destroy(self, dbg, sr):
        log.debug("{}: SR.destroy sr={}".format(dbg, sr))
        cfg = _read_stash(sr)
        client = datacoreapi.DataCoreClient.from_sr_config(cfg)
        prefix = datacoreapi.vdisk_prefix(sr)
        ours = [d for d in client.list_virtualdisks() if d.get("Alias", "").startswith(prefix)]
        if ours:
            raise Exception(
                "SR.destroy refused: {} vDisk(s) still present in SR (delete them first)".format(len(ours))
            )
        self.detach(dbg, sr)

    def ls(self, dbg, sr):
        log.debug("{}: SR.ls sr={}".format(dbg, sr))
        cfg = _read_stash(sr)
        client = datacoreapi.DataCoreClient.from_sr_config(cfg)
        prefix = datacoreapi.vdisk_prefix(sr)
        return [
            datacoreapi.vdisk_to_vdi_info(d, sr)
            for d in client.list_virtualdisks()
            if d.get("Alias", "").startswith(prefix)
        ]

    def stat(self, dbg, sr):
        log.debug("{}: SR.stat sr={}".format(dbg, sr))
        cfg = _read_stash(sr)
        client = datacoreapi.DataCoreClient.from_sr_config(cfg)
        # /pools doesn't return capacity in the bare list response.
        # For the spike we report 0 — a follow-up will probe a richer per-pool endpoint.
        client.list_pools()
        return {
            "sr": sr,
            "name": cfg.get("sr-name", "DataCore SR"),
            "description": cfg.get("sr-description", "") or "",
            "total_space": 0,
            "free_space": 0,
            "uuid": sr,
            "datasources": [],
            "clustered": True,
            "health": ["Healthy", ""],
        }

    def set_name(self, dbg, sr, new_name):
        cfg = _read_stash(sr)
        cfg["sr-name"] = new_name
        _write_stash(sr, cfg)

    def set_description(self, dbg, sr, new_description):
        cfg = _read_stash(sr)
        cfg["sr-description"] = new_description
        _write_stash(sr, cfg)


if __name__ == "__main__":
    log.log_call_argv()
    cmd = xapi.storage.api.v5.volume.SR_commandline(Implementation())
    base = os.path.basename(sys.argv[0])
    if base == "SR.probe":
        cmd.probe()
    elif base == "SR.attach":
        cmd.attach()
    elif base == "SR.create":
        cmd.create()
    elif base == "SR.destroy":
        cmd.destroy()
    elif base == "SR.detach":
        cmd.detach()
    elif base == "SR.ls":
        cmd.ls()
    elif base == "SR.set_description":
        cmd.set_description()
    elif base == "SR.set_name":
        cmd.set_name()
    elif base == "SR.stat":
        cmd.stat()
    else:
        raise xapi.storage.api.v5.volume.Unimplemented(base)
