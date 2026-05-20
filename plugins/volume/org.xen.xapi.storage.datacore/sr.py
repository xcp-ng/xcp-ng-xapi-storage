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
        """Enumerate the (first-pool, second-pool) pairs available on the
        DataCore endpoint so the operator can pick one and assemble the
        full SR.create command.

        The SMAPIv3 v5 API expects a list of
            {configuration, complete, sr, extra_info}
        records — one per candidate configuration. We surface one record
        per cross-server pool pair, mark complete=False (the operator still
        needs to add iscsi-portals + host-id + password), and put
        human-readable pool/server names in extra_info so `xe sr-probe`
        output is readable.

        With this in place, the discovery workflow becomes:

            xe sr-probe type=datacore \\
                device-config:rest-endpoint=https://<dc>  \\
                device-config:username=<user>             \\
                device-config:password=<pw>
        """
        log.debug("{}: SR.probe".format(dbg))
        client = datacoreapi.DataCoreClient.from_sr_config(configuration)
        pools = client.list_pools()
        portals_by_server = client.list_iscsi_target_portals_by_server()

        # Group pools by their owning server. Pool ID format is
        # "{ServerId}:{pool-guid}" — the ServerId is the leading prefix and
        # also appears as a top-level field on each pool record.
        pools_by_server = {}
        for p in pools:
            server_id = p.get("ServerId") or datacoreapi._server_id_from_pool(
                p.get("Id", ""))
            if server_id:
                pools_by_server.setdefault(server_id, []).append(p)

        # Enumerate cross-server pairs. (A, B) and (B, A) are the same
        # mirror at the array level so we dedupe by sorting the server
        # IDs: only emit pairs where first.ServerId < second.ServerId.
        # The operator can swap first/second on the actual sr-create
        # command if they want a specific PreferredServer.
        results = []
        servers = sorted(pools_by_server.keys())
        for i, srv_a in enumerate(servers):
            for srv_b in servers[i + 1:]:
                for p_a in pools_by_server[srv_a]:
                    for p_b in pools_by_server[srv_b]:
                        suggested = dict(configuration)
                        suggested["first-pool"] = p_a.get("Id", "")
                        suggested["second-pool"] = p_b.get("Id", "")
                        # Suggested iscsi-portals: one portal per server (simplest
                        # HA). The operator can still override with extra portals
                        # for multi-NIC / additional path redundancy — the full
                        # per-server portal list is exposed in extra_info below.
                        portals_a = portals_by_server.get(srv_a) or []
                        portals_b = portals_by_server.get(srv_b) or []
                        if portals_a and portals_b:
                            suggested["iscsi-portals"] = "{},{}".format(
                                portals_a[0], portals_b[0])
                        extra = {
                            "first-pool-name":  p_a.get("Alias") or p_a.get("Caption") or "",
                            "first-server-id":  srv_a,
                            "second-pool-name": p_b.get("Alias") or p_b.get("Caption") or "",
                            "second-server-id": srv_b,
                        }
                        if portals_a:
                            extra["iscsi-portals-first-server-all"] = ",".join(portals_a)
                        if portals_b:
                            extra["iscsi-portals-second-server-all"] = ",".join(portals_b)
                        # NOTE: omit the `sr` key entirely (don't set to None).
                        # The SMAPIv5 API type-checker on dom0 treats
                        # `'sr' in entry` as "key present" and then unconditionally
                        # subscripts entry['sr']['sr'], which crashes with
                        # "'NoneType' object is not subscriptable" if we set
                        # sr=None. Omitting the key is the correct way to say
                        # "no existing SR is associated with this candidate".
                        results.append({
                            "configuration": suggested,
                            "complete": False,
                            "extra_info": extra,
                        })
        log.info("{}: SR.probe surfaced {} candidate pool pair(s) across {} server(s)".format(
            dbg, len(results), len(servers)))
        return results

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

        portals_raw = configuration.get("iscsi-portals", "")
        if portals_raw:
            iqn = _host_iqn()
            host_id = configuration.get("host-id")
            if not host_id:
                # Re-attach path after a successful first-time setup: the
                # IQN is already registered against a DataCore host, so we
                # can resolve the host-id directly. First-time setup still
                # needs device-config:host-id (or an operator-side IQN
                # pre-registration) to bootstrap the host object.
                host_id = client.find_host_id_by_iqn(iqn)
                if not host_id:
                    raise Exception(
                        "SR.attach: initiator IQN {!r} is not registered "
                        "against any DataCore host, and device-config "
                        "has no 'host-id' to bootstrap from. Either set "
                        "device-config:host-id=<datacore-host-id> once "
                        "(plugin will RegisterPort the IQN and subsequent "
                        "attaches will resolve automatically), or register "
                        "the IQN against an existing host in the DataCore "
                        "GUI / PowerShell first.".format(iqn))
                log.info("{}: SR.attach resolved DataCore host-id={} via IQN {}".format(
                    dbg, host_id, iqn))
            else:
                log.debug("{}: SR.attach using configured host-id={}".format(dbg, host_id))
            client.register_port_idempotent(host_id, iqn)
            portals = [p.strip() for p in portals_raw.split(",") if p.strip()]
            _iscsi_setup(dbg, portals, iqn)
        else:
            log.debug("{}: SR.attach skipping iscsi setup (no iscsi-portals)".format(dbg))
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

        # Mirrored capacity is bounded by the smaller of the two legs (every
        # vDisk consumes its full Size on both pools, so the slower-growing
        # pool gates how much we can host). Allocated = our SR's vDisks.
        # If either lookup fails, report 0/0 — better than raising on a
        # purely-informational call.
        total = 0
        free = 0
        try:
            cap_a = client.pool_capacity_bytes(cfg["first-pool"])
            cap_b = client.pool_capacity_bytes(cfg["second-pool"])
            total = min(cap_a, cap_b)
            allocated = client.sr_allocated_bytes(sr)
            free = max(0, total - allocated)
            log.debug("{}: SR.stat total={} alloc={} free={}".format(
                dbg, total, allocated, free))
        except Exception as e:
            log.warning("{}: SR.stat: capacity lookup failed, reporting 0: {}".format(
                dbg, e))

        return {
            "sr": sr,
            "name": cfg.get("sr-name", "DataCore SR"),
            "description": cfg.get("sr-description", "") or "",
            "total_space": total,
            "free_space": free,
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
