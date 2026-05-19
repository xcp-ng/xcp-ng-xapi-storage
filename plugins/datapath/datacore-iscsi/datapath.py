#!/usr/bin/env python3

"""
Datapath for DataCore vDisks served via iSCSI.

URI shape:  datacore-iscsi://<sr-uuid>/<vdisk-id>

`attach`  Serve on the array, rescan iSCSI, wait for the per-path sd device,
          tune scheduler, register WWID with dm-multipath (when multipathd is
          active), wait for /dev/mapper/3<wwn>, and return that path.
`detach`  Flush the dm-multipath map first, then evict the SCSI paths, then
          Unserve on the array.

If multipathd is not active on the host (single-path setup), `attach` returns
the /dev/disk/by-id/scsi-3<wwn> symlink instead, and `detach` skips the flush.

Assumes `SR.attach` has already established iSCSI sessions to the DataCore
portals — DataCore's Serve refuses with ErrorCode 4 if there's no live session
between initiator and target.
"""

import json
import os
import subprocess
import sys
import time
from urllib.parse import urlparse

import xapi.storage.api.v5.datapath
from xapi.storage import log

# Reuse the REST client from the volume plugin (installed alongside)
sys.path.insert(
    0,
    "/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.datacore",
)
import datacoreapi  # noqa: E402


STASH_DIR = "/run/datacore-sr"
DEVICE_POLL_TIMEOUT = 30  # seconds
DEVICE_POLL_INTERVAL = 0.5
MPATH_POLL_TIMEOUT = 15
MPATH_POLL_INTERVAL = 0.3


def _read_stash(sr_uuid):
    with open(os.path.join(STASH_DIR, "{}.json".format(sr_uuid))) as f:
        return json.load(f)


def _parse_uri(uri):
    # datacore-iscsi://<sr-uuid>/<vdisk-id>
    u = urlparse(uri)
    if u.scheme != "datacore-iscsi":
        raise Exception("Unsupported URI scheme: {}".format(u.scheme))
    return u.netloc, u.path.lstrip("/")


def _wait_for_device(wwn, dbg):
    dev = "/dev/disk/by-id/scsi-3{}".format(wwn)
    deadline = time.time() + DEVICE_POLL_TIMEOUT
    while time.time() < deadline:
        if os.path.exists(dev):
            log.debug("{}: device {} ready -> {}".format(dbg, dev, os.path.realpath(dev)))
            return dev
        time.sleep(DEVICE_POLL_INTERVAL)
    raise Exception("Device {} did not appear within {}s".format(dev, DEVICE_POLL_TIMEOUT))


def _rescan_iscsi():
    subprocess.run(["iscsiadm", "-m", "session", "--rescan"],
                   check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def _iter_sd_paths_for_wwn(wwn):
    """Yield the sd* device names for every Dom0 SCSI path to a given WWN."""
    out = subprocess.run(["lsblk", "-rno", "NAME,WWN"],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         check=False)
    target = "0x" + wwn.lower()
    for line in out.stdout.decode().splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[1].lower() == target and parts[0].startswith("sd"):
            yield parts[0]


def _evict_scsi_paths_for_wwn(wwn, dbg):
    """Delete every /dev/sd* path whose WWN matches via /sys/block/<dev>/device/delete.

    Without this, after Unserve the orphaned sd* entries linger and confuse the
    next rescan — the device reappears but without the WWN attribute so udev
    never recreates the /dev/disk/by-id/scsi-3<wwn> symlink we wait for.
    """
    try:
        for name in _iter_sd_paths_for_wwn(wwn):
            sysfile = "/sys/block/{}/device/delete".format(name)
            if os.path.exists(sysfile):
                log.debug("{}: evict /dev/{}".format(dbg, name))
                with open(sysfile, "w") as f:
                    f.write("1")
    except Exception as e:
        log.error("{}: scsi evict failed: {}".format(dbg, e))


def _multipath_active():
    """True if multipathd is running. Drives whether attach uses /dev/mapper."""
    r = subprocess.run(["systemctl", "is-active", "multipathd"],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    return r.stdout.decode().strip() == "active"


def _register_with_multipath(wwn, dbg):
    """Add the WWID to /etc/multipath/wwids and trigger map creation.

    XCP-ng's multipath.conf sets `find_multipaths yes`, so dm-multipath only
    builds a map for WWIDs that have been explicitly registered. XAPI's
    lvmoiscsi backend does the equivalent at SR.create time; we do it per
    vDisk attach.
    """
    mpath_wwid = "3" + wwn.lower()
    subprocess.run(["multipath", "-a", mpath_wwid],
                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    # Scan; idempotent — already-existing maps are left alone, new eligible
    # WWIDs get a map created.
    subprocess.run(["multipath"],
                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    log.debug("{}: multipath registered {}".format(dbg, mpath_wwid))


def _wait_for_mpath(wwn, dbg):
    """Poll for /dev/mapper/3<wwn>. Returns the path or None on timeout."""
    target = "/dev/mapper/3" + wwn.lower()
    deadline = time.time() + MPATH_POLL_TIMEOUT
    while time.time() < deadline:
        if os.path.exists(target):
            log.debug("{}: mpath device ready {}".format(dbg, target))
            return target
        time.sleep(MPATH_POLL_INTERVAL)
    log.error("{}: mpath device {} did not appear within {}s".format(
        dbg, target, MPATH_POLL_TIMEOUT))
    return None


def _flush_multipath(wwn, dbg):
    """Flush the dm map for this WWID. Non-zero exit is harmless (no map)."""
    mpath_wwid = "3" + wwn.lower()
    r = subprocess.run(["multipath", "-f", mpath_wwid],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    log.debug("{}: multipath -f {} exit={}".format(dbg, mpath_wwid, r.returncode))


def _write_sysfs(path, value, dbg):
    try:
        with open(path, "w") as f:
            f.write(value)
        log.debug("{}: wrote {!r} to {}".format(dbg, value, path))
        return True
    except IOError as e:
        log.error("{}: write to {} failed: {}".format(dbg, path, e))
        return False


def _tune_scheduler(wwn, dbg, scheduler="noop"):
    """Set the block-layer I/O scheduler on every device involved in this LUN.

    XCP-ng 8.3's kernel defaults SCSI devices (and dm-multipath devices) to cfq
    — the legacy spinning-disk scheduler that adds per-process queueing latency
    actively harmful on iSCSI LUNs. XAPI's own lvmoiscsi backend tunes its
    devices to noop; we match that so SR types compare apples-to-apples.

    Tunes BOTH the underlying sd paths (in case any I/O bypasses dm) AND the
    /dev/mapper dm device that actually carries blkback's I/O. Must run AFTER
    `multipath` has settled — earlier runs get clobbered by udev events the
    multipath reconfigure emits.
    """
    for name in _iter_sd_paths_for_wwn(wwn):
        sysfile = "/sys/block/{}/queue/scheduler".format(name)
        if os.path.exists(sysfile):
            _write_sysfs(sysfile, scheduler, dbg)

    mpath = "/dev/mapper/3" + wwn.lower()
    if os.path.exists(mpath):
        dm_name = os.path.basename(os.path.realpath(mpath))  # e.g. "dm-4"
        sysfile = "/sys/block/{}/queue/scheduler".format(dm_name)
        if os.path.exists(sysfile):
            _write_sysfs(sysfile, scheduler, dbg)


class Implementation(xapi.storage.api.v5.datapath.Datapath_skeleton):
    def open(self, dbg, uri, persistent):
        log.debug("{}: Datapath.open uri={} persistent={}".format(dbg, uri, persistent))

    def close(self, dbg, uri):
        log.debug("{}: Datapath.close uri={}".format(dbg, uri))

    def attach(self, dbg, uri, domain):
        log.debug("{}: Datapath.attach uri={} domain={}".format(dbg, uri, domain))
        sr_uuid, vdisk_id = _parse_uri(uri)
        cfg = _read_stash(sr_uuid)
        host_id = cfg.get("host-id")
        if not host_id:
            raise Exception("Datapath.attach: SR config missing 'host-id'")

        client = datacoreapi.DataCoreClient.from_sr_config(cfg)
        d = client.find_vdisk_by_id(vdisk_id)
        if d is None:
            raise Exception("Datapath.attach: vdisk {} not found on array".format(vdisk_id))
        wwn = d["ScsiDeviceIdString"].lower()

        client.serve_vdisk(vdisk_id, host_id)
        _rescan_iscsi()
        dev = _wait_for_device(wwn, dbg)

        # Prefer /dev/mapper/<wwid> when multipath is active so I/O can go via
        # dm-multipath (failover or load-balanced per /etc/multipath.conf).
        # Falls back to the sd-by-id path if multipath isn't available or the
        # map doesn't materialize.
        if _multipath_active():
            _register_with_multipath(wwn, dbg)
            mpath = _wait_for_mpath(wwn, dbg)
            if mpath is not None:
                dev = mpath

        # Tune the scheduler last — multipath reconfigure emits udev events
        # that reset queue settings on the sd paths back to the kernel default
        # (cfq on XCP-ng 8.3). Also tunes the dm-multipath device itself, which
        # has its own queue separate from the underlying sd's.
        _tune_scheduler(wwn, dbg)

        return {
            "implementations": [
                ["XenDisk", {
                    "backend_type": "vbd",
                    "params": dev,
                    "extra": {},
                }],
                ["BlockDevice", {"path": dev}],
            ]
        }

    def activate(self, dbg, uri, domain):
        log.debug("{}: Datapath.activate uri={} domain={}".format(dbg, uri, domain))

    def activate_readonly(self, dbg, uri, domain):
        log.debug("{}: Datapath.activate_readonly uri={} domain={}".format(dbg, uri, domain))

    def deactivate(self, dbg, uri, domain):
        log.debug("{}: Datapath.deactivate uri={} domain={}".format(dbg, uri, domain))

    def detach(self, dbg, uri, domain):
        log.debug("{}: Datapath.detach uri={} domain={}".format(dbg, uri, domain))
        sr_uuid, vdisk_id = _parse_uri(uri)
        cfg = _read_stash(sr_uuid)
        host_id = cfg.get("host-id")
        if not host_id:
            return
        client = datacoreapi.DataCoreClient.from_sr_config(cfg)
        d = client.find_vdisk_by_id(vdisk_id)
        # Flush dm-multipath first so it releases its hold on the underlying
        # sd paths, then evict the sd paths so the kernel sees a clean
        # disconnect, then Unserve on the array.
        if d:
            wwn = d["ScsiDeviceIdString"].lower()
            _flush_multipath(wwn, dbg)
            _evict_scsi_paths_for_wwn(wwn, dbg)
        try:
            client.unserve_vdisk(vdisk_id, host_id)
        except Exception as e:
            log.error("{}: Datapath.detach Unserve failed: {}".format(dbg, e))


if __name__ == "__main__":
    log.log_call_argv()
    cmd = xapi.storage.api.v5.datapath.Datapath_commandline(Implementation())
    base = os.path.basename(sys.argv[0])
    if base == "Datapath.open":
        cmd.open()
    elif base == "Datapath.close":
        cmd.close()
    elif base == "Datapath.attach":
        cmd.attach()
    elif base == "Datapath.activate":
        cmd.activate()
    elif base == "Datapath.activate_readonly":
        cmd.activate_readonly()
    elif base == "Datapath.deactivate":
        cmd.deactivate()
    elif base == "Datapath.detach":
        cmd.detach()
    else:
        raise xapi.storage.api.v5.datapath.Unimplemented(base)
