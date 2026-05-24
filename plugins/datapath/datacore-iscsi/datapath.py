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

import glob
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
DEVICE_POLL_TIMEOUT = 90  # seconds — udev can fall behind under bursty rescans
                          # (e.g. parallel clones + cloud-init customisation);
                          # 30 s was tight enough that the long tail tripped it.
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
    raise Exception("{}: Device {} did not appear within {}s".format(dbg, dev, DEVICE_POLL_TIMEOUT))


def _rescan_iscsi():
    subprocess.run(["iscsiadm", "-m", "session", "--rescan"],
                   check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # When iSCSI Serve maps a NEW LUN into a kernel sd slot that previously
    # held an Unserved LUN, the kernel keeps the sd entry and just updates
    # the wwid sysfs attribute — it does NOT fire an "add" or "change"
    # uevent. udev's persistent-storage rules therefore never re-evaluate,
    # and /dev/disk/by-id/scsi-3<wwn> stays pointed at the previous wwn
    # (the one from the Unserved LUN). _wait_for_device then times out
    # at 30 s on a symlink that never gets regenerated.
    #
    # Forcing a change-action trigger on all block devices makes udev
    # re-run its rules with the current wwid attribute and update the
    # by-id symlinks to match. settle drains the resulting queue.
    try:
        subprocess.run(["udevadm", "trigger", "--action=change",
                        "--subsystem-match=block"],
                       check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception as e:
        log.error("Failed to trigger udevadm: {}".format(e))
        # Continue anyway as this is not critical for operation
    try:
        subprocess.run(["udevadm", "settle", "--timeout=30"],
                       check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception as e:
        log.error("Failed to settle udev: {}".format(e))
        # Continue anyway as this is not critical for operation


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
                log.info("{}: evict /dev/{} (wwn={})".format(dbg, name, wwn))
                with open(sysfile, "w") as f:
                    f.write("1")
    except Exception as e:
        log.error("{}: scsi evict failed: {}".format(dbg, e))


def _read_sysfs(path):
    """Read a sysfs file, trimming trailing whitespace. Returns '' on any error."""
    try:
        with open(path) as f:
            return f.read().rstrip()
    except (FileNotFoundError, IOError):
        return ""


def _evict_orphan_datacore_sds(dbg):
    """Evict /dev/sd* entries that look like DataCore LUNs but have no wwid.

    The in-band detach path (`_evict_scsi_paths_for_wwn`) catches stale paths
    when Datapath.detach is called cleanly. But out-of-band sequences —
    notably VM hard-shutdown then restart, where XAPI doesn't reliably call
    Datapath.detach before the next Datapath.attach — leave sd entries
    behind. After Unserve+Serve+rescan the kernel keeps the existing sd
    entry but its INQUIRY data isn't refreshed, so `/sys/block/sdX/device/wwid`
    is empty and udev never creates `/dev/disk/by-id/scsi-3<wwn>`. The
    poll loop in Datapath.attach then times out at 30 s.

    Healthy active paths always have wwid populated (that's what udev keyed
    off to build the by-id link in the first place), so filtering on
    "DataCore Virtual Disk AND empty wwid" reliably hits only orphans. We
    do this BEFORE the next Serve+rescan so the rescan recreates fresh sd
    entries with the wwid attribute populated correctly.
    """
    try:
        names = sorted(os.listdir("/sys/block"))
    except OSError:
        return
    for name in names:
        if not name.startswith("sd"):
            continue
        dev_dir = "/sys/block/{}/device".format(name)
        if (_read_sysfs(dev_dir + "/vendor") != "DataCore"
                or _read_sysfs(dev_dir + "/model") != "Virtual Disk"):
            continue
        if _read_sysfs(dev_dir + "/wwid"):
            continue  # healthy active path; leave it alone
        log.info("{}: evict orphan DataCore sd /dev/{} (empty wwid)".format(dbg, name))
        delete = dev_dir + "/delete"
        try:
            with open(delete, "w") as f:
                f.write("1")
        except IOError as e:
            log.error("{}: failed to evict /dev/{}: {}".format(dbg, name, e))


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
    log.info("{}: multipath registered {}".format(dbg, mpath_wwid))


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
    """Tear down the dm-mapper state for `wwn` in the right order.

    The naive `multipath -f <wwid>` fails as soon as a partition-child
    mapping exists on top of the parent — and partition children appear
    automatically the moment dom0 attaches a LUN that carries a partition
    table (any cloned template, any cloud-init drive after `mkfs.vfat`).
    Without the chain below, the parent map lingers forever as a corpse
    with "Device or resource busy"; the next clone's iSCSI rescan piles up
    on top of accumulated corpses until udev can no longer keep its by-id
    symlinks fresh inside `DEVICE_POLL_TIMEOUT`, and Datapath.attach
    starts timing out for unrelated LUNs. We learnt this the hard way.

    The chain must be in this exact order:
      1. `kpartx -d <parent>` drops the auto-spawned partition children.
      2. Belt-and-braces `dmsetup remove --force` on any `*p*` strays
         that kpartx missed (e.g. when a partition was renamed by udev).
      3. `multipath -f <wwid>` now flushes the parent — the refcount has
         hit zero.
      4. `multipath -w <wwid>` purges the entry from /etc/multipath/wwids
         so multipathd doesn't resurrect the map on its next rescan.

    Every step is best-effort: non-zero exit is logged but doesn't raise,
    because partial cleanup (e.g. no partition children present) is the
    common case and should be silent."""
    mpath_wwid = "3" + wwn.lower()
    mpath_path = "/dev/mapper/" + mpath_wwid

    # 1. kpartx -d — drops partition mappings spawned from the parent.
    r = subprocess.run(["kpartx", "-d", mpath_path],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    if r.returncode != 0:
        log.debug("{}: kpartx -d {} exit={} (likely no partitions): {}".format(
            dbg, mpath_path, r.returncode, r.stderr.decode(errors="replace").strip()))

    # 2. Backstop: hunt for any partition-shaped names kpartx left behind.
    for stray in glob.glob(mpath_path + "p*") + glob.glob(mpath_path + "[0-9]*"):
        name = os.path.basename(stray)
        r = subprocess.run(["dmsetup", "remove", "--force", name],
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        log.debug("{}: dmsetup remove {} exit={}".format(dbg, name, r.returncode))

    # 3. Flush the parent map.
    r = subprocess.run(["multipath", "-f", mpath_wwid],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    log.debug("{}: multipath -f {} exit={}".format(dbg, mpath_wwid, r.returncode))

    # 3b. Backstop: `multipath -f` returns silently (exit 0) without removing
    # the map when its underlying paths are already gone — common after an
    # out-of-band sd eviction. Fall back to `dmsetup remove --force` directly
    # on the parent in that case so corpses don't accumulate.
    if os.path.exists(mpath_path):
        r = subprocess.run(["dmsetup", "remove", "--force", mpath_wwid],
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        log.debug("{}: dmsetup remove --force {} exit={}".format(
            dbg, mpath_wwid, r.returncode))

    # 4. Purge the wwid from /etc/multipath/wwids so multipathd doesn't
    # re-register the map on its next rescan. Without this step the map
    # comes back even after a successful `-f`.
    r = subprocess.run(["multipath", "-w", mpath_wwid],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    log.debug("{}: multipath -w {} exit={}".format(dbg, mpath_wwid, r.returncode))


def _flush_orphan_multipath_maps(dbg):
    """Sweep `/dev/mapper/3*` for DataCore multipath maps whose underlying
    SCSI paths are gone, and tear them down with the full flush chain.

    Sister to `_evict_orphan_datacore_sds`: same idea, the dm-mapper layer.
    An orphan parent map sticks around if the prior Datapath.detach skipped
    `_flush_multipath` (worker killed mid-flight, hard VM shutdown, plugin
    crash during teardown). Each lingering map costs udev work on the next
    attach: it has to skip over the corpses building its by-id table, and
    enough of them pile up to push the by-id symlink for a fresh LUN past
    DEVICE_POLL_TIMEOUT. Cleaning them at attach-start keeps the udev
    pipeline drainable.

    Identification: any `/dev/mapper/3<wwn>` whose `dmsetup info` reports
    a UUID starting with `mpath-` AND whose `multipath -ll <wwid>` shows
    no `active` paths is considered orphan. The template's real LUN
    (with active paths) is left alone.
    """
    try:
        candidates = [
            os.path.basename(p)
            for p in glob.glob("/dev/mapper/3*")
            if not _is_partition_child(os.path.basename(p))
        ]
    except OSError:
        return
    for name in candidates:
        r = subprocess.run(["dmsetup", "info", "-c", "--noheadings",
                            "-o", "uuid", name],
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        if not r.stdout.decode().strip().startswith("mpath-"):
            continue  # not a multipath map; leave alone
        # If there's at least one active path, it's the live LUN. Skip.
        ll = subprocess.run(["multipath", "-ll", name],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        if b"active ready running" in ll.stdout:
            continue
        log.info("{}: flush orphan multipath map {}".format(dbg, name))
        # strip the leading "3" to get the wwn the way _flush_multipath wants it
        _flush_multipath(name[1:], dbg)


def _is_partition_child(name):
    """`360030d90...p1` or `360030d90...1` style child of a parent map."""
    # parent wwid is exactly 33 chars (a leading "3" + 32 hex). Anything
    # longer is a partition.
    return len(name) > 33


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

        # Scrub orphan sd entries from prior out-of-band detach sequences
        # (e.g. VM hard-shutdown then restart) before re-Serving. Otherwise
        # the rescan keeps the wwid-less orphans and the by-id symlink never
        # materialises. Safe to run unconditionally — only touches DataCore
        # sd's whose wwid is empty.
        _evict_orphan_datacore_sds(dbg)
        # Also sweep stale dm-mapper parents whose paths are gone — same
        # idea, the multipath layer. Cheap if nothing's there; critical
        # after burst cycles or VM crashes that skipped clean detach.
        _flush_orphan_multipath_maps(dbg)

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
        # If there's an active outbound mirror for this VDI, this is the
        # cutover deactivate (VM I/O paused). Run the snap1-vs-snap2 delta
        # pass before XAPI proceeds with detach + Volume.destroy, otherwise
        # the destination ends up with stale data (snapshot point-in-time
        # rather than the live source at cutover).
        try:
            _sr_uuid, vdisk_id = _parse_uri(uri)
        except Exception:
            return
        try:
            import data as data_mod  # lazy: data.py imports datapath at top level
            data_mod.cutover_delta(vdisk_id, dbg)
        except Exception as e:
            log.error("{}: Datapath.deactivate cutover_delta: {}".format(dbg, e))

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
