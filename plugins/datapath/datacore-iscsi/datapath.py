#!/usr/bin/env python3

"""
Datapath for DataCore vDisks served via iSCSI.

URI shape:  datacore-iscsi://<sr-uuid>/<vdisk-id>

`attach`  calls Operation: Serve on the array, rescans iSCSI sessions, and waits
          for /dev/disk/by-id/scsi-3<ScsiDeviceIdString> to appear in Dom0.
`detach`  calls Operation: Unserve and rescans (so the SCSI sd* entry is removed).

`open/close/activate/deactivate` are essentially no-ops — the device is fully
usable as soon as `attach` returns.

Assumes `SR.attach` has already established iSCSI sessions to the DataCore
portals (DataCore's Serve refuses with ErrorCode 4 if there's no live session
between initiator and target — see datacore.md §2.1.2).
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


def _evict_scsi_paths_for_wwn(wwn, dbg):
    """Delete every /dev/sd* path whose WWN matches via /sys/block/<dev>/device/delete.

    Without this, after Unserve the orphaned sd* entries linger and confuse the
    next rescan — the device reappears but without the WWN attribute so udev
    never recreates the /dev/disk/by-id/scsi-3<wwn> symlink we wait for.
    """
    try:
        out = subprocess.run(["lsblk", "-rno", "NAME,WWN"],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             check=False)
        prefix = "0x" + wwn.lower()
        for line in out.stdout.decode().splitlines():
            parts = line.split()
            if len(parts) >= 2 and parts[1].lower() == prefix and parts[0].startswith("sd"):
                sysfile = "/sys/block/{}/device/delete".format(parts[0])
                if os.path.exists(sysfile):
                    log.debug("{}: evict /dev/{} ({})".format(dbg, parts[0], sysfile))
                    with open(sysfile, "w") as f:
                        f.write("1")
    except Exception as e:
        log.error("{}: scsi evict failed: {}".format(dbg, e))


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
        # Evict SCSI devices BEFORE Unserve so the kernel sees clean disconnect.
        if d:
            _evict_scsi_paths_for_wwn(d["ScsiDeviceIdString"].lower(), dbg)
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
