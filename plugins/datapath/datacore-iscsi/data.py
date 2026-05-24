#!/usr/bin/env python3

"""
Data module for the DataCore datapath — outbound live VDI migration.

XAPI's `Storage_smapiv3_migrate.MIRROR.send_start` calls `Data.mirror` on
the source datapath with a destination NBD URL (the source-side nbdproxy
socket that bridges to the remote host's `/services/SM/nbdproxy/import/...`).
We return a `MirrorV1(key)` operation handle; XAPI polls `Data.stat` until
the operation reports complete, then triggers the cutover.

Mirror lifecycle:
  1. Snapshot the source vDisk via DataCore REST (differential, instant, COW).
  2. Serve the snapshot to dom0 and wait for its multipath/by-id device.
  3. Spawn `qemu-img convert -p -n -f raw -O raw /dev/<snap> <remote-url>`
     in a background, session-detached subprocess.
  4. Persist state on tmpfs so stat()/cancel()/destroy() can find the job.
  5. Return MirrorV1(key) immediately.

stat() reads the state, checks if the qemu-img subprocess is still alive,
and parses the latest progress percentage from qemu-img's `-p` output. Reports
complete once the subprocess has exited.

KNOWN LIMITATION (v1): writes the guest makes during the bulk copy land on
the live source vDisk, not on our snapshot. The destination is consistent
with the snapshot's point-in-time view, not with the live source at cutover.
Phase D will add a delta pass via the Datapath.deactivate hook.
"""

import json
import os
import re
import signal
import subprocess
import sys
import time
import uuid as uuidlib
from urllib.parse import urlparse, parse_qs, unquote

import xapi.storage.api.v5.datapath
from xapi.storage import log

# Sibling datapath.py — reuse its iSCSI/multipath plumbing for the snapshot.
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
import datapath as dp  # noqa: E402

# REST client lives with the volume plugin.
sys.path.insert(
    0,
    "/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.datacore",
)
import datacoreapi  # noqa: E402


MIRROR_DIR = "/run/datacore-sr"
QEMU_IMG = "/usr/lib64/xen/bin/qemu-img"  # XCP-ng's bundled QEMU, not on default PATH
QEMU_NBD = "/usr/lib64/xen/bin/qemu-nbd"
SNAP_NAME_PREFIX = "xcp-mirror"
DELTA_CHUNK_SIZE = 4 * 1024 * 1024  # 4 MiB block-checksum granularity for the cutover diff


def _state_path(key):
    return os.path.join(MIRROR_DIR, "mirror-{}.json".format(key))


def _progress_path(key):
    return os.path.join(MIRROR_DIR, "mirror-{}.progress".format(key))


def _read_state(key):
    try:
        with open(_state_path(key)) as f:
            return json.load(f)
    except FileNotFoundError:
        return None


def _write_state(state):
    path = _state_path(state["key"])
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, path)


def _alive(pid):
    if not pid:
        return False
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError, OSError):
        return False


_PROGRESS_RE = re.compile(r"\(\s*([\d.]+)\s*/\s*100\s*%\s*\)")


def _parse_progress(key):
    """qemu-img convert -p writes lines like '    (12.34/100%)' separated by
    \\r. Read the tail of the file and pick the last match. Returns a float
    in [0.0, 1.0], or 0.0 if nothing parseable yet."""
    try:
        with open(_progress_path(key)) as f:
            data = f.read()
    except FileNotFoundError:
        return 0.0
    matches = _PROGRESS_RE.findall(data)
    if not matches:
        return 0.0
    try:
        return min(1.0, float(matches[-1]) / 100.0)
    except ValueError:
        return 0.0


def _translate_nbd_url_for_qemu(url):
    """qemu-img 4.2.1 (shipped with XCP-ng 8.3) segfaults on the modern
    `nbd+unix:///<export>?socket=<sock>` URI form (and on `--image-opts
    driver=nbd,server.type=unix,server.path=...`). Translate to the legacy
    `nbd:unix:<sock>:exportname=<export>` form which works. TCP forms
    (`nbd://host:port/export`) pass through."""
    if not url.startswith("nbd+unix:"):
        return url
    p = urlparse(url)
    export = unquote(p.path.lstrip("/"))
    q = parse_qs(p.query)
    sock = (q.get("socket") or [None])[0]
    if not sock:
        raise Exception("nbd+unix URL has no socket parameter: {}".format(url))
    return "nbd:unix:{}:exportname={}".format(sock, export)


def _connect_nbd_destination(remote_url, dbg):
    """Bind the destination NBD endpoint to a free /dev/nbdN so the connection
    stays alive across the bulk-copy and the cutover-delta phases.

    XAPI's `export_nbd_proxy` on the source host accepts a single client,
    proxies until EOF, then closes the server socket. If we let `qemu-img
    convert` open and close its own connection we lose the proxy and can't
    reopen for the delta. Holding /dev/nbdN open keeps the proxy alive.

    Returns the device path. modprobe nbd if needed."""
    subprocess.run(["modprobe", "nbd"], check=False,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    qemu_url = _translate_nbd_url_for_qemu(remote_url)
    last_err = None
    for i in range(16):
        dev = "/dev/nbd{}".format(i)
        if not os.path.exists(dev):
            continue
        # Skip devices already in use — an unconnected nbd device has size 0.
        try:
            with open("/sys/block/nbd{}/size".format(i)) as f:
                sz = int(f.read().strip())
        except OSError:
            sz = -1
        if sz != 0:
            continue
        # qemu-nbd --connect forks a worker that holds the connection and
        # exits the parent. With stdout=PIPE Python keeps stdout open, which
        # blocks the parent's exit — we'd then loop and create another
        # --connect on the next device. Use DEVNULL for stdout, keep stderr
        # as PIPE for diagnostics. `--format=raw` is required: without it
        # qemu-nbd probes the URL, detects raw, and restricts writes to
        # block 0 as a safety measure; --format=raw lifts the restriction.
        r = subprocess.run(
            [QEMU_NBD, "--connect=" + dev, "--format=raw", qemu_url],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        if r.returncode != 0:
            last_err = r.stderr.decode(errors="replace").strip()
            continue
        # Confirm the binding actually took (the daemon may have raced).
        time.sleep(0.2)
        try:
            with open("/sys/block/nbd{}/size".format(i)) as f:
                new_sz = int(f.read().strip())
        except OSError:
            new_sz = 0
        if new_sz <= 0:
            last_err = "nbd device size is 0 after connect"
            continue
        log.debug("{}: NBD destination bound to {} (sectors={})".format(dbg, dev, new_sz))
        return dev
    raise Exception("could not bind destination NBD to any /dev/nbdN: {}".format(last_err))


def _disconnect_nbd(dev, dbg):
    """Tear down a qemu-nbd --connect binding. Best-effort."""
    if not dev:
        return
    r = subprocess.run([QEMU_NBD, "--disconnect", dev],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if r.returncode != 0:
        log.error("{}: qemu-nbd --disconnect {} failed: {}".format(
            dbg, dev, r.stderr.decode(errors="replace").strip()))


def _attach_snapshot(snap_vdisk_id, host_id, client, dbg):
    """Serve `snap_vdisk_id` to dom0's host_id and wait for the kernel device.
    Returns (wwn, device-path). Reuses datapath.py's iSCSI+multipath helpers."""
    snap = client.find_vdisk_by_id(snap_vdisk_id)
    if snap is None:
        raise Exception("snapshot vdisk {} not found on array".format(snap_vdisk_id))
    snap_wwn = snap["ScsiDeviceIdString"].lower()

    dp._evict_orphan_datacore_sds(dbg)
    client.serve_vdisk(snap_vdisk_id, host_id)
    dp._rescan_iscsi()
    dev = dp._wait_for_device(snap_wwn, dbg)

    if dp._multipath_active():
        dp._register_with_multipath(snap_wwn, dbg)
        mpath = dp._wait_for_mpath(snap_wwn, dbg)
        if mpath is not None:
            dev = mpath
    return snap_wwn, dev


def _detach_snapshot(snap_vdisk_id, snap_wwn, host_id, client, dbg):
    """Best-effort teardown: flush multipath, evict sd paths, unserve, delete.
    Errors are logged but do not raise — callers run this from cleanup paths
    where progress matters more than diagnostic precision."""
    if snap_wwn:
        try:
            dp._flush_multipath(snap_wwn, dbg)
        except Exception as e:
            log.error("{}: snap {} flush_multipath: {}".format(dbg, snap_wwn, e))
        try:
            dp._evict_scsi_paths_for_wwn(snap_wwn, dbg)
        except Exception as e:
            log.error("{}: snap {} evict sd: {}".format(dbg, snap_wwn, e))
    try:
        client.unserve_vdisk(snap_vdisk_id, host_id)
    except Exception as e:
        log.error("{}: snap {} unserve: {}".format(dbg, snap_vdisk_id, e))
    try:
        client.delete_vdisk(snap_vdisk_id)
    except Exception as e:
        log.error("{}: snap {} delete: {}".format(dbg, snap_vdisk_id, e))


def _find_active_mirror_for_vdisk(vdisk_id):
    """Scan tmpfs for a mirror state matching this vdisk_id. Returns the
    state dict or None. Called by Datapath.deactivate to decide whether
    to run the cutover delta."""
    try:
        files = os.listdir(MIRROR_DIR)
    except OSError:
        return None
    for f in files:
        if not (f.startswith("mirror-") and f.endswith(".json")):
            continue
        key = f[len("mirror-"):-len(".json")]
        state = _read_state(key)
        if state is None:
            continue
        if state.get("vdisk_id") == vdisk_id:
            return state
    return None


def cutover_delta(vdisk_id, dbg):
    """Cutover hook called from Datapath.deactivate on the source VBD.

    XAPI calls Datapath.deactivate with the VM paused (post-`pre_deactivate_hook`
    in the migration flow). At this moment:
      - The bulk copy is already on the destination (qemu-img convert ran during
        mirror() against snap1).
      - The VM may have written to the live source between snap1 and now.
      - The destination NBD connection (/dev/nbdN) is still bound (qemu-nbd is
        still running because we didn't disconnect it).

    Steps:
      1. Take a second differential snapshot (snap2) — captures current state.
      2. Attach snap2 to dom0.
      3. Block-checksum diff snap1 vs snap2 in 4 MiB chunks; for each differing
         chunk, seek+write snap2's bytes to /dev/nbdN at the matching offset.
      4. Disconnect /dev/nbdN (releases the proxy on XAPI's side).
      5. Detach + destroy snap1 and snap2; remove state files.

    No-op if no mirror state matches this vdisk_id (normal deactivate path)."""
    state = _find_active_mirror_for_vdisk(vdisk_id)
    if state is None:
        return
    key = state["key"]
    log.info("{}: cutover_delta start key={} vdisk={}".format(dbg, key, vdisk_id))

    cfg = dp._read_stash(state["sr_uuid"])
    client = datacoreapi.DataCoreClient.from_sr_config(cfg)

    snap2_id = None
    snap2_wwn = None
    try:
        snap2_name = "{}-delta-{}".format(SNAP_NAME_PREFIX, int(time.time()))
        snap2 = client.create_differential_snapshot(
            vdisk_id, snap2_name, cfg["first-pool"],
            description="xcp:mirror-delta={}".format(vdisk_id))
        snap2_id = snap2["Id"]
        client.wait_for_vdisk_online(snap2_id)
        snap2_wwn, snap2_dev = _attach_snapshot(snap2_id, state["host_id"], client, dbg)
        log.info("{}: cutover_delta: snap2={} dev={}".format(dbg, snap2_id, snap2_dev))

        snap1_dev = state["snap_dev"]
        nbd_dev = state["nbd_dev"]
        n_chunks = 0
        n_diff = 0
        bytes_shipped = 0
        with open(snap1_dev, "rb") as s1, open(snap2_dev, "rb") as s2, \
                open(nbd_dev, "r+b") as dst:
            offset = 0
            while True:
                a = s1.read(DELTA_CHUNK_SIZE)
                b = s2.read(DELTA_CHUNK_SIZE)
                if not a and not b:
                    break
                if len(a) != len(b):
                    # Shouldn't happen for same-size snapshots
                    log.error("{}: cutover_delta: chunk size mismatch at {}".format(dbg, offset))
                    break
                n_chunks += 1
                # Direct byte comparison is ~memcmp — orders of magnitude
                # faster than hashing both chunks. We're going to write the
                # differing bytes anyway, so a hash buys nothing.
                if a != b:
                    n_diff += 1
                    bytes_shipped += len(b)
                    dst.seek(offset)
                    dst.write(b)
                offset += len(a)
            dst.flush()
            os.fsync(dst.fileno())
        log.info("{}: cutover_delta: {} chunks compared, {} differed, {} bytes shipped".format(
            dbg, n_chunks, n_diff, bytes_shipped))
    except Exception as e:
        log.error("{}: cutover_delta failed: {}".format(dbg, e))
        # Continue with cleanup regardless — leaving the snapshot served
        # would block XAPI's post-cutover Volume.destroy of the source.
    finally:
        _disconnect_nbd(state.get("nbd_dev"), dbg)
        if snap2_id:
            _detach_snapshot(snap2_id, snap2_wwn, state["host_id"], client, dbg)
        _detach_snapshot(state["snap_id"], state.get("snap_wwn"),
                         state["host_id"], client, dbg)
        for p in (_state_path(key), _progress_path(key)):
            try:
                os.unlink(p)
            except FileNotFoundError:
                pass
        log.info("{}: cutover_delta done key={}".format(dbg, key))


class Implementation(xapi.storage.api.v5.datapath.Data_skeleton):

    def mirror(self, dbg, uri, domain, remote):
        log.debug("{}: Data.mirror uri={} domain={} remote={}".format(
            dbg, uri, domain, remote))
        sr_uuid, vdisk_id = dp._parse_uri(uri)
        cfg = dp._read_stash(sr_uuid)
        host_id = cfg.get("host-id")
        if not host_id:
            raise Exception("Data.mirror: SR config missing 'host-id'")
        first_pool = cfg.get("first-pool")
        if not first_pool:
            raise Exception("Data.mirror: SR config missing 'first-pool'")

        client = datacoreapi.DataCoreClient.from_sr_config(cfg)

        snap_name = "{}-{}-{}".format(SNAP_NAME_PREFIX, vdisk_id[:8], int(time.time()))
        log.info("{}: Data.mirror: snapshotting vdisk={} as {}".format(
            dbg, vdisk_id, snap_name))
        snap = client.create_differential_snapshot(
            vdisk_id, snap_name, first_pool,
            description="xcp:mirror-source={}".format(vdisk_id))
        snap_id = snap["Id"]
        log.info("{}: Data.mirror: snapshot Id={} — waiting for Online".format(dbg, snap_id))
        client.wait_for_vdisk_online(snap_id)

        snap_wwn, snap_dev = _attach_snapshot(snap_id, host_id, client, dbg)
        log.info("{}: Data.mirror: snapshot device {} (wwn={})".format(dbg, snap_dev, snap_wwn))

        # Hold the destination NBD open via qemu-nbd --connect so the proxy
        # stays alive after the bulk copy — needed for the cutover delta.
        nbd_dev = _connect_nbd_destination(remote, dbg)

        key = str(uuidlib.uuid4())
        progress = _progress_path(key)
        os.makedirs(MIRROR_DIR, exist_ok=True)
        prog_fh = open(progress, "wb")
        args = [
            QEMU_IMG, "convert", "-p", "-n",
            # qemu-img convert defaults to cache=unsafe (no fsync at end);
            # writes get buffered in qemu-nbd's memory and never reach the
            # destination's backing storage. writethrough flushes every
            # write so the destination is durable when we move on.
            "-t", "writethrough",
            "-f", "raw", "-O", "raw",
            snap_dev, nbd_dev,
        ]
        log.info("{}: Data.mirror: spawning {}".format(dbg, " ".join(args)))
        proc = subprocess.Popen(
            args, stdout=prog_fh, stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        prog_fh.close()

        state = {
            "key": key,
            "vdisk_id": vdisk_id,
            "sr_uuid": sr_uuid,
            "snap_id": snap_id,
            "snap_wwn": snap_wwn,
            "snap_dev": snap_dev,
            "host_id": host_id,
            "remote": remote,
            "nbd_dev": nbd_dev,
            "qemu_pid": proc.pid,
            "progress_path": progress,
            "started_at": time.time(),
        }
        _write_state(state)
        log.info("{}: Data.mirror -> MirrorV1 key={} pid={} nbd={}".format(
            dbg, key, proc.pid, nbd_dev))
        return ["MirrorV1", key]

    def stat(self, dbg, operation):
        log.debug("{}: Data.stat operation={}".format(dbg, operation))
        if not isinstance(operation, (list, tuple)) or len(operation) < 2:
            return {"failed": True, "complete": True, "progress": None}
        op_type, key = operation[0], operation[1]
        if op_type != "MirrorV1":
            return {"failed": True, "complete": True, "progress": None}
        state = _read_state(key)
        if state is None:
            # Either we never created this key, or destroy() already cleaned
            # it up. Treat unknown-but-after-the-fact polls as "completed
            # successfully" — XAPI sometimes stats after its own
            # finalize/cleanup, and returning failed=true breaks the
            # migration's post-cutover flow.
            return {"failed": False, "complete": True, "progress": 1.0}
        progress = _parse_progress(key)
        pid = state.get("qemu_pid")
        if _alive(pid):
            return {"failed": False, "complete": False, "progress": progress}
        # qemu-img has exited. We can't reap (different parent), so use the
        # progress file as the success/failure heuristic: 100% means qemu-img
        # finished the convert; anything less means it died early. Cleanup
        # (snapshot detach, NBD disconnect) happens in Datapath.deactivate's
        # cutover hook — not here, because we need snap1 alive for the diff.
        success = progress >= 0.999
        return {"failed": not success, "complete": True, "progress": progress}

    def cancel(self, dbg, operation):
        log.debug("{}: Data.cancel operation={}".format(dbg, operation))
        if not isinstance(operation, (list, tuple)) or len(operation) < 2:
            return
        op_type, key = operation[0], operation[1]
        if op_type != "MirrorV1":
            return
        state = _read_state(key)
        if state is None:
            return
        pid = state.get("qemu_pid")
        if _alive(pid):
            try:
                os.kill(pid, signal.SIGTERM)
            except OSError as e:
                log.error("{}: Data.cancel SIGTERM pid={}: {}".format(dbg, pid, e))

    def destroy(self, dbg, operation):
        log.debug("{}: Data.destroy operation={}".format(dbg, operation))
        if not isinstance(operation, (list, tuple)) or len(operation) < 2:
            return
        op_type, key = operation[0], operation[1]
        if op_type != "MirrorV1":
            return
        state = _read_state(key)
        if state is None:
            return
        pid = state.get("qemu_pid")
        if _alive(pid):
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass
        _disconnect_nbd(state.get("nbd_dev"), dbg)
        try:
            cfg = dp._read_stash(state["sr_uuid"])
            client = datacoreapi.DataCoreClient.from_sr_config(cfg)
            _detach_snapshot(
                state["snap_id"], state.get("snap_wwn"),
                state["host_id"], client, dbg)
        except Exception as e:
            log.error("{}: Data.destroy: snapshot cleanup failed: {}".format(dbg, e))
        for p in (_state_path(key), _progress_path(key)):
            try:
                os.unlink(p)
            except FileNotFoundError:
                pass

    def ls(self, dbg):
        log.debug("{}: Data.ls".format(dbg))
        try:
            files = os.listdir(MIRROR_DIR)
        except OSError:
            return []
        ops = []
        for f in files:
            if f.startswith("mirror-") and f.endswith(".json"):
                key = f[len("mirror-"):-len(".json")]
                ops.append(["MirrorV1", key])
        return ops


if __name__ == "__main__":
    log.log_call_argv()
    cmd = xapi.storage.api.v5.datapath.Data_commandline(Implementation())
    base = os.path.basename(sys.argv[0])
    if base == "Data.mirror":
        cmd.mirror()
    elif base == "Data.stat":
        cmd.stat()
    elif base == "Data.cancel":
        cmd.cancel()
    elif base == "Data.destroy":
        cmd.destroy()
    elif base == "Data.ls":
        cmd.ls()
    else:
        raise xapi.storage.api.v5.datapath.Unimplemented(base)
