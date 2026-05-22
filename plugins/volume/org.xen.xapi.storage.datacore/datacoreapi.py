"""
Thin REST client for DataCore SANsymphony.

Used by sr.py and volume.py. Stateless per process invocation — each plugin
script call instantiates a fresh client from the SR configuration.

API quirks (discovered in the spike — see datacore.md):
  - Every request requires a custom `ServerHost` header.
  - HTTP Basic Auth on every call works (no separate OpenSession needed).
  - `POST /virtualdisks` body shape differs from the GET response (Name vs Alias,
    Size as bytes vs {"Value": N}, FirstPool/SecondPool vs FirstHostId/etc).
  - POST responses are always wrapped in a single-element list.
  - DELETE is standard HTTP DELETE, not POST + {"Operation":"Delete"}.
  - Operation: Unserve returns HTTP 200 with an empty body.
"""

import contextlib
import fcntl
import json
import os
import subprocess
import sys
import time
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
import urllib3
from urllib3.util.retry import Retry

from xapi.storage import log

QEMU_IMG = "/usr/lib64/xen/bin/qemu-img"  # XCP-ng's bundled QEMU; not on default PATH


def _datapath_helpers():
    """Lazy cross-import of the datapath plugin's iSCSI/multipath helpers.

    These primitives (Serve + iSCSI rescan + by-id wait + multipath register)
    live in the datapath plugin because that's where they're naturally used.
    Volume.clone needs them too — to attach a snapshot + destination pair to
    dom0 and run qemu-img convert between them. Importing lazily keeps the
    volume plugin from depending on the datapath script being present at
    module-load time."""
    dp_path = "/usr/libexec/xapi-storage-script/datapath/datacore-iscsi"
    if dp_path not in sys.path:
        sys.path.insert(0, dp_path)
    import datapath as dp  # noqa: E402
    return dp

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

API_PATH = "/RestService/rest.svc/1.0"
VOLUME_TYPE_MIRRORED = 2
SNAPSHOT_TYPE_DIFFERENTIAL = 1  # instant, COW on the array, source-dependent
PORT_TYPE_ISCSI = 3  # iSCSI initiator/target port in /ports (4 was seen for FC/other)

# Observed empirically (probe, 2026-05-20): a freshly created mirrored vDisk
# reports DiskStatus=1 ("not up-to-date" — mirror legs still syncing) and
# transitions to DiskStatus=0 ("Online") once the array considers it ready.
# `POST /snapshots` against a DiskStatus=1 source fails with
# "Cannot perform snapshot operations on virtual disk ... while it is not
# up-to-date", so Volume.create must block until the steady state.
DISK_STATUS_ONLINE = 0
VDISK_READY_TIMEOUT = 60.0
VDISK_READY_POLL_INTERVAL = 0.5

# (connect, read) seconds. Connect is short — TCP handshake to a healthy
# DataCore server completes in milliseconds. Read is generous because some
# REST operations (create_mirrored_vdisk, serve_vdisk) take a few seconds.
HTTP_TIMEOUT = (5, 60)

# POST is deliberately excluded from retries. urllib3.Retry doesn't know
# whether a 5xx / connection-reset POST was processed before failing, and
# retrying would risk duplicate vDisk creation / duplicate Serve mappings.
# GET / PUT / DELETE are all idempotent in our API so retrying is safe.
_RETRY_METHODS = frozenset(["GET", "PUT", "DELETE"])
_RETRY_STATUS = (500, 502, 503, 504)


class DataCoreError(Exception):
    pass


def _truthy(v):
    return str(v).lower() in ("true", "1", "yes", "on")


# tmpfs directory shared with sr.py's STASH_DIR. Same root-only access model
# as XAPI's secret store (`session.xenapi.secret.get_value` requires root in
# dom0), so caching here doesn't lower the security bar.
PASSWORD_CACHE_DIR = "/run/datacore-sr"


def _password_cache_path(sr_uuid):
    return os.path.join(PASSWORD_CACHE_DIR, "{}.pw".format(sr_uuid))


def _read_cached_password(sr_uuid):
    try:
        with open(_password_cache_path(sr_uuid)) as f:
            return f.read()
    except FileNotFoundError:
        return None


def _write_cached_password(sr_uuid, password):
    """Atomic write with 0600 perms. Best-effort: failures don't break the call."""
    try:
        os.makedirs(PASSWORD_CACHE_DIR, exist_ok=True)
        dst = _password_cache_path(sr_uuid)
        tmp = dst + ".tmp"
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(password)
        os.replace(tmp, dst)
    except OSError:
        # Caching is purely an optimisation; never block a working call.
        pass


def clear_password_cache(sr_uuid):
    """Remove the cached password for an SR. Called by sr.py on SR.attach
    (to invalidate before a credential refresh) and SR.detach (cleanup)."""
    try:
        os.unlink(_password_cache_path(sr_uuid))
    except FileNotFoundError:
        pass


def _resolve_password(cfg):
    """
    XAPI silently rewrites the device-config key `password` to `password_secret`
    holding a Secret UUID. The plugin must look up the plaintext via XAPI.
    Plain `password` is supported as a passthrough for direct (non-XAPI) testing.

    Resolved plaintexts are cached on tmpfs (PASSWORD_CACHE_DIR) keyed by
    sr-uuid so that the dozens of plugin sub-process invocations XAPI fires
    per SR operation don't each open + log out an XAPI session. The cache
    is populated lazily and invalidated by sr.py at SR.attach/SR.detach.
    """
    if "password" in cfg:
        return cfg["password"]
    secret_uuid = cfg.get("password_secret")
    if not secret_uuid:
        raise DataCoreError("No 'password' or 'password_secret' in SR configuration")
    sr_uuid = cfg.get("sr-uuid")
    if sr_uuid:
        cached = _read_cached_password(sr_uuid)
        if cached is not None:
            return cached
    import XenAPI
    session = XenAPI.xapi_local()
    session.xenapi.login_with_password("", "", "1.0", "datacore-plugin")
    try:
        ref = session.xenapi.secret.get_by_uuid(secret_uuid)
        pw = session.xenapi.secret.get_value(ref)
    finally:
        session.xenapi.session.logout()
    if sr_uuid:
        _write_cached_password(sr_uuid, pw)
    return pw


def _build_retry():
    """urllib3 Retry policy. total=4 with backoff_factor=0.5 gives sleeps of
    roughly 0s, 1s, 2s, 4s between attempts — bounded at ~7s of waiting."""
    return Retry(
        total=4,
        connect=4,
        read=2,
        status=2,
        backoff_factor=0.5,
        status_forcelist=_RETRY_STATUS,
        allowed_methods=_RETRY_METHODS,
        raise_on_status=False,
    )


class DataCoreClient:
    def __init__(self, endpoint, username, password, verify=False):
        self.base = endpoint.rstrip("/") + API_PATH
        self.session = requests.Session()
        self.session.verify = verify
        self.session.auth = (username, password)
        self.session.headers["ServerHost"] = urlparse(endpoint).hostname or "localhost"
        adapter = HTTPAdapter(max_retries=_build_retry())
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    @classmethod
    def from_sr_config(cls, cfg):
        return cls(
            cfg["rest-endpoint"],
            cfg["username"],
            _resolve_password(cfg),
            verify=_truthy(cfg.get("tls-verify", "false")),
        )

    def _check(self, r):
        if r.status_code >= 400:
            try:
                msg = r.json().get("Message", r.text)
            except ValueError:
                msg = r.text
            raise DataCoreError("HTTP {}: {}".format(r.status_code, msg))
        if not r.content:
            return None
        try:
            return r.json()
        except ValueError:
            return r.text

    def get(self, path):
        return self._check(self.session.get(self.base + path, timeout=HTTP_TIMEOUT))

    def post(self, path, body=None):
        return self._check(self.session.post(self.base + path, json=body or {},
                                             timeout=HTTP_TIMEOUT))

    def put(self, path, body):
        return self._check(self.session.put(self.base + path, json=body,
                                            timeout=HTTP_TIMEOUT))

    def delete(self, path):
        return self._check(self.session.delete(self.base + path, timeout=HTTP_TIMEOUT))

    def list_pools(self):
        return self.get("/pools") or []

    def pool_chunk_size_bytes(self, pool_id):
        """Return the pool's allocation unit (ChunkSize) in bytes.

        DataCore refuses to create vDisks below this size with HTTP 400
        "the disk size provided is less than the storage allocation unit
        size", which we hit on VDIs like XO's 10 MiB CloudConfigDrive.
        Returns 0 if the pool isn't found — caller decides how to handle
        the missing-pool case.
        """
        try:
            for p in self.list_pools():
                if p.get("Id") == pool_id:
                    return int((p.get("ChunkSize") or {}).get("Value", 0))
        except Exception as e:
            raise DataCoreError("Failed to get pool chunk size for {}: {}".format(pool_id, e))
        return 0

    def align_size_to_chunk(self, size, chunk):
        """Round `size` up to the next multiple of `chunk`. Pure helper."""
        if chunk <= 0:
            return size
        return ((int(size) + chunk - 1) // chunk) * chunk

    def list_pool_members(self):
        """Flat list of physical-disk members across all pools.

        Each item carries `DiskPoolId` and `Size.Value` (bytes). The bare
        `/pools` listing has no capacity field, and `/pools/{id}` 404s,
        so summing members is the documented path to pool capacity.
        """
        return self.get("/poolmembers") or []

    def pool_capacity_bytes(self, pool_id):
        """Sum the Size of all members of a pool. Returns 0 on lookup failure
        or empty pool (caller decides how to interpret 0 — never raises)."""
        total = 0
        try:
            members = self.list_pool_members()
        except DataCoreError:
            return 0
        for m in members:
            if m.get("DiskPoolId") != pool_id:
                continue
            sz = m.get("Size") or {}
            try:
                total += int(sz.get("Value", 0))
            except (TypeError, ValueError, AttributeError):
                pass
        return total

    def list_virtualdisks(self):
        return self.get("/virtualdisks") or []

    def sr_allocated_bytes(self, sr_uuid):
        """Sum the logical Size of every vDisk owned by this SR (Alias-prefix
        match). For mirrored vDisks, Size is the logical (single-leg-equivalent)
        bytes, which is what we want for "allocated against mirrored capacity".

        Caveat: only counts our own SR's vDisks. If the same DataCore pools
        are shared with other XCP-ng SRs or other tenants, their allocations
        are invisible to us and free_space ends up optimistic. There is no
        documented per-pool "allocated bytes" REST endpoint; /performance/{pool}
        returns a capacity-shaped record but its fields read 0 on lab kit
        without DataCore's performance collection enabled.
        """
        prefix = vdisk_prefix(sr_uuid)
        total = 0
        for d in self.list_virtualdisks():
            if not d.get("Alias", "").startswith(prefix):
                continue
            sz = d.get("Size") or {}
            try:
                total += int(sz.get("Value", 0))
            except (TypeError, ValueError, AttributeError):
                pass
        return total

    def find_vdisk_by_id(self, vdisk_id):
        for d in self.list_virtualdisks():
            if d.get("Id") == vdisk_id:
                return d
        return None

    def resize_vdisk(self, vdisk_id, new_size, snap_settle_timeout=15.0):
        """Online resize a vDisk via PUT /virtualdisks/{id} with the Size field.

        DataCore does NOT use `POST {"Operation": "Resize"}` for this — that
        operation is "is not valid for this request". Resize is just a property
        change like Name/Description, done via PUT. The array accepts shrink
        as well as grow, but Volume.resize refuses shrink (data-loss risk).

        Snapshot-cleanup is async on the array side. After DELETE on a
        snapshot returns and `/snapshots` is empty, PUT on the parent's
        Size still returns 400 "Virtual disk ... cannot be resized
        because it has snapshots attached" for a few seconds. We poll
        the same call with backoff up to snap_settle_timeout so callers
        don't have to think about the race.
        """
        deadline = time.monotonic() + snap_settle_timeout
        while True:
            try:
                return self.put("/virtualdisks/{}".format(vdisk_id),
                                {"Size": int(new_size)})
            except DataCoreError as e:
                if "snapshots attached" not in str(e).lower():
                    raise
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.5)

    def serve_vdisk(self, vdisk_id, host_id):
        """Serve the vDisk to `host_id`. Idempotent — re-Serving a vDisk
        that is already mapped to the same host returns HTTP 400
        "A path with the chosen initiator and target ports already exists",
        which we treat as success. This matters after VM-shutdown-then-
        restart cycles where Datapath.detach was not called: the LUN is
        still mapped on the array, and the next Datapath.attach must not
        fail just because the array agrees the mapping already exists."""
        try:
            return self.post("/virtualdisks/{}".format(vdisk_id),
                             {"Operation": "Serve", "Host": host_id})
        except DataCoreError as e:
            if "already exists" in str(e).lower():
                return None
            raise

    def unserve_vdisk(self, vdisk_id, host_id):
        return self.post("/virtualdisks/{}".format(vdisk_id),
                         {"Operation": "Unserve", "Host": host_id})

    def list_iscsi_target_portals_by_server(self):
        """Returns {server_id: [portal_ip, ...]} for every iSCSI target portal
        configured on each DataCore server.

        Identifies target ports (vs the local Microsoft iSCSI initiator on
        each DataCore server) by checking that PortName is an IQN AND that
        IScsiPortStateInfo.PortalsState carries one or more Address entries.
        Initiator ports don't expose a portal (they connect outward, not
        listen), so the PortalsState presence is the reliable discriminator
        across DataCore versions.
        """
        out = {}
        for p in (self.get("/ports") or []):
            if not (p.get("PortName") or "").startswith("iqn."):
                continue
            info = p.get("IScsiPortStateInfo") or {}
            portals = info.get("PortalsState") or []
            server_id = p.get("HostId")
            if not server_id:
                continue
            for portal in portals:
                addr_obj = portal.get("Address") or {}
                addr = addr_obj.get("Address")
                if addr:
                    out.setdefault(server_id, []).append(addr)
        return out

    def find_host_id_by_iqn(self, iqn):
        """Look up which DataCore host owns a given initiator IQN.

        DataCore exposes a flat `/ports` listing where each port carries
        HostId + PortName. We scan it for an iSCSI port whose PortName matches.

        Returns the HostId string, or None if the IQN isn't registered yet.
        First-time setup (operator added the host in the GUI but hasn't
        registered the initiator IQN) falls into the None branch — the
        caller must surface a clear bootstrap instruction at that point.
        """
        ports = self.get("/ports") or []
        for p in ports:
            if (p.get("PortType") == PORT_TYPE_ISCSI
                    and p.get("PortName") == iqn):
                return p.get("HostId")
        return None

    def register_port_idempotent(self, host_id, iqn):
        """Register an initiator IQN against the DataCore host object.
        Returns the port object (existing or new). Idempotent on already-registered."""
        try:
            return self.post("/hosts/{}".format(host_id),
                             {"Operation": "RegisterPort", "Port": iqn, "PortType": "iSCSI"})
        except DataCoreError as e:
            if "already" in str(e).lower() or "exists" in str(e).lower():
                return None
            raise

    def create_mirrored_vdisk(self, name, first_pool, second_pool, size, description=""):
        resp = self.post("/virtualdisks", {
            "Name": name,
            "Description": description or "",
            "FirstPool": first_pool,
            "SecondPool": second_pool,
            "Size": int(size),
            "Type": VOLUME_TYPE_MIRRORED,
        })
        if isinstance(resp, list) and resp:
            return resp[0]
        raise DataCoreError("Unexpected create response: {!r}".format(resp))

    def wait_for_vdisk_online(self, vdisk_id, timeout=VDISK_READY_TIMEOUT,
                              interval=VDISK_READY_POLL_INTERVAL):
        """Poll the vDisk until DiskStatus reports Online, or raise on timeout.

        Newly-created mirrored vDisks start at DiskStatus=1 (legs syncing) and
        transition to DiskStatus=0 within ~2s for an empty 512 MiB vDisk on
        the lab rig. Snapshot/clone refuses to operate on a not-yet-Online
        source, so callers that may immediately follow create with another
        operation must block here.

        If DiskStatus is missing from the response (older DataCore versions
        or future schema changes), assume ready rather than spinning forever.
        """
        deadline = time.monotonic() + timeout
        last_status = None
        while True:
            d = self.find_vdisk_by_id(vdisk_id)
            if d is None:
                raise DataCoreError(
                    "wait_for_vdisk_online: vDisk {} disappeared".format(vdisk_id))
            status = d.get("DiskStatus")
            if status is None:
                return d  # field absent: trust the array, don't spin
            if status == DISK_STATUS_ONLINE:
                return d
            last_status = status
            if time.monotonic() >= deadline:
                raise DataCoreError(
                    "vDisk {} did not become ready within {}s "
                    "(DiskStatus stayed at {}); manual cleanup may be needed".format(
                        vdisk_id, timeout, last_status))
            time.sleep(interval)

    def update_description(self, vdisk_id, description):
        self.put("/virtualdisks/{}".format(vdisk_id), {"Description": description})

    def delete_vdisk(self, vdisk_id):
        self.delete("/virtualdisks/{}".format(vdisk_id))

    def create_differential_snapshot(self, source_vdisk_id, name, destination_pool,
                                     description=""):
        """POST /snapshots with Type=1 (Differential).

        Per DataCore docs: instant (State=2 immediately), COW on the array,
        source-dependent. The destination vDisk is single-leg (Type=0) on the
        chosen pool — DataCore snapshots cannot be mirrored ("The snapshot can
        only exist on one server.", Snapshot Operations docs).

        Description does NOT propagate from source; the caller must PUT
        metadata onto the new vDisk afterward.

        Returns the new vDisk record (looked up via the returned snapshot's
        DestinationLogicalDisk / by Name fallback).
        """
        resp = self.post("/snapshots", {
            "VirtualDisk":     source_vdisk_id,
            "Name":            name,
            "Type":            SNAPSHOT_TYPE_DIFFERENTIAL,
            "DestinationPool": destination_pool,
        })
        snap = resp[0] if isinstance(resp, list) and resp else resp
        if not isinstance(snap, dict):
            raise DataCoreError("Unexpected /snapshots response: {!r}".format(resp))

        # Resolve the destination vDisk. The snapshot lineage object's exact
        # shape isn't documented; try the commonly-seen fields first, then
        # fall back to a Name lookup against /virtualdisks.
        dest_id = (snap.get("DestinationVirtualDisk")
                   or snap.get("DestinationVirtualDiskId")
                   or snap.get("Destination"))
        if dest_id:
            d = self.find_vdisk_by_id(dest_id)
            if d is not None:
                return d
        for d in self.list_virtualdisks():
            if d.get("Alias") == name:
                return d
        raise DataCoreError(
            "Snapshot created but destination vDisk not found (name={!r})".format(name))


# -------- VDI <-> DataCore vDisk translation helpers --------

VDI_NAME_PREFIX = "xcp"


def vdisk_prefix(sr_uuid):
    """vDisk Alias prefix used to filter our vDisks within a multi-tenant array."""
    return "{}-{}-".format(VDI_NAME_PREFIX, sr_uuid[:8])


def parse_metadata(description):
    """Description is a JSON blob; missing/invalid -> empty dict."""
    if not description:
        return {}
    try:
        return json.loads(description)
    except (ValueError, TypeError):
        return {}


def encode_metadata(vdi_uuid, sr_uuid, vdi_name, sharable, read_write,
                    description="", custom=None, is_snapshot=False,
                    parent_vdi_uuid=None, max_len=1024):
    blob = {
        "xcp-ng:vdi-uuid": vdi_uuid,
        "xcp-ng:sr-uuid": sr_uuid,
        "xcp-ng:vdi-name": (vdi_name or "")[:200],
        "xcp-ng:vdi-description": (description or "")[:200],
        "xcp-ng:sharable": bool(sharable),
        "xcp-ng:read-write": bool(read_write),
    }
    if is_snapshot:
        blob["xcp-ng:is-snapshot"] = True
    if parent_vdi_uuid:
        blob["xcp-ng:parent-vdi-uuid"] = parent_vdi_uuid
    for k, v in (custom or {}).items():
        blob["xcp-ng:custom:{}".format(k)] = v
    out = json.dumps(blob, separators=(",", ":"))
    if len(out) > max_len:
        raise DataCoreError("Metadata blob exceeds {} chars".format(max_len))
    return out


def _server_id_from_pool(pool_id):
    """Pool ID format is `{ServerId}:{pool-guid}` — extract the leading server id."""
    return pool_id.split(":", 1)[0] if pool_id else ""


def _vdisk_preferred_server_id(d):
    """Best-effort extraction of the source vDisk's preferred server.

    The exact JSON shape isn't documented; this checks the commonly-seen
    field names. Returns "" if it can't be determined.
    """
    for key in ("PreferredServer", "PreferredServerId", "FirstHostId"):
        v = d.get(key)
        if isinstance(v, dict):
            v = v.get("Id") or v.get("Caption")
        if isinstance(v, str) and v:
            return v
    return ""


def pick_snapshot_pool(cfg, source_vdisk):
    """Pick the destination pool for a snapshot of `source_vdisk`.

    DataCore's snapshot constraint is asymmetric depending on the source's
    mirror status:

      * **Type=2 mirrored vDisks** (templates, normal SMAPIv3-created volumes):
        the snapshot's single LD can live on either server, since both servers
        have direct visibility to the source. Best practice (per Snapshot
        Operations docs) is to put it on the *non-preferred* side so snapshot
        capacity balances instead of piling on the primary.

      * **Type=0 single-server vDisks** (our fast clones — destinations of
        a differential snapshot; CD/ISO LUNs; any other single-LD vDisk):
        the source physically exists on only one server. DataCore rejects
        snapshot create with `HTTP 400: Disk ... does not belong to server X`
        if the destination pool is on the *other* server. The snapshot MUST
        land on the same side as the source.

    Failing to distinguish these blew up `VM.snapshot` on a VM whose boot
    disk was a clone (Type=0 on ServerB): the function returned ServerA's
    pool per the "non-preferred-side" rule and DataCore refused. Fixed by
    checking `Type` first.
    """
    first = cfg["first-pool"]
    second = cfg["second-pool"]
    preferred = _vdisk_preferred_server_id(source_vdisk)

    if source_vdisk.get("Type") == 0:
        # Single-server source — snapshot must co-locate with the (only) LD.
        if preferred and preferred == _server_id_from_pool(first):
            return first
        if preferred and preferred == _server_id_from_pool(second):
            return second
        # If we can't tell which server holds the source, prefer first.
        return first

    # Mirrored source — balance load by placing snapshot on the non-preferred side.
    if preferred and preferred == _server_id_from_pool(first):
        return second
    if preferred and preferred == _server_id_from_pool(second):
        return first
    return second


def vdisk_to_vdi_info(d, sr_uuid):
    """Convert a DataCore vDisk record into the dict shape SMAPIv3 expects.

    Authoritative VDI UUID comes from the JSON metadata in Description (DataCore
    truncates Alias at 48 chars, so the full UUID does not fit in the Alias when
    combined with our prefix). Falls back to DataCore's Id if no metadata.
    """
    meta = parse_metadata(d.get("Description"))
    alias = d.get("Alias", "")
    vdi_uuid = meta.get("xcp-ng:vdi-uuid") or d["Id"]
    custom = {k[len("xcp-ng:custom:"):]: v
              for k, v in meta.items() if k.startswith("xcp-ng:custom:")}
    return {
        "key": vdi_uuid,
        "uuid": vdi_uuid,
        "name": meta.get("xcp-ng:vdi-name", alias),
        "description": meta.get("xcp-ng:vdi-description", ""),
        "read_write": bool(meta.get("xcp-ng:read-write", True)),
        "virtual_size": d["Size"]["Value"],
        "physical_utilisation": d["Size"]["Value"],
        "uri": ["datacore-iscsi://{}/{}".format(sr_uuid, d["Id"])],
        "sharable": bool(meta.get("xcp-ng:sharable", False)),
        "keys": custom,
    }


def find_vdisk_by_vdi_uuid(client, sr_uuid, vdi_uuid):
    """Locate a DataCore vDisk owned by this SR whose metadata records the given VDI UUID."""
    prefix = vdisk_prefix(sr_uuid)
    for d in client.list_virtualdisks():
        if not d.get("Alias", "").startswith(prefix):
            continue
        meta = parse_metadata(d.get("Description"))
        if meta.get("xcp-ng:vdi-uuid") == vdi_uuid:
            return d
    return None


def find_snapshot_record_by_dest_vdisk(client, vdisk):
    """Find the snapshot RECORD whose destination LD lives on `vdisk`.
    Returns the snapshot dict (with .Id) or None.

    Differential clones look like normal Type=0 vDisks but have a parent
    snapshot record on the array. Volume.destroy on such a clone has to
    DELETE the snapshot record (which cascades to delete the dest vDisk),
    not just the vDisk directly — DataCore refuses the vDisk delete when
    a snapshot record points at one of its logical disks."""
    # The vDisk's LogicalDisks have Ids; the snapshot record's
    # DestinationLogicalDiskId points to one of them.
    ld_ids = set()
    for ld in client.get("/logicaldisks") or []:
        if ld.get("VirtualDiskId") == vdisk["Id"]:
            ld_ids.add(ld["Id"])
    if not ld_ids:
        return None
    for sn in client.get("/snapshots") or []:
        if sn.get("DestinationLogicalDiskId") in ld_ids:
            return sn
    return None


def delete_snapshot_record(client, snap_record_id):
    """DELETE /snapshots/<id>. Cascades to delete the snapshot's destination
    vDisk. The Id format contains `{...}` which has to be URL-encoded for
    the REST endpoint to match the route."""
    enc = snap_record_id.replace("{", "%7B").replace("}", "%7D")
    return client.delete("/snapshots/" + enc)


def promote_to_independent(client, cfg, sr, src_vdisk, new_size, dbg):
    """Convert a differential-snapshot clone into an independent mirrored
    vDisk at `new_size`. Used by Volume.resize when DataCore refuses to
    grow a clone because of its snapshot parent.

    Steps:
      1. Create a new independent mirrored vDisk at `new_size`, carrying
         the same `xcp-ng:vdi-uuid` metadata as the source clone (so
         XAPI's next `find_vdisk_by_vdi_uuid` resolves to the new vDisk).
      2. Serve both to dom0, run `qemu-img convert` between them.
      3. Find the snapshot RECORD that owns the source clone's dest LD,
         delete it — this cascades to delete the old clone's vDisk.

    XAPI's per-VDI lock holds across this call, so concurrent ops on the
    same vDisk queue naturally. Other vDisks in the same SR may proceed
    in parallel.
    """
    host_id = cfg.get("host-id")
    if not host_id:
        raise DataCoreError("promote_to_independent: SR config missing host-id")

    src_id = src_vdisk["Id"]
    src_wwn = src_vdisk["ScsiDeviceIdString"].lower()
    src_meta = parse_metadata(src_vdisk.get("Description"))
    vdi_uuid = src_meta.get("xcp-ng:vdi-uuid")
    if not vdi_uuid:
        raise DataCoreError(
            "promote_to_independent: source vdisk {} has no xcp-ng:vdi-uuid metadata".format(src_id))

    # 1. Provision the new independent mirrored vDisk at the requested size.
    # Use a temp alias during promote; switch to the canonical alias after
    # the old vDisk is gone (so DataCore doesn't reject a duplicate name).
    tmp_alias = "{}promote-{}".format(vdisk_prefix(sr), vdi_uuid[:8])
    canonical_alias = "{}{}".format(vdisk_prefix(sr), vdi_uuid)
    new_meta = encode_metadata(
        vdi_uuid=vdi_uuid,
        sr_uuid=sr,
        vdi_name=src_meta.get("xcp-ng:vdi-name", ""),
        description=src_meta.get("xcp-ng:vdi-description", ""),
        sharable=bool(src_meta.get("xcp-ng:sharable", False)),
        read_write=True,
        is_snapshot=False,
        parent_vdi_uuid=src_meta.get("xcp-ng:parent-vdi-uuid"),
    )
    log.info("{}: promote: creating independent vDisk {} at {} bytes".format(
        dbg, tmp_alias, new_size))
    new_d = client.create_mirrored_vdisk(
        tmp_alias, cfg["first-pool"], cfg["second-pool"],
        new_size, description=new_meta)
    new_d = client.wait_for_vdisk_online(new_d["Id"])
    new_wwn = new_d["ScsiDeviceIdString"].lower()

    try:
        # 2. Copy data via dom0.
        copy_vdisk_data(client, host_id, src_id, src_wwn,
                        new_d["Id"], new_wwn, dbg)
        unserve_and_evict(client, host_id, src_id, src_wwn, dbg)
        unserve_and_evict(client, host_id, new_d["Id"], new_wwn, dbg)
    except Exception:
        # Cleanup the half-baked new vDisk on failure; the source clone
        # stays intact (XAPI's resize call propagates the error to the user).
        try:
            unserve_and_evict(client, host_id, new_d["Id"], new_wwn, dbg)
            client.delete_vdisk(new_d["Id"])
        except Exception as ee:
            log.error("{}: promote cleanup-after-failure: {}".format(dbg, ee))
        raise

    # 3. Delete the old clone via its snapshot record (cascades to dest vDisk).
    snap_record = find_snapshot_record_by_dest_vdisk(client, src_vdisk)
    if snap_record:
        log.info("{}: promote: deleting snapshot record {}".format(dbg, snap_record["Id"]))
        try:
            delete_snapshot_record(client, snap_record["Id"])
        except Exception as e:
            log.error("{}: promote: snapshot record delete: {}".format(dbg, e))
            # Fall through and try the direct vDisk delete as a backstop.
    try:
        client.delete_vdisk(src_id)
    except DataCoreError as e:
        # Expected to fail if the snapshot record already cascaded the delete.
        if "not found" not in str(e).lower() and "does not exist" not in str(e).lower():
            log.error("{}: promote: src delete (best-effort): {}".format(dbg, e))

    # 4. Rename the new vDisk to the canonical alias now that the old is gone.
    try:
        client.put("/virtualdisks/" + new_d["Id"], {"Caption": canonical_alias})
    except Exception as e:
        log.error("{}: promote: rename to canonical alias failed: {}".format(dbg, e))
    log.info("{}: promote: complete (vdi_uuid={} new vDisk={})".format(
        dbg, vdi_uuid, new_d["Id"]))
    return new_d


_DOM0_ATTACH_LOCK_PATH = "/run/datacore-sr/.dom0-attach.lock"


@contextlib.contextmanager
def _dom0_attach_lock():
    """Host-wide mutex around the dom0 iSCSI/udev/multipath machinery.

    Concurrent clone workers each Serve their own snapshot+destination pair
    and then call iscsiadm rescan + udevadm settle + multipath register.
    Those operations share global state in /sys/block, /dev/disk/by-id, and
    /dev/mapper — running them simultaneously from multiple processes races:
    udev queues events but `wait_for_device`'s 30 s by-id symlink poll
    times out before udev catches up.

    Holding this flock around the attach/detach critical sections
    serialises only the dom0-side setup (~5 s per worker). The actual data
    copy via `qemu-img convert` runs unlocked, in parallel across workers."""
    os.makedirs(COPY_DIR, exist_ok=True)
    f = open(_DOM0_ATTACH_LOCK_PATH, "w")
    try:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        f.close()


def _attach_vdisk_to_dom0(client, host_id, vdisk_id, wwn, dbg):
    """Serve `vdisk_id` to dom0 and wait for the kernel device. Returns the
    device path (multipath if active, else sd-by-id). Holds the dom0 lock
    around the iSCSI/udev/multipath dance so concurrent workers don't race."""
    dp = _datapath_helpers()
    with _dom0_attach_lock():
        dp._evict_orphan_datacore_sds(dbg)
        dp._flush_orphan_multipath_maps(dbg)
        client.serve_vdisk(vdisk_id, host_id)
        dp._rescan_iscsi()
        dev = dp._wait_for_device(wwn, dbg)
        if dp._multipath_active():
            dp._register_with_multipath(wwn, dbg)
            mpath = dp._wait_for_mpath(wwn, dbg)
            if mpath is not None:
                dev = mpath
    return dev


COPY_DIR = "/run/datacore-sr"


def copy_vdisk_data(client, host_id, src_vdisk_id, src_wwn,
                    dst_vdisk_id, dst_wwn, dbg):
    """Attach src + dst to dom0 and run `qemu-img convert` between them.

    Used by Volume.clone: source is a fresh differential snapshot of the
    template (frozen point-in-time, safe to read even if the template is
    in use); destination is a fresh empty mirrored vDisk at the same size.

    Writes go through writethrough cache mode so the data is durable on the
    destination by the time qemu-img exits (qemu-img defaults to cache=unsafe
    which skips fsync at end — that bites us with qemu-nbd-style destinations
    but applies to raw block devices too on some kernels)."""
    src_dev = _attach_vdisk_to_dom0(client, host_id, src_vdisk_id, src_wwn, dbg)
    dst_dev = _attach_vdisk_to_dom0(client, host_id, dst_vdisk_id, dst_wwn, dbg)
    log.info("{}: copy_vdisk_data: {} -> {}".format(dbg, src_dev, dst_dev))
    subprocess.run(
        [QEMU_IMG, "convert", "-n", "-t", "writethrough",
         "-f", "raw", "-O", "raw", src_dev, dst_dev],
        check=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def unserve_and_evict(client, host_id, vdisk_id, wwn, dbg):
    """Reverse of `_attach_vdisk_to_dom0`: best-effort teardown that flushes
    multipath, evicts sd paths, then Unserves on the array. Errors are
    logged but not raised — callers run this from cleanup paths where
    forward progress matters more than perfect reporting. Holds the same
    dom0 lock as the attach side."""
    dp = _datapath_helpers()
    with _dom0_attach_lock():
        if wwn:
            try:
                dp._flush_multipath(wwn, dbg)
            except Exception as e:
                log.error("{}: unserve_and_evict flush_multipath {}: {}".format(dbg, wwn, e))
            try:
                dp._evict_scsi_paths_for_wwn(wwn, dbg)
            except Exception as e:
                log.error("{}: unserve_and_evict evict_scsi {}: {}".format(dbg, wwn, e))
        try:
            client.unserve_vdisk(vdisk_id, host_id)
        except Exception as e:
            log.error("{}: unserve_and_evict unserve {}: {}".format(dbg, vdisk_id, e))
