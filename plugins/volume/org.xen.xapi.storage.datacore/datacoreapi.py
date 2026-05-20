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

import json
import os
import time
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
import urllib3
from urllib3.util.retry import Retry

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

    def resize_vdisk(self, vdisk_id, new_size):
        """Online resize a vDisk via PUT /virtualdisks/{id} with the Size field.

        DataCore does NOT use `POST {"Operation": "Resize"}` for this — that
        operation is "is not valid for this request". Resize is just a property
        change like Name/Description, done via PUT. The array accepts shrink
        as well as grow, but Volume.resize refuses shrink (data-loss risk).
        """
        return self.put("/virtualdisks/{}".format(vdisk_id),
                        {"Size": int(new_size)})

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
    """Pick the non-preferred-side pool for a snapshot, per DataCore best practice.

    Quoting Snapshot Operations docs: "Where possible create snapshots on the
    non-preferred side of a mirrored Virtual Disk." That keeps snapshot
    capacity balanced across both servers instead of piling everything on the
    primary. DataCore snapshots are always single-pool ("The snapshot can only
    exist on one server"); HA of the snapshot itself is out of scope at this
    API level (see plugin README).

    If we can't determine the source's preferred server, default to
    second-pool (still avoids the all-on-first-pool failure mode).
    """
    first = cfg["first-pool"]
    second = cfg["second-pool"]
    preferred = _vdisk_preferred_server_id(source_vdisk)
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
