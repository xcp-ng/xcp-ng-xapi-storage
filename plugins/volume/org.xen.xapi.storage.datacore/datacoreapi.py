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
from urllib.parse import urlparse

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

API_PATH = "/RestService/rest.svc/1.0"
VOLUME_TYPE_MIRRORED = 2


class DataCoreError(Exception):
    pass


def _truthy(v):
    return str(v).lower() in ("true", "1", "yes", "on")


def _resolve_password(cfg):
    """
    XAPI silently rewrites the device-config key `password` to `password_secret`
    holding a Secret UUID. The plugin must look up the plaintext via XAPI.
    Plain `password` is supported as a passthrough for direct (non-XAPI) testing.
    """
    if "password" in cfg:
        return cfg["password"]
    secret_uuid = cfg.get("password_secret")
    if not secret_uuid:
        raise DataCoreError("No 'password' or 'password_secret' in SR configuration")
    import XenAPI
    session = XenAPI.xapi_local()
    session.xenapi.login_with_password("", "", "1.0", "datacore-plugin")
    try:
        ref = session.xenapi.secret.get_by_uuid(secret_uuid)
        return session.xenapi.secret.get_value(ref)
    finally:
        session.xenapi.session.logout()


class DataCoreClient:
    def __init__(self, endpoint, username, password, verify=False):
        self.base = endpoint.rstrip("/") + API_PATH
        self.session = requests.Session()
        self.session.verify = verify
        self.session.auth = (username, password)
        self.session.headers["ServerHost"] = urlparse(endpoint).hostname or "localhost"

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
        return self._check(self.session.get(self.base + path))

    def post(self, path, body=None):
        return self._check(self.session.post(self.base + path, json=body or {}))

    def put(self, path, body):
        return self._check(self.session.put(self.base + path, json=body))

    def delete(self, path):
        return self._check(self.session.delete(self.base + path))

    def list_pools(self):
        return self.get("/pools") or []

    def list_virtualdisks(self):
        return self.get("/virtualdisks") or []

    def find_vdisk_by_id(self, vdisk_id):
        for d in self.list_virtualdisks():
            if d.get("Id") == vdisk_id:
                return d
        return None

    def serve_vdisk(self, vdisk_id, host_id):
        return self.post("/virtualdisks/{}".format(vdisk_id),
                         {"Operation": "Serve", "Host": host_id})

    def unserve_vdisk(self, vdisk_id, host_id):
        return self.post("/virtualdisks/{}".format(vdisk_id),
                         {"Operation": "Unserve", "Host": host_id})

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

    def update_description(self, vdisk_id, description):
        self.put("/virtualdisks/{}".format(vdisk_id), {"Description": description})

    def delete_vdisk(self, vdisk_id):
        self.delete("/virtualdisks/{}".format(vdisk_id))


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


def encode_metadata(vdi_uuid, sr_uuid, vdi_name, sharable, read_write, custom=None,
                    max_len=1024):
    blob = {
        "xcp-ng:vdi-uuid": vdi_uuid,
        "xcp-ng:sr-uuid": sr_uuid,
        "xcp-ng:vdi-name": (vdi_name or "")[:200],
        "xcp-ng:sharable": bool(sharable),
        "xcp-ng:read-write": bool(read_write),
    }
    for k, v in (custom or {}).items():
        blob["xcp-ng:custom:{}".format(k)] = v
    out = json.dumps(blob, separators=(",", ":"))
    if len(out) > max_len:
        raise DataCoreError("Metadata blob exceeds {} chars".format(max_len))
    return out


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
        "description": "",
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
