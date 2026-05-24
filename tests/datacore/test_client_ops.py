"""REST client config + the two operation wrappers that gained behavior
beyond a bare HTTP call: serve_vdisk (idempotency) and wait_for_vdisk_online
(state-machine polling)."""

import time

import pytest

import datacoreapi


# -------- _build_retry adapter config --------

def test_retry_excludes_post():
    """POST must NOT be in allowed_methods. urllib3 can't tell a 5xx-but-
    processed POST from a 5xx-and-rejected one, and retrying a Create or
    Serve would risk duplicate side-effects."""
    r = datacoreapi._build_retry()
    assert "POST" not in r.allowed_methods
    for verb in ("GET", "PUT", "DELETE"):
        assert verb in r.allowed_methods, verb


def test_retry_status_forcelist_covers_server_errors():
    r = datacoreapi._build_retry()
    assert set(r.status_forcelist) == {500, 502, 503, 504}


def test_retry_counters_match_intent():
    r = datacoreapi._build_retry()
    assert r.total == 4
    assert r.connect == 4
    # backoff 0.5 gives roughly 0s, 1s, 2s, 4s — bounded ~7s
    assert r.backoff_factor == 0.5


def test_client_mounts_retry_adapter_on_both_schemes():
    """Both http:// and https:// must get the retry adapter — DataCore is
    HTTPS in production but http:// is supported for local testing."""
    c = datacoreapi.DataCoreClient("https://example.invalid", "u", "p", verify=False)
    adapter_https = c.session.get_adapter("https://example.invalid/")
    adapter_http  = c.session.get_adapter("http://example.invalid/")
    # Both adapters should have the same Retry config
    for adapter in (adapter_https, adapter_http):
        assert adapter.max_retries.total == 4
        assert "POST" not in adapter.max_retries.allowed_methods


def test_http_timeout_constant():
    """The (connect, read) tuple is what protects against a hung DataCore
    server. Connect must be short; read must be generous for slow ops."""
    connect, read = datacoreapi.HTTP_TIMEOUT
    assert 1 <= connect <= 10, connect       # fast handshake to a live host
    assert 30 <= read   <= 120, read         # generous for create/serve


def test_serverhost_header_set_from_endpoint_hostname():
    """DataCore returns ErrorCode 9 if the ServerHost header is missing."""
    c = datacoreapi.DataCoreClient(
        "https://datacore.example.com:8443", "u", "p", verify=False)
    assert c.session.headers["ServerHost"] == "datacore.example.com"


# -------- serve_vdisk idempotency --------

class FakePostClient(datacoreapi.DataCoreClient):
    """Override .post to return canned data or raise a canned error."""
    def __init__(self, ret=None, raise_with=None):
        self._ret = ret
        self._raise_with = raise_with

    def post(self, path, body=None):
        if self._raise_with is not None:
            raise self._raise_with
        return self._ret


def test_serve_vdisk_success_path():
    c = FakePostClient(ret=[{"ok": True}])
    assert c.serve_vdisk("vdisk-1", "host-1") == [{"ok": True}]


def test_serve_vdisk_treats_already_exists_as_success():
    """After VM-shutdown-then-restart, the LUN is often still mapped on the
    array. The next Datapath.attach must not fail just because DataCore
    agrees the mapping exists."""
    c = FakePostClient(raise_with=datacoreapi.DataCoreError(
        "HTTP 400: A path with the chosen initiator and target ports already exists."
    ))
    assert c.serve_vdisk("vdisk-1", "host-1") is None


def test_serve_vdisk_other_400_propagates():
    """We must NOT swallow unrelated 400s — e.g. unknown host id."""
    c = FakePostClient(raise_with=datacoreapi.DataCoreError(
        "HTTP 400: Host 'unknown' does not exist"
    ))
    with pytest.raises(datacoreapi.DataCoreError, match="Host 'unknown'"):
        c.serve_vdisk("vdisk-1", "host-1")


def test_serve_vdisk_non_datacore_error_propagates():
    """A connection-level error is unrelated to the idempotency story."""
    class NetErr(Exception):
        pass
    c = FakePostClient(raise_with=NetErr("connection reset"))
    with pytest.raises(NetErr):
        c.serve_vdisk("vdisk-1", "host-1")


# -------- wait_for_vdisk_online --------

class FakeFindClient(datacoreapi.DataCoreClient):
    """Yield a scripted sequence of DiskStatus values from successive
    find_vdisk_by_id calls. Exhaust -> last value sticks."""
    def __init__(self, statuses):
        self._statuses = list(statuses)
        self.calls = 0

    def find_vdisk_by_id(self, vid):
        self.calls += 1
        s = self._statuses.pop(0) if self._statuses else (
            self._statuses[-1] if self._statuses else 0)
        return {"Id": vid, "DiskStatus": s}


def test_wait_returns_immediately_when_already_online():
    c = FakeFindClient([0])
    t0 = time.monotonic()
    r = c.wait_for_vdisk_online("vid", interval=0.05)
    assert r["DiskStatus"] == datacoreapi.DISK_STATUS_ONLINE
    assert c.calls == 1
    assert (time.monotonic() - t0) < 0.1


def test_wait_polls_until_status_transitions_to_online():
    c = FakeFindClient([1, 1, 1, 0])
    r = c.wait_for_vdisk_online("vid", interval=0.05)
    assert r["DiskStatus"] == 0
    assert c.calls == 4


def test_wait_times_out_when_status_never_reaches_online():
    """Bounded timeout: if mirror sync never converges, raise with the last
    seen DiskStatus so the operator has a starting point."""
    c = FakeFindClient([1] * 100)
    with pytest.raises(datacoreapi.DataCoreError) as exc_info:
        c.wait_for_vdisk_online("vid", timeout=0.3, interval=0.05)
    msg = str(exc_info.value)
    assert "did not become ready" in msg
    assert "DiskStatus stayed at 1" in msg


def test_wait_returns_immediately_when_diskstatus_field_missing():
    """Future schema / older DataCore: if the field isn't present, trust
    the array and don't spin forever."""
    class NoStatusClient(datacoreapi.DataCoreClient):
        def __init__(self): self.calls = 0
        def find_vdisk_by_id(self, vid):
            self.calls += 1
            return {"Id": vid}  # no DiskStatus key
    c = NoStatusClient()
    r = c.wait_for_vdisk_online("vid", interval=0.05)
    assert r.get("DiskStatus") is None
    assert c.calls == 1


def test_wait_raises_when_vdisk_disappears():
    """If the vDisk is gone between create and the next find, that's a
    failure mode worth surfacing distinctly from "still syncing"."""
    class GoneClient(datacoreapi.DataCoreClient):
        def __init__(self): pass
        def find_vdisk_by_id(self, vid):
            return None
    with pytest.raises(datacoreapi.DataCoreError, match="disappeared"):
        GoneClient().wait_for_vdisk_online("vid")
