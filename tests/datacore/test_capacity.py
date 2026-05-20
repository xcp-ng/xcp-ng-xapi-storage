"""SR.stat capacity computation. Bugs here surface as wrong physical-size
or free_space in xe sr-list — annoying but not data-loss."""

import datacoreapi


POOL_A = "pool-a"
POOL_B = "pool-b"


class FakeClient(datacoreapi.DataCoreClient):
    """Inherit so list_pool_members / list_virtualdisks / pool_capacity_bytes
    / sr_allocated_bytes resolve normally. Override __init__ to skip the
    HTTP session setup, and .get() to return canned data."""

    def __init__(self, members=None, vdisks=None, raise_on=None):
        self._members = members or []
        self._vdisks = vdisks or []
        self._raise_on = raise_on or set()

    def get(self, path):
        if path in self._raise_on:
            raise datacoreapi.DataCoreError("simulated failure")
        if path == "/poolmembers":
            return self._members
        if path == "/virtualdisks":
            return self._vdisks
        raise AssertionError("unexpected GET: " + path)


# -------- pool_capacity_bytes --------

def test_pool_capacity_sums_only_matching_pool():
    c = FakeClient(members=[
        {"DiskPoolId": POOL_A, "Size": {"Value": 100}},
        {"DiskPoolId": POOL_A, "Size": {"Value": 50}},
        {"DiskPoolId": POOL_B, "Size": {"Value": 200}},
        {"DiskPoolId": "unrelated", "Size": {"Value": 999}},
    ])
    assert c.pool_capacity_bytes(POOL_A) == 150
    assert c.pool_capacity_bytes(POOL_B) == 200


def test_pool_capacity_handles_malformed_sizes():
    """Production has seen Size as None, missing key, missing Value, and
    non-numeric Value. None of these should propagate or raise."""
    c = FakeClient(members=[
        {"DiskPoolId": POOL_A, "Size": {"Value": 100}},
        {"DiskPoolId": POOL_A, "Size": {}},              # missing Value
        {"DiskPoolId": POOL_A, "Size": {"Value": "x"}},  # non-numeric
        {"DiskPoolId": POOL_A, "Size": None},            # None instead of dict
        {"DiskPoolId": POOL_A},                          # no Size key at all
    ])
    assert c.pool_capacity_bytes(POOL_A) == 100


def test_pool_capacity_empty_pool_returns_zero():
    c = FakeClient(members=[])
    assert c.pool_capacity_bytes(POOL_A) == 0


def test_pool_capacity_endpoint_failure_returns_zero():
    """An informational call (SR.stat) must not raise when /poolmembers fails.
    Reporting 0 is honest; raising would break sr-list output."""
    c = FakeClient(raise_on={"/poolmembers"})
    assert c.pool_capacity_bytes(POOL_A) == 0


# -------- sr_allocated_bytes --------

SR = "abcdef12-1111-2222-3333-444444444444"
OTHER_SR = "deadbeef-aaaa-bbbb-cccc-dddddddddddd"


def test_sr_allocated_filters_by_alias_prefix():
    """Only count our SR's vDisks (Alias prefix match). Other-tenant vDisks
    on the same DataCore pools are invisible — known limitation noted in
    the README."""
    c = FakeClient(vdisks=[
        {"Alias": datacoreapi.vdisk_prefix(SR) + "1", "Size": {"Value": 10}},
        {"Alias": datacoreapi.vdisk_prefix(SR) + "2", "Size": {"Value":  5}},
        {"Alias": datacoreapi.vdisk_prefix(OTHER_SR) + "x", "Size": {"Value": 999}},
        {"Alias": "completely-unrelated", "Size": {"Value": 999}},
    ])
    assert c.sr_allocated_bytes(SR) == 15


def test_sr_allocated_handles_malformed_sizes():
    c = FakeClient(vdisks=[
        {"Alias": datacoreapi.vdisk_prefix(SR) + "ok",   "Size": {"Value": 100}},
        {"Alias": datacoreapi.vdisk_prefix(SR) + "n",    "Size": None},
        {"Alias": datacoreapi.vdisk_prefix(SR) + "miss"},  # no Size at all
    ])
    assert c.sr_allocated_bytes(SR) == 100


def test_sr_allocated_empty_returns_zero():
    c = FakeClient(vdisks=[])
    assert c.sr_allocated_bytes(SR) == 0


# -------- The min(A, B) - allocated end-to-end shape --------

def test_full_capacity_picture():
    """The interpretation SR.stat uses: total_space = min(pool A, pool B)
    because mirrored vDisks consume both legs; free = total - our allocated."""
    c = FakeClient(
        members=[
            {"DiskPoolId": POOL_A, "Size": {"Value": 200 * 10**9}},
            {"DiskPoolId": POOL_B, "Size": {"Value": 100 * 10**9}},
        ],
        vdisks=[
            {"Alias": datacoreapi.vdisk_prefix(SR) + "x", "Size": {"Value": 30 * 10**9}},
        ],
    )
    total = min(c.pool_capacity_bytes(POOL_A), c.pool_capacity_bytes(POOL_B))
    free = total - c.sr_allocated_bytes(SR)
    assert total == 100 * 10**9       # smaller leg gates
    assert free == 70 * 10**9         # 100 - 30
