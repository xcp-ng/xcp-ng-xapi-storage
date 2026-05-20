"""SR.probe — the discovery surface for `xe sr-probe type=datacore`. Wrong
shape here means xe sr-probe is unusable for new operators; wrong pairing
logic means we suggest mirror configurations the array would reject.
"""

import datacoreapi
import sr as sr_mod


class FakePoolsClient(datacoreapi.DataCoreClient):
    """Skip session setup; canned response on /pools."""
    def __init__(self, pools):
        self._pools = pools

    def get(self, path):
        assert path == "/pools", path
        return self._pools


def _call_probe(client, monkeypatch, configuration):
    """Invoke SR.probe.Implementation directly with our FakeClient. We
    monkey-patch DataCoreClient.from_sr_config to return the fake."""
    monkeypatch.setattr(
        datacoreapi.DataCoreClient, "from_sr_config",
        staticmethod(lambda cfg: client),
    )
    impl = sr_mod.Implementation()
    return impl.probe("dbg", configuration)


def test_probe_returns_list_not_dict(monkeypatch):
    """The SMAPIv3 v5 API expects a LIST of probe_result_sr records.
    Returning a dict (the original stub did) makes XAPI's type-check raise.
    """
    c = FakePoolsClient([])
    results = _call_probe(c, monkeypatch, {})
    assert isinstance(results, list)


def test_probe_empty_when_no_pools(monkeypatch):
    """Auth worked but no pools configured → empty list, not an error."""
    c = FakePoolsClient([])
    assert _call_probe(c, monkeypatch, {}) == []


def test_probe_empty_when_only_one_server(monkeypatch):
    """A single-server SANsymphony can't host mirrored vDisks. Return no
    pairs — the operator gets immediate "no valid config" feedback
    instead of being able to assemble a doomed sr-create."""
    c = FakePoolsClient([
        {"Id": "SRV-A:{p1}", "ServerId": "SRV-A", "Alias": "Pool 1"},
        {"Id": "SRV-A:{p2}", "ServerId": "SRV-A", "Alias": "Pool 2"},
    ])
    assert _call_probe(c, monkeypatch, {}) == []


def test_probe_yields_one_entry_per_cross_server_pair(monkeypatch):
    """The canonical 2-server / 1-pool-per-server lab: exactly one pair."""
    c = FakePoolsClient([
        {"Id": "SRV-A:{pa}", "ServerId": "SRV-A", "Alias": "Pool A"},
        {"Id": "SRV-B:{pb}", "ServerId": "SRV-B", "Alias": "Pool B"},
    ])
    results = _call_probe(c, monkeypatch, {})
    assert len(results) == 1
    r = results[0]
    assert r["configuration"]["first-pool"] == "SRV-A:{pa}"
    assert r["configuration"]["second-pool"] == "SRV-B:{pb}"
    assert r["complete"] is False  # still need iscsi-portals, host-id, password
    # `sr` key omitted, not set to None — see comment in sr.probe()
    assert "sr" not in r
    assert r["extra_info"]["first-pool-name"] == "Pool A"
    assert r["extra_info"]["second-pool-name"] == "Pool B"
    assert r["extra_info"]["first-server-id"] == "SRV-A"
    assert r["extra_info"]["second-server-id"] == "SRV-B"


def test_probe_dedupes_symmetric_pairs(monkeypatch):
    """For (A, B) we emit one entry; we do NOT also emit (B, A) — that's
    the same mirror at the array level. Operator can swap on the actual
    sr-create command if they want a specific PreferredServer."""
    c = FakePoolsClient([
        {"Id": "SRV-A:{pa}", "ServerId": "SRV-A", "Alias": "Pool A"},
        {"Id": "SRV-B:{pb}", "ServerId": "SRV-B", "Alias": "Pool B"},
    ])
    results = _call_probe(c, monkeypatch, {})
    assert len(results) == 1


def test_probe_multi_pool_per_server_yields_all_combinations(monkeypatch):
    """2 pools on A × 3 pools on B → 6 distinct mirror configurations."""
    c = FakePoolsClient([
        {"Id": "SRV-A:{a1}", "ServerId": "SRV-A", "Alias": "A1"},
        {"Id": "SRV-A:{a2}", "ServerId": "SRV-A", "Alias": "A2"},
        {"Id": "SRV-B:{b1}", "ServerId": "SRV-B", "Alias": "B1"},
        {"Id": "SRV-B:{b2}", "ServerId": "SRV-B", "Alias": "B2"},
        {"Id": "SRV-B:{b3}", "ServerId": "SRV-B", "Alias": "B3"},
    ])
    results = _call_probe(c, monkeypatch, {})
    assert len(results) == 6
    # Confirm every entry has cross-server pools
    for r in results:
        assert r["extra_info"]["first-server-id"] != r["extra_info"]["second-server-id"]


def test_probe_preserves_input_configuration_keys(monkeypatch):
    """The caller passed credentials; the candidate configurations we emit
    must include them so the operator can copy-paste the result into
    sr-create without re-typing rest-endpoint/username/password."""
    c = FakePoolsClient([
        {"Id": "SRV-A:{pa}", "ServerId": "SRV-A", "Alias": "Pool A"},
        {"Id": "SRV-B:{pb}", "ServerId": "SRV-B", "Alias": "Pool B"},
    ])
    inp = {
        "rest-endpoint": "https://datacore",
        "username": "Administrateur",
        "password_secret": "uuid-xyz",
    }
    results = _call_probe(c, monkeypatch, inp)
    for r in results:
        for k, v in inp.items():
            assert r["configuration"][k] == v


def test_probe_falls_back_to_pool_id_prefix_when_serverid_missing(monkeypatch):
    """If a pool record happens to lack the top-level ServerId field, we
    fall back to parsing the leading "{ServerId}:" prefix off the Id.
    Defensive against schema variations across DataCore versions."""
    c = FakePoolsClient([
        {"Id": "SRV-A:{pa}", "Alias": "Pool A"},   # no ServerId field
        {"Id": "SRV-B:{pb}", "Alias": "Pool B"},
    ])
    results = _call_probe(c, monkeypatch, {})
    assert len(results) == 1
    assert results[0]["extra_info"]["first-server-id"] == "SRV-A"
