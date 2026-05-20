"""Snapshot pool selection + DataCore host lookup by initiator IQN.

Two narrow areas where the wrong answer hurts:
  pick_snapshot_pool — wrong choice = HA imbalance, all snapshots on one server
  find_host_id_by_iqn — wrong answer = SR.attach for the wrong host, or wrongly
  reports "no host" and rejects the attach
"""

import datacoreapi


# -------- pick_snapshot_pool --------

POOL_A = "SRV-A-id:{pool-a-guid}"
POOL_B = "SRV-B-id:{pool-b-guid}"
CFG = {"first-pool": POOL_A, "second-pool": POOL_B}


def test_pick_snapshot_pool_source_prefers_A_lands_on_B():
    """Per DataCore docs ("Where possible create snapshots on the non-preferred
    side of a mirrored Virtual Disk"), a source whose PreferredServer is on
    pool A must produce a snapshot destination on pool B."""
    assert datacoreapi.pick_snapshot_pool(
        CFG, {"PreferredServer": "SRV-A-id"}
    ) == POOL_B


def test_pick_snapshot_pool_source_prefers_B_lands_on_A():
    assert datacoreapi.pick_snapshot_pool(
        CFG, {"PreferredServer": "SRV-B-id"}
    ) == POOL_A


def test_pick_snapshot_pool_unknown_preference_defaults_to_second():
    """No PreferredServer field -> default to second-pool so we don't pile
    everything onto first-pool by accident."""
    assert datacoreapi.pick_snapshot_pool(CFG, {}) == POOL_B


def test_pick_snapshot_pool_handles_dict_shaped_preferred_server():
    """DataCore sometimes returns PreferredServer as {Id, Caption} dict."""
    assert datacoreapi.pick_snapshot_pool(
        CFG, {"PreferredServer": {"Id": "SRV-A-id", "Caption": "name"}}
    ) == POOL_B


def test_pick_snapshot_pool_uses_first_host_id_fallback():
    """If PreferredServer is missing, fall back to FirstHostId field."""
    assert datacoreapi.pick_snapshot_pool(
        CFG, {"FirstHostId": "SRV-B-id"}
    ) == POOL_A


def test_server_id_from_pool_handles_empty():
    """Defensive: empty / None pool id must not raise."""
    assert datacoreapi._server_id_from_pool("") == ""
    assert datacoreapi._server_id_from_pool(None) == ""


def test_server_id_from_pool_extracts_leading_server_id():
    assert datacoreapi._server_id_from_pool("SRV-X:{pool-guid}") == "SRV-X"


# -------- find_host_id_by_iqn --------

class FakePortsClient:
    def __init__(self, ports):
        self._ports = ports

    def get(self, path):
        assert path == "/ports", path
        return self._ports


def test_find_host_id_by_iqn_match():
    """Common case: our initiator IQN is registered as PortType=3 (iSCSI)
    against one DataCore host."""
    c = FakePortsClient([
        {"PortName": "iqn.other", "PortType": 3, "HostId": "other-host"},
        {"PortName": "iqn.mine",  "PortType": 3, "HostId": "my-host"},
    ])
    assert datacoreapi.DataCoreClient.find_host_id_by_iqn(c, "iqn.mine") == "my-host"


def test_find_host_id_by_iqn_ignores_non_iscsi_port_type():
    """A port with the matching PortName but PortType != 3 (e.g. FC) must
    NOT be considered a match — we'd attach the wrong transport."""
    c = FakePortsClient([
        {"PortName": "iqn.mine", "PortType": 4, "HostId": "wrong-host"},
    ])
    assert datacoreapi.DataCoreClient.find_host_id_by_iqn(c, "iqn.mine") is None


def test_find_host_id_by_iqn_no_match_returns_none():
    """First-time setup before RegisterPort: the IQN isn't anywhere in /ports.
    The caller (SR.attach) then surfaces a bootstrap instruction."""
    c = FakePortsClient([
        {"PortName": "iqn.someone-else", "PortType": 3, "HostId": "x"},
    ])
    assert datacoreapi.DataCoreClient.find_host_id_by_iqn(c, "iqn.mine") is None


def test_find_host_id_by_iqn_empty_ports_list():
    c = FakePortsClient([])
    assert datacoreapi.DataCoreClient.find_host_id_by_iqn(c, "iqn.x") is None


def test_find_host_id_by_iqn_handles_none_response():
    """Defensive: /ports endpoint might return None (e.g. parser issue).
    Must not raise."""

    class NoneClient:
        def get(self, path):
            return None

    assert datacoreapi.DataCoreClient.find_host_id_by_iqn(
        NoneClient(), "iqn.x") is None


# -------- list_iscsi_target_portals_by_server --------

def test_list_target_portals_groups_by_server():
    """One target port per server, each with one portal IP."""
    c = FakePortsClient([
        {
            "PortName": "iqn.2000-08.com.datacore:srv-a-1",
            "HostId": "SRV-A",
            "IScsiPortStateInfo": {
                "PortalsState": [{"Address": {"Address": "10.0.0.1"}}],
            },
        },
        {
            "PortName": "iqn.2000-08.com.datacore:srv-b-1",
            "HostId": "SRV-B",
            "IScsiPortStateInfo": {
                "PortalsState": [{"Address": {"Address": "10.0.0.2"}}],
            },
        },
    ])
    portals = datacoreapi.DataCoreClient.list_iscsi_target_portals_by_server(c)
    assert portals == {"SRV-A": ["10.0.0.1"], "SRV-B": ["10.0.0.2"]}


def test_list_target_portals_skips_non_iqn_ports():
    """FC port (no iqn. prefix) must NOT be misclassified as iSCSI target."""
    c = FakePortsClient([
        {
            "PortName": "20-02-44-9F-7F-A1-77-F9",  # FC WWN, not IQN
            "HostId": "SRV-A",
            "IScsiPortStateInfo": {
                "PortalsState": [{"Address": {"Address": "10.0.0.1"}}],
            },
        },
    ])
    portals = datacoreapi.DataCoreClient.list_iscsi_target_portals_by_server(c)
    assert portals == {}


def test_list_target_portals_skips_initiator_ports():
    """Initiator ports don't have PortalsState entries (they connect outward,
    not listen). Filtering on Address presence skips them automatically —
    no need for fragile RoleCapability/PortMode magic-number checks."""
    c = FakePortsClient([
        {
            "PortName": "iqn.1991-05.com.microsoft:somehost",  # initiator
            "HostId": "SRV-A",
            # No IScsiPortStateInfo OR empty PortalsState
        },
        {
            "PortName": "iqn.2000-08.com.datacore:srv-a-1",  # target
            "HostId": "SRV-A",
            "IScsiPortStateInfo": {
                "PortalsState": [{"Address": {"Address": "10.0.0.1"}}],
            },
        },
    ])
    portals = datacoreapi.DataCoreClient.list_iscsi_target_portals_by_server(c)
    assert portals == {"SRV-A": ["10.0.0.1"]}


def test_list_target_portals_accumulates_multi_nic_per_server():
    """Two iSCSI target ports on the same server, different NICs: both IPs
    should land in the same server's list, order preserved."""
    c = FakePortsClient([
        {
            "PortName": "iqn.2000-08.com.datacore:srv-a-1",
            "HostId": "SRV-A",
            "IScsiPortStateInfo": {
                "PortalsState": [{"Address": {"Address": "10.0.0.1"}}],
            },
        },
        {
            "PortName": "iqn.2000-08.com.datacore:srv-a-2",
            "HostId": "SRV-A",
            "IScsiPortStateInfo": {
                "PortalsState": [{"Address": {"Address": "10.0.0.11"}}],
            },
        },
    ])
    portals = datacoreapi.DataCoreClient.list_iscsi_target_portals_by_server(c)
    assert portals == {"SRV-A": ["10.0.0.1", "10.0.0.11"]}


def test_list_target_portals_empty_when_no_ports():
    """Defensive: empty /ports."""
    assert datacoreapi.DataCoreClient.list_iscsi_target_portals_by_server(
        FakePortsClient([])) == {}
