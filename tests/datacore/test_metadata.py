"""Tests for the xcp-ng metadata blob encoding stored in each vDisk's
Description field. The blob is the source of truth for VDI identity
because DataCore truncates Alias at ~48 chars."""

import json

import pytest

import datacoreapi
import volume


def test_encode_minimum_fields():
    blob = datacoreapi.encode_metadata(
        vdi_uuid="vu", sr_uuid="su", vdi_name="n",
        sharable=False, read_write=True,
    )
    parsed = json.loads(blob)
    assert parsed["xcp-ng:vdi-uuid"] == "vu"
    assert parsed["xcp-ng:sr-uuid"] == "su"
    assert parsed["xcp-ng:vdi-name"] == "n"
    assert parsed["xcp-ng:read-write"] is True
    assert parsed["xcp-ng:sharable"] is False
    # Optional keys must not appear unless asked
    assert "xcp-ng:is-snapshot" not in parsed
    assert "xcp-ng:parent-vdi-uuid" not in parsed


def test_encode_with_snapshot_lineage_and_custom():
    blob = datacoreapi.encode_metadata(
        vdi_uuid="snap", sr_uuid="sr", vdi_name="my-vdi",
        description="some description",
        sharable=False, read_write=False,
        is_snapshot=True, parent_vdi_uuid="parent",
        custom={"role": "system", "tier": "gold"},
    )
    parsed = json.loads(blob)
    assert parsed["xcp-ng:is-snapshot"] is True
    assert parsed["xcp-ng:parent-vdi-uuid"] == "parent"
    assert parsed["xcp-ng:vdi-description"] == "some description"
    assert parsed["xcp-ng:custom:role"] == "system"
    assert parsed["xcp-ng:custom:tier"] == "gold"


def test_encode_truncates_long_name_and_description():
    blob = datacoreapi.encode_metadata(
        vdi_uuid="u", sr_uuid="s",
        vdi_name="x" * 500,
        description="y" * 500,
        sharable=False, read_write=True,
    )
    parsed = json.loads(blob)
    assert len(parsed["xcp-ng:vdi-name"]) == 200
    assert len(parsed["xcp-ng:vdi-description"]) == 200


def test_encode_enforces_1024_char_cap():
    # Pad with a huge custom value to bust the cap and confirm the error
    with pytest.raises(datacoreapi.DataCoreError) as exc_info:
        datacoreapi.encode_metadata(
            vdi_uuid="u", sr_uuid="s", vdi_name="n",
            sharable=False, read_write=True,
            custom={"junk": "x" * 2000},
        )
    assert "1024" in str(exc_info.value)


def test_parse_metadata_handles_garbage():
    # Empty / None / non-JSON should not raise — should return {}
    assert datacoreapi.parse_metadata("") == {}
    assert datacoreapi.parse_metadata(None) == {}
    assert datacoreapi.parse_metadata("not json") == {}
    assert datacoreapi.parse_metadata('{"valid": "json"}') == {"valid": "json"}


def test_vdisk_to_vdi_info_uses_metadata():
    """vdisk_to_vdi_info should surface the metadata-encoded VDI identity,
    custom keys, and read_write flag — not the DataCore-side Alias."""
    meta = datacoreapi.encode_metadata(
        vdi_uuid="vdi-1", sr_uuid="sr-1", vdi_name="readable name",
        description="readable desc",
        sharable=False, read_write=False,
        custom={"role": "system"},
    )
    d = {
        "Id": "datacore-side-id",
        "Alias": "xcp-sr-1-vdi-1-truncated",
        "Description": meta,
        "Size": {"Value": 1073741824},
    }
    info = datacoreapi.vdisk_to_vdi_info(d, "sr-1")
    assert info["key"] == "vdi-1"
    assert info["uuid"] == "vdi-1"
    assert info["name"] == "readable name"
    assert info["description"] == "readable desc"
    assert info["read_write"] is False
    assert info["virtual_size"] == 1073741824
    assert info["keys"] == {"role": "system"}
    # URI scheme is what the datapath plugin matches on
    assert info["uri"] == ["datacore-iscsi://sr-1/datacore-side-id"]


def test_vdisk_to_vdi_info_falls_back_on_missing_metadata():
    """Older vDisks without a metadata blob (or with an unparseable one)
    must still produce a valid vdi_info using the array's Id."""
    d = {
        "Id": "no-metadata-id",
        "Alias": "no-metadata-alias",
        "Description": None,
        "Size": {"Value": 1024},
    }
    info = datacoreapi.vdisk_to_vdi_info(d, "sr-x")
    assert info["key"] == "no-metadata-id"
    assert info["name"] == "no-metadata-alias"
    assert info["read_write"] is True  # default
    assert info["description"] == ""


def test_reencode_metadata_adds_and_removes_keys():
    """volume._reencode_metadata is the round-trip helper used by
    Volume.set / unset / set_name / set_description. It must:
    - update fields in place
    - delete keys when override value is None
    - preserve untouched keys (including unknown ones from a future schema)
    """
    base = datacoreapi.encode_metadata(
        vdi_uuid="u", sr_uuid="s", vdi_name="original",
        sharable=False, read_write=True,
        custom={"role": "system", "tier": "silver"},
    )
    d = {"Description": base}
    new_blob = volume._reencode_metadata(
        d,
        **{
            "xcp-ng:vdi-name": "renamed",
            "xcp-ng:custom:role": None,       # delete
            "xcp-ng:custom:tier": "gold",     # change
            "xcp-ng:custom:owner": "alice",   # new
        }
    )
    parsed = json.loads(new_blob)
    assert parsed["xcp-ng:vdi-name"] == "renamed"
    assert "xcp-ng:custom:role" not in parsed
    assert parsed["xcp-ng:custom:tier"] == "gold"
    assert parsed["xcp-ng:custom:owner"] == "alice"
    # untouched
    assert parsed["xcp-ng:vdi-uuid"] == "u"
    assert parsed["xcp-ng:sr-uuid"] == "s"
    assert parsed["xcp-ng:read-write"] is True


def test_reencode_metadata_preserves_unknown_keys():
    """Forward-compat: if a future plugin version writes new xcp-ng:* keys,
    an older plugin running Volume.set_name must not strip them."""
    d = {"Description": json.dumps({
        "xcp-ng:vdi-uuid": "u",
        "xcp-ng:vdi-name": "n",
        "xcp-ng:future-feature": "do not lose me",
    })}
    new_blob = volume._reencode_metadata(d, **{"xcp-ng:vdi-name": "renamed"})
    parsed = json.loads(new_blob)
    assert parsed["xcp-ng:future-feature"] == "do not lose me"


def test_reencode_metadata_enforces_1024_cap():
    d = {"Description": json.dumps({"xcp-ng:vdi-uuid": "u"})}
    with pytest.raises(datacoreapi.DataCoreError):
        volume._reencode_metadata(d, **{"xcp-ng:huge": "x" * 2000})


def test_find_vdisk_by_vdi_uuid_filters_by_prefix():
    """find_vdisk_by_vdi_uuid uses Alias prefix + metadata lookup, so a vDisk
    in a different SR (different prefix) must not be returned even if its
    metadata happens to contain the same VDI UUID."""
    sr = "abcdef12-1111-2222-3333-444444444444"
    other_sr = "deadbeef-aaaa-bbbb-cccc-dddddddddddd"
    meta = datacoreapi.encode_metadata(
        vdi_uuid="target-vdi", sr_uuid=sr, vdi_name="x",
        sharable=False, read_write=True,
    )
    vdisks = [
        # right metadata but wrong-SR alias
        {"Alias": datacoreapi.vdisk_prefix(other_sr) + "x", "Description": meta},
        # right SR prefix and metadata
        {"Alias": datacoreapi.vdisk_prefix(sr) + "x", "Description": meta},
    ]

    class FakeClient:
        def list_virtualdisks(self):
            return vdisks

    found = datacoreapi.find_vdisk_by_vdi_uuid(FakeClient(), sr, "target-vdi")
    assert found is vdisks[1]  # the SR-prefixed one
