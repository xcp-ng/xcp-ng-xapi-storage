"""Datapath helpers: URI parsing and orphan-sd eviction. The eviction test
is the headline one — without it we silently fail to reclaim sd entries
after VM hard-shutdown cycles and `Datapath.attach` hangs at 30 s timeout."""

import builtins
import io
import os

import pytest

import datapath


# -------- _parse_uri --------

def test_parse_uri_valid_datacore():
    sr, vid = datapath._parse_uri("datacore-iscsi://my-sr-uuid/my-vdisk-id")
    assert sr == "my-sr-uuid"
    assert vid == "my-vdisk-id"


def test_parse_uri_rejects_unsupported_scheme():
    """We should fail fast if XAPI ever calls us with a URI for a different
    backend — silent acceptance would mask configuration bugs."""
    with pytest.raises(Exception, match="Unsupported URI scheme"):
        datapath._parse_uri("file:///dev/sda")


def test_parse_uri_strips_leading_slash_from_path():
    """The vdisk id never contains a leading slash on the wire, but urlparse
    leaves one in u.path; the helper must strip it consistently."""
    _sr, vid = datapath._parse_uri("datacore-iscsi://sr/0123456789abcdef")
    assert vid == "0123456789abcdef"
    assert not vid.startswith("/")


# -------- _evict_orphan_datacore_sds --------

@pytest.fixture
def fake_sysblock(tmp_path, monkeypatch):
    """Build a fake /sys/block tree and route the helper's os.listdir +
    open() through it. Returns a callable used to add devices.

    Each device entry:
      <tmp_path>/<name>/device/{vendor,model,wwid,delete}

    The `delete` file is what the helper writes "1" to when evicting; we
    inspect its contents to see which devices were evicted.
    """

    def make_device(name, vendor, model, wwid):
        dev = tmp_path / name / "device"
        dev.mkdir(parents=True)
        (dev / "vendor").write_text(vendor + "\n")
        (dev / "model").write_text(model + "\n")
        if wwid is not None:
            (dev / "wwid").write_text(wwid)
        (dev / "delete").write_text("")  # empty placeholder; helper writes "1"

    orig_listdir = os.listdir
    orig_open = builtins.open

    def fake_listdir(path):
        if path == "/sys/block":
            return orig_listdir(str(tmp_path))
        return orig_listdir(path)

    def fake_open(path, *a, **kw):
        if isinstance(path, str) and path.startswith("/sys/block/"):
            path = path.replace("/sys/block/", str(tmp_path) + "/", 1)
        return orig_open(path, *a, **kw)

    monkeypatch.setattr(os, "listdir", fake_listdir)
    monkeypatch.setattr(builtins, "open", fake_open)
    return make_device


def _was_evicted(tmp_path_for_name):
    """Helper used after evict: check if the delete sysfs file got "1"."""
    return tmp_path_for_name.read_text() == "1"


def test_evict_skips_healthy_datacore_device(tmp_path, fake_sysblock):
    """The most important non-action: healthy LUNs (wwid populated) must
    NOT get their sd entry deleted."""
    fake_sysblock("sda", "DataCore", "Virtual Disk", "naa.healthywwid")
    datapath._evict_orphan_datacore_sds("dbg")
    assert not _was_evicted(tmp_path / "sda" / "device" / "delete")


def test_evict_orphan_with_empty_wwid_file(tmp_path, fake_sysblock):
    """The bug-trigger case: vendor matches, model matches, wwid file
    exists but is empty (kernel kept the sd entry without refreshing
    INQUIRY data)."""
    fake_sysblock("sdb", "DataCore", "Virtual Disk", "")
    datapath._evict_orphan_datacore_sds("dbg")
    assert _was_evicted(tmp_path / "sdb" / "device" / "delete")


def test_evict_orphan_with_missing_wwid_file(tmp_path, fake_sysblock):
    """Another orphan shape: the wwid file doesn't exist at all."""
    fake_sysblock("sdc", "DataCore", "Virtual Disk", None)
    datapath._evict_orphan_datacore_sds("dbg")
    assert _was_evicted(tmp_path / "sdc" / "device" / "delete")


def test_evict_orphan_with_whitespace_only_wwid(tmp_path, fake_sysblock):
    """rstrip() must reduce whitespace-only wwid to empty so it counts
    as an orphan."""
    fake_sysblock("sdd", "DataCore", "Virtual Disk", "   \n")
    datapath._evict_orphan_datacore_sds("dbg")
    assert _was_evicted(tmp_path / "sdd" / "device" / "delete")


def test_evict_skips_non_datacore_device(tmp_path, fake_sysblock):
    """Don't touch other vendors' sd entries, even if they're orphaned
    (their hosting SR's plugin owns them)."""
    fake_sysblock("sde", "OtherIscs", "Whatever", "")
    datapath._evict_orphan_datacore_sds("dbg")
    assert not _was_evicted(tmp_path / "sde" / "device" / "delete")


def test_evict_handles_padded_vendor_and_model(tmp_path, fake_sysblock):
    """Sysfs files often have trailing spaces/newlines (the kernel pads
    INQUIRY fields to fixed length). The matcher must rstrip before
    comparing."""
    fake_sysblock("sdf", "DataCore  ", "Virtual Disk  ", "naa.x")
    datapath._evict_orphan_datacore_sds("dbg")
    # wwid is populated -> not an orphan -> not evicted
    assert not _was_evicted(tmp_path / "sdf" / "device" / "delete")


def test_evict_skips_non_sd_entries(tmp_path, fake_sysblock):
    """A `loop0` or `dm-3` would never have DataCore vendor anyway, but
    the loop's `startswith("sd")` filter is the load-bearing guard."""
    fake_sysblock("loop0", "DataCore", "Virtual Disk", "")
    datapath._evict_orphan_datacore_sds("dbg")
    assert not _was_evicted(tmp_path / "loop0" / "device" / "delete")


def test_evict_picks_orphans_out_of_a_mixed_population(tmp_path, fake_sysblock):
    """End-to-end shape: a mixed population with healthy DataCore, orphan
    DataCore, and unrelated entries — only the orphan gets evicted."""
    fake_sysblock("sda", "DataCore",  "Virtual Disk", "naa.healthy")   # keep
    fake_sysblock("sdb", "DataCore",  "Virtual Disk", "")              # evict
    fake_sysblock("sdc", "DataCore",  "Virtual Disk", None)            # evict
    fake_sysblock("sdd", "OtherIscs", "Whatever",     "")              # keep
    datapath._evict_orphan_datacore_sds("dbg")
    assert not _was_evicted(tmp_path / "sda" / "device" / "delete")
    assert     _was_evicted(tmp_path / "sdb" / "device" / "delete")
    assert     _was_evicted(tmp_path / "sdc" / "device" / "delete")
    assert not _was_evicted(tmp_path / "sdd" / "device" / "delete")


# -------- _read_sysfs --------

def test_read_sysfs_returns_empty_on_missing_file(tmp_path):
    """_read_sysfs is used to probe vendor/model/wwid; if any file is
    missing it must return '' rather than raise, otherwise a single
    misconfigured sd would crash the eviction loop."""
    assert datapath._read_sysfs(str(tmp_path / "nonexistent")) == ""


def test_read_sysfs_strips_trailing_whitespace(tmp_path):
    f = tmp_path / "vendor"
    f.write_text("DataCore   \n")
    assert datapath._read_sysfs(str(f)) == "DataCore"
