"""Password cache: file under /run/datacore-sr/<sr>.pw with 0600 perms.

Wrong behavior here would either leak credentials (over-permissive file) or
re-open a XenAPI session on every plugin invocation (cache not hit). We've
seen both regressions during development, so they're worth pinning down."""

import os
import stat

import pytest

import datacoreapi


@pytest.fixture
def tmp_cache_dir(tmp_path, monkeypatch):
    """Redirect PASSWORD_CACHE_DIR to a per-test tmpdir so tests don't
    collide with each other or with /run/datacore-sr on a real host."""
    cache = tmp_path / "datacore-sr"
    monkeypatch.setattr(datacoreapi, "PASSWORD_CACHE_DIR", str(cache))
    return cache


def test_read_returns_none_when_cache_missing(tmp_cache_dir):
    """First call after SR.attach: no cache yet, must return None so the
    caller falls through to XenAPI."""
    assert datacoreapi._read_cached_password("sr-1") is None


def test_write_then_read_round_trip(tmp_cache_dir):
    datacoreapi._write_cached_password("sr-1", "hunter2")
    assert datacoreapi._read_cached_password("sr-1") == "hunter2"


def test_write_creates_directory_if_missing(tmp_cache_dir, tmp_path):
    """The cache dir doesn't necessarily exist before the first write."""
    assert not tmp_cache_dir.exists()
    datacoreapi._write_cached_password("sr-1", "x")
    assert tmp_cache_dir.exists()


def test_write_uses_0600_perms(tmp_cache_dir):
    """The whole point of the cache existing: it must not be world-readable.
    /run is tmpfs and root-only by default, but defense-in-depth."""
    datacoreapi._write_cached_password("sr-1", "secret")
    path = os.path.join(str(tmp_cache_dir), "sr-1.pw")
    mode = stat.S_IMODE(os.stat(path).st_mode)
    assert mode == 0o600, "expected 0o600, got " + oct(mode)


def test_write_is_atomic_no_temp_left_behind(tmp_cache_dir):
    """Confirm only the final .pw file remains — no .tmp / .partial left."""
    datacoreapi._write_cached_password("sr-1", "x")
    entries = sorted(os.listdir(str(tmp_cache_dir)))
    assert entries == ["sr-1.pw"], entries


def test_clear_removes_cache(tmp_cache_dir):
    datacoreapi._write_cached_password("sr-1", "x")
    assert datacoreapi._read_cached_password("sr-1") == "x"
    datacoreapi.clear_password_cache("sr-1")
    assert datacoreapi._read_cached_password("sr-1") is None


def test_clear_is_idempotent_on_missing_file(tmp_cache_dir):
    """SR.detach calls clear unconditionally — must not raise if there
    was never a cache file (e.g. attach failed before warming it)."""
    datacoreapi.clear_password_cache("sr-1")  # no file exists
    datacoreapi.clear_password_cache("sr-1")  # still no file
    # nothing raised


def test_write_failures_are_silent(monkeypatch, tmp_cache_dir):
    """Cache writes are best-effort — a disk-full / perm error must NOT
    break the calling plugin operation."""
    def _boom(*a, **kw):
        raise OSError("simulated disk full")
    monkeypatch.setattr(os, "open", _boom)
    # Should NOT raise:
    datacoreapi._write_cached_password("sr-1", "x")
    # And the cache stays empty:
    assert datacoreapi._read_cached_password("sr-1") is None


def test_resolve_password_plain_passthrough(tmp_cache_dir):
    """Direct (non-XAPI) test invocations pass `password` plaintext. The
    resolver must return it unchanged — no cache lookup, no XenAPI."""
    assert datacoreapi._resolve_password({"password": "p"}) == "p"


def test_resolve_password_cache_hit_skips_xenapi(tmp_cache_dir):
    """The key behavior: when the cache is warm, _resolve_password must
    NOT import or call XenAPI. We verify this by NOT stubbing XenAPI in
    sys.modules — a real call would ImportError on this dev box."""
    datacoreapi._write_cached_password("sr-warm", "cached-pw")
    cfg = {"password_secret": "uuid-doesnt-matter", "sr-uuid": "sr-warm"}
    assert datacoreapi._resolve_password(cfg) == "cached-pw"


def test_resolve_password_errors_when_no_credentials(tmp_cache_dir):
    """Defensive: SR config with neither password nor password_secret must
    surface a clear error, not e.g. AttributeError on a None lookup."""
    with pytest.raises(datacoreapi.DataCoreError) as exc_info:
        datacoreapi._resolve_password({})
    assert "No 'password'" in str(exc_info.value)
