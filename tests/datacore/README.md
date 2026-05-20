# DataCore plugin unit tests

Pure-Python unit tests for the SMAPIv3 DataCore plugin. Cover the logic
that runs inside `datacoreapi.py`, `sr.py`, `volume.py`, and `datapath.py`
without touching:

- a real DataCore array (every REST call is stubbed)
- a real XCP-ng dom0 (`xapi.storage.*` modules are stubbed in `conftest.py`)
- real iSCSI / multipath / sysfs (a fake `/sys/block` tree is built per test)

## Running

From the repo root:

```
pip install pytest      # if not already installed
pytest tests/datacore/
```

## What's covered

| File | Surface |
|---|---|
| `test_metadata.py` | `encode_metadata` / `parse_metadata` / `vdisk_to_vdi_info`, the 1024-char Description cap, `volume._reencode_metadata` add/delete/preserve behavior, `find_vdisk_by_vdi_uuid` prefix filtering |
| `test_pool_and_host.py` | `pick_snapshot_pool` (non-preferred-side placement), `find_host_id_by_iqn` (IQN-to-host resolution at SR.attach) |
| `test_capacity.py` | `pool_capacity_bytes` and `sr_allocated_bytes` (SR.stat numbers) — including malformed Size handling and endpoint-failure safety |
| `test_client_ops.py` | `_build_retry` adapter config, `HTTP_TIMEOUT` constant, `serve_vdisk` idempotency on "already exists", `wait_for_vdisk_online` state machine |
| `test_password_cache.py` | Password-cache file under `/run/datacore-sr/`, 0600 perms, atomic write, idempotent clear, `_resolve_password` cache-hit path skipping XenAPI |
| `test_datapath.py` | `_parse_uri`, `_evict_orphan_datacore_sds` (the orphan-sd matrix), `_read_sysfs` defensive handling |

## What's **not** covered

These need integration testing against a real DataCore + XCP-ng pair:

- The actual REST round-trips (`POST /virtualdisks`, `/snapshots`, etc.)
- iSCSI session setup, `multipath -a`, `/dev/mapper/...` materialisation
- XAPI ↔ plugin interaction (xe sr-create, vdi-snapshot, vbd-plug, …)

The lab-side validation history for those lives in commit messages and
the plugin README.
