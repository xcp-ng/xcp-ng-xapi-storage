# DataCore SANsymphony SMAPIv3 plugin

A SMAPIv3 storage plugin for XCP-ng that maps each VDI 1:1 to a DataCore
SANsymphony virtual disk via the SANsymphony REST API. No carved LUNs,
no LVM, no VHD. Each VDI is an array-native vDisk; snapshots and clones
use DataCore's native differential snapshot facility.

## Supported topology

This plugin targets one specific SANsymphony deployment shape:

- **SANsymphony 2-node HA, synchronous mirror** — every vDisk is mirrored
  across one pool on each of two DataCore servers. Tested against
  SANsymphony 10.0 PSP 20.

Not currently supported:

- **Single-node SANsymphony** (no mirror) — `Volume.create` requires a
  second pool today and posts `Type=2` mirrored vDisks. A future revision
  could make `second-pool` optional.
- **Stretched cluster** (geographically separated 2-node) — likely works
  as-is since the REST API and mirror semantics are identical, but not
  tested by us.
- **N-node Server Groups** (>2 servers) — each vDisk is still mirrored
  across a chosen pair of pools, so the plugin works once you pick a
  pair at `SR.create` time. There is no dynamic pair selection.
- **Other DataCore products** — Bolt (object), Puls8 (CSI), vFilO (file)
  have entirely different APIs and are out of scope.

## Requirements

- XCP-ng 8.3+ (Dom0 Python 3.6 baseline)
- `python3-requests` available in Dom0
- Network reachability from Dom0 to:
  - DataCore REST endpoint (HTTPS, port 443 by default)
  - All DataCore iSCSI portals (TCP 3260)
- A DataCore admin Windows account whose credentials this plugin uses
  to call the REST API. Passed via `device-config:username` + `password`
  (the latter is auto-stored as an XAPI secret).

## Installation

```
make install        # drops plugin files + udev rule + multipath snippet
multipathd reconfigure
udevadm control --reload
```

Files installed:

| Path | Purpose |
|---|---|
| `/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.datacore/` | Volume plugin (plugin.py, sr.py, volume.py, datacoreapi.py + SR.*/Volume.*/Plugin.* symlinks) |
| `/usr/libexec/xapi-storage-script/datapath/datacore-iscsi/` | Datapath plugin |
| `/etc/multipath/conf.d/datacore-vates.conf` | DataCore-tuned dm-multipath device profile |
| `/etc/udev/rules.d/99-datacore-vates.rules` | Pins `noop` scheduler on DataCore sd's and dm devices |

`multipathd reconfigure` is required for the multipath snippet to take
effect without rebooting. A future revision will add a post-install hook;
for now do it manually after the first install.

## First-time SR setup

You need to know a few DataCore-side identifiers before `SR.create`:

- **REST endpoint** — `https://<datacore-server>` (any one server of the
  HA pair).
- **first-pool / second-pool** — pool IDs in `{ServerId}:{poolGUID}`
  format, one per DataCore server. Find via `GET /pools` or the
  SANsymphony GUI.
- **iscsi-portals** — comma-separated DataCore Front-End port IPs.
- **host-id** *(optional after first attach)* — the DataCore host
  object ID for this XCP-ng host. Required for the first SR.attach
  ever; subsequent attaches resolve via initiator-IQN lookup
  (`GET /ports`) and the key can be removed.

```
xe sr-create type=datacore name-label="my-datacore" shared=false \
    device-config:rest-endpoint=https://192.168.1.87 \
    device-config:username=Administrateur \
    device-config:password=<plaintext> \
    device-config:first-pool="<ServerA-Id>:{<poolA-guid>}" \
    device-config:second-pool="<ServerB-Id>:{<poolB-guid>}" \
    device-config:iscsi-portals=192.168.1.87,192.168.1.88 \
    device-config:host-id=<datacore-host-object-id>
```

XAPI silently converts the `password` key to a `password_secret` UUID
behind the scenes; the plaintext is never persisted in `device-config`.

If the DataCore host object doesn't yet exist for this XCP-ng host,
create it in the SANsymphony GUI first (Hosts → Add Host). The plugin
will register the XCP-ng initiator IQN against it on the first
`SR.attach` via `RegisterPort`.

After the first successful attach the `host-id` device-config key can
be removed — `SR.attach` resolves it automatically from the IQN.

## Known limitations

- **Snapshot HA gap.** DataCore snapshots live on a single server pool
  ("The snapshot can only exist on one server" — Snapshot Operations
  docs). If the server pool holding the snapshot dies, the snapshot is
  lost; the source vDisk's mirrored data is still safe. To balance load
  the plugin places each snapshot on the *non-preferred* side of the
  source's mirror (DataCore best practice). Real HA for snapshots
  requires DataCore's async replication, not addressed in-plugin.
- **Online VDI resize is gated by XAPI, not the plugin.** `xe vdi-resize`
  on a plugged VDI is refused at the XAPI layer even though the plugin
  advertises `VDI_RESIZE_ONLINE` — XCP-ng's `lvmoiscsi` has the same gate.
  Workaround: vbd-unplug → vdi-resize → vbd-plug.
- **`free_space` is SR-centric.** If the same DataCore pools are shared
  with another XCP-ng SR or a non-XCP tenant, the plugin only counts
  its own vDisks (Alias-prefix match) and reports an optimistic
  `free_space`. `total_space` (sum of `/poolmembers`) is correct.
- **`Volume.create` blocks ~2–15 s on mirror sync.** Freshly created
  mirrored vDisks report `DiskStatus=1` while DataCore syncs legs;
  the plugin polls until `DiskStatus=0` before returning so an
  immediately-following snapshot or attach doesn't race. Bounded
  timeout: 60 s.

## Troubleshooting

- **Storage script log**: `journalctl -u xapi-storage-script` or
  `grep datacore /var/log/daemon.log`. Each plugin invocation logs
  the JSON-RPC method and its result.
- **XAPI plugin sessions**: `grep "originator=datacore-plugin"
  /var/log/xensource.log` should show one new session per `SR.attach`
  (cache warmup) and zero per subsequent `Volume.*` call (cache hit).
- **Orphan sd entries**: if a VDI plug times out at 30 s in
  `Datapath.attach`, look for DataCore sd's with empty `wwid`:
  `for sd in /sys/block/sd*; do
    [ "$(cat $sd/device/vendor 2>/dev/null | tr -d ' ')" = "DataCore" ] || continue;
    echo "$sd wwid=$(cat $sd/device/wwid 2>/dev/null)";
   done`.
  The plugin evicts these automatically at the start of each
  `Datapath.attach`, but if you hit one the manual recovery is
  `echo 1 > /sys/block/sdX/device/delete` for each orphan.
- **`Cannot perform snapshot operations ... while it is not up-to-date`**:
  shouldn't happen anymore since `Volume.create` waits for mirror sync,
  but if it does it means the source's `DiskStatus` is 1. Inspect via
  `GET /virtualdisks/<id>`.
