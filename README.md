# Plugins to manage xapi storage datapaths

These plugins dictate how a storage volumes (local files, local block devices, iSCSI LUNs, Ceph RBD devices etc etc) should be mapped to Virtual Machines running on Xen.

The [xapi storage interface](https://xapi-project.github.io/xapi-storage) describes the concepts, features and APIs in more detail.

Datapath plugins are named by URI schemes. Internally we have the following low-level implementations. These should not be referenced directly by Volume plugins:

- `qdisk`: opens a file with `qemu-dp`.
- `tapdisk`: opens a file with `tapdisk` and then `tapdisk` serves the
  VM directly using the user-space grant-table and grant-mapping code.

## Build & installation

Install dependencies: `make3`, `make`, `python-setuptools`, `nbd`, `python-psutil`, `qemu-dp`, `systemd` and `xapi-storage`.

Run these commands in the project directory:

```bash
mkdir build
cd build
cmake ..
make install
```

Do not forget to start qemuback service after installation:

```
systemctl start qemuback.service
```

## Issues and solutions

Important note: `xapi-storage-script` uses [inotify](https://en.wikipedia.org/wiki/Inotify) to monitor plugins, so never delete
`/usr/libexec/xapi-storage-script/datapath/` or `/usr/libexec/xapi-storage-script/volume/` folders!


> I can't create SRs or plugins are not found.

Ensure you have installed the same `xcp-ng-xapi-storage` package in all hosts of your pool.


> I can't start my VM with SMAPIv3 disks.

`xapi-storage-script` and `qemuback` services must be started to start correctly VMs.

You can check it with:

```
systemctl status xapi-storage-script.service
systemctl status qemuback.service
```
