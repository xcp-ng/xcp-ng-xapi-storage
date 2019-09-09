# Plugins to manage xapi storage datapaths

These plugins dictate how a storage volumes (local files, local block devices, iSCSI LUNs, Ceph RBD devices etc etc) should be mapped to Virtual Machines running on Xen.

The [xapi storage interface](https://xapi-project.github.io/xapi-storage) describes the concepts, features and APIs in more detail.

Datapath plugins are named by URI schemes. Internally we have the following low-level implementations. These should not be referenced directly by Volume plugins:

- `qdisk`: opens a file with `qemu-dp`.
- `tapdisk`: opens a file with `tapdisk` and then `tapdisk` serves the
  VM directly using the user-space grant-table and grant-mapping code.

# Writing rules to create extra symlinks for an SM plugin

`make` infers all commands a python file belonging to an SM plugin can be run with and creates the appropriate symlinks in the same directory. However, it might be the case that the plugin needs to provide a command, whose implementation is identical to another plugin's (and we don't want to duplicate the code).

This is possible by creating a `Rules.mk` file in the plugin directory of interest. The file should contain one `make` variable named `RULES`, which consists of `words` (in Makefile terminology). Each `word` is a `decorated rule`, which has the following form: `@<target_1>@...@<target_n>-><prerequisite>`

For example, assume there exists `datapath/loop+blkback/Rules.mk`, which contains the following text:
`RULES := \`
&emsp;&emsp;&emsp`@Foo.bar@Foo.baz->datapath/tapdisk/datapath.py \`
&emsp;&emsp;&emsp;`@Boo.biz@Boo.boz->datapath/tapdisk/plugin.py`

On build time, `@Foo.bar` and `@Foo.baz` (pointing to `../tapdisk/datapath.py`) and `@Boo.biz` and `@Boo.boz` (pointing to `../tapdisk/plugin.py`) will be created under `datapath/loop+blkback/` as extras to the plugin's implicit rules' commands.
