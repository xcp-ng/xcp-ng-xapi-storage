import errno
import os
import psutil
import re
import signal
import subprocess
import time

import cPickle

# TODO: Get rid of 'image' module
# import image
from xapi.storage import log
from xapi.storage.libs import qmp
from xapi.storage.libs import util
from xapi.storage.libs.util import mkdir_p
from xapi.storage.libs.util import var_run_prefix
import xen.lowlevel.xs

QEMU_PROC_METADATA_FILE = "meta.pickle"

QEMU_DP = "/usr/lib64/qemu-dp/bin/qemu-dp"

# Use two cgroups like blktap:
# https://github.com/xapi-project/blktap/blob/1b337b56b74d6e2387cf1681db9e12c182eb4227/control/tap-ctl-spawn.c#L241-L253
QEMU_DP_CGROUP_BLKIO = "blkio:/vm.slice/"
QEMU_DP_CGROUP_CPU = "cpu,cpuacct:/"

NBD_CLIENT = "/usr/sbin/nbd-client"

IMAGE_TYPES = frozenset(['qcow2'])
LEAF_NODE_NAME = 'qemu_node'
NEW_LEAF_NODE_NAME = 'new_qemu_node'


class Qemudisk(object):
    def __init__(self, pid, qmp_sock, key, f):
        self.pid = pid
        self.qmp_sock = qmp_sock
        self.key = key
        self.f = f
        self.nbd_unix_sock = os.path.join(
            os.path.dirname(qmp_sock), 'qemu-nbd.{}'.format(key))

    def __repr__(self):
        return "Qemudisk(%d, %s, %s, %s)" % (self.pid,
                                             self.qmp_sock,
                                             self.key,
                                             self.f)

    def get_nbd_device_path(self, dbg, key):
        return "/dev/qemudisk/%s" % (key)

    def _qmp_connect(self, dbg):
        for i in range(1, 50):
            try:
                self.qmp = qmp.QEMUMonitorProtocol(self.qmp_sock)
                self.qmp.connect()
                break
            except:
                self.qmp = None
                time.sleep(0.1)
        if not self.qmp:
            raise Exception("Connection to QMP failed: {}".format(
                self.qmp_sock))

    def _qmp_disconnect(self, dbg):
        try:
            self.qmp.close()
        except:
            log.debug("{}: unable to close properly qmp connection".format(dbg))
        finally:
            self.qmp = None

    def _qmp_command(self, dbg, command, **args):
        log.debug("%s: sending QMP '%s' args %s" % (dbg, command, args))
        resp = self.qmp.command(command, **args)
        log.debug("%s: received QMP response '%s'" % (dbg, resp))
        return resp

    def _blockdev_add(self, dbg, file_path, node_name, is_snapshot=False):
        args = {"driver": "qcow2",
                "cache": {'direct': True, 'no-flush': True},
                'discard': 'unmap',
                "file": {'driver': 'file', 'aio': 'native',
                         'filename': file_path},
                "node-name": node_name}
        if is_snapshot:
            args['backing'] = None
        self._qmp_command(dbg, "blockdev-add", **args)

    def open(self, dbg, key, f):
        # FIXME: this would not work for raw support
        # assert isinstance(f, image.Cow)
        log.debug("%s: opening image %s in qemu with sock %s" %
                  (dbg, f, self.qmp_sock))
        self.f = f.path
        self._qmp_connect(dbg)
        # FIXME: we can not hardcode qcow2 here
        # args = {"driver": "raw",
        self._blockdev_add(dbg, self.f, LEAF_NODE_NAME)

        # Start an NBD server exposing this blockdev
        self._qmp_command(dbg, "nbd-server-start",
                          addr={'type': 'unix',
                                'data': {'path': self.nbd_unix_sock}})
        self._qmp_command(dbg, "nbd-server-add",
                          device=LEAF_NODE_NAME, writable=True)
        self._qmp_disconnect(dbg)

    def _kill_qemu(self):
        try:
            p = psutil.Process(self.pid)
            cmdline = p.cmdline()
            if cmdline[0] is QEMU_DP and cmdline[1] is self.qmp_sock:
                os.kill(self.pid, signal.SIGKILL)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    def close(self, dbg, key, f):
        # FIXME: this would not work for raw support
        # assert isinstance(f, image.Cow)
        log.debug("%s: closing image %s in qemu with sock %s" %
                  (dbg, f, self.qmp_sock))

        try:
            self._qmp_connect(dbg)
            path = "{}/{}".format(var_run_prefix(), key)
            try:
                with open(path, 'r') as f:
                    line = f.readline().strip()
                os.unlink(path)
                args = {
                    'type': 'qdisk',
                    'domid': int(re.search(r'domain/(\d+)/', line).group(1)),
                    'devid': int(re.search(r'vbd/(\d+)/', line).group(1))
                }
                self._qmp_command(dbg, "xen-unwatch-device", **args)
            except:
                log.debug('No VBD found')

            # Stop the NBD server
            self._qmp_command(dbg, "nbd-server-stop")

            # Remove the block device
            args = {"node-name": LEAF_NODE_NAME}
            self._qmp_command(dbg, "blockdev-del", **args)
            self._qmp_disconnect(dbg)
        except Exception as e:
            log.debug('{}: failed to close qemu: {}'.format(dbg, e))
            self._kill_qemu()

    def quit(self, dbg, key):
        # ask the qemu process to shutdown
        try:
            self._qmp_connect(dbg)
            self._qmp_command(dbg, "quit")
            self._qmp_disconnect(dbg)
        except:
            self._kill_qemu()

    def commit(self, dbg, node, parent):
        self._qmp_connect(dbg)
        args = {"job-id": "commit-{}".format(node),
                "device": LEAF_NODE_NAME,
                "top": node,
                "base": parent}
        self._qmp_command(dbg, 'block-commit', **args)
        for i in range(50):
            res = self._qmp_command(dbg, "query-block-jobs")
            if len(res) == 0:
                break
            time.sleep(0.1)
        self._qmp_disconnect(dbg)

    def relink(self, dbg, node, parent):
        self._qmp_connect(dbg)
        args = {"device": LEAF_NODE_NAME,
                "top": node,
                "base": parent}
        self._qmp_command(dbg, 'relink-chain', **args)
        self._qmp_disconnect(dbg)

    def snap(self, dbg, new_path):
        self._qmp_connect(dbg)
        self._blockdev_add(dbg, new_path, NEW_LEAF_NODE_NAME, True)
        args = {"node": LEAF_NODE_NAME,
                "overlay": NEW_LEAF_NODE_NAME}
        self._qmp_command(dbg, 'blockdev-snapshot', **args)
        self._qmp_disconnect(dbg)
        self.f = new_path


def find_qdisk_by_path(dbg, path):
    xs = xen.lowlevel.xs.xs()
    for frontend_domain_id in xs.ls('', '/local/domain/0/backend/qdisk'):
        for device_id in xs.ls('', '/local/domain/0/backend/qdisk/%s'
                               % (frontend_domain_id)):
            uri_param = xs.read(
                '', '/local/domain/0/backend/qdisk/%s/%s/params'
                % (frontend_domain_id, device_id))
            if uri_param == ("qcow2:" + path):
                qdisk = {}
                qdisk["frontend_domain_id"] = frontend_domain_id
                qdisk["device_id"] = device_id
                return qdisk
    return None


def create(dbg, key):
    socket_dir = os.path.join(var_run_prefix(), 'qemu-dp')
    mkdir_p(socket_dir, 0o0700)
    qmp_sock = os.path.join(socket_dir, 'qmp_sock.{}'.format(key))
    log.debug("spawning qemu process with qmp socket at %s" % (qmp_sock))
    cmd = [QEMU_DP, qmp_sock]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    util.set_cgroup(p.pid, QEMU_DP_CGROUP_BLKIO)
    util.set_cgroup(p.pid, QEMU_DP_CGROUP_CPU)
    log.debug("new qemu process has pid %d" % (p.pid))
    return Qemudisk(p.pid, qmp_sock, key, None)


# TODO lots of code duplicated from tapdisk.py below here

def _metadata_dir(path):
    return os.path.join(var_run_prefix(), "nonpersistent",
                        "dp-qemu", os.path.realpath(path).lstrip('/'))


def save_qemudisk_metadata(dbg, path, tap):
    """ Record the qemudisk metadata for this VDI in host-local storage """
    dirname = _metadata_dir(path)
    try:
        os.makedirs(dirname, mode=0o755)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    with open(dirname + "/" + QEMU_PROC_METADATA_FILE, "w") as fd:
        cPickle.dump(tap.__dict__, fd)


def load_qemudisk_metadata(dbg, path):
    """Recover the qemudisk metadata for this VDI from host-local
       storage."""
    dirname = _metadata_dir(path)
    log.debug("%s: load_qemudisk_metadata: trying '%s'" % (dbg, dirname))
    filename = dirname + "/" + QEMU_PROC_METADATA_FILE
    # No need to check for file existence;
    # if file not there, IOError is raised
    # if not(os.path.exists(filename)):
    #    raise Exception('volume doesn\'t exist')
    #    #raise xapi.storage.api.v5.volume.Volume_does_not_exist(dirname)
    with open(filename, "r") as fd:
        meta = cPickle.load(fd)
        qemu_be = Qemudisk(meta['pid'], meta['qmp_sock'],
                           meta['key'], meta['f'])

    return qemu_be
