import json
from xapi.storage.libs.libcow.cowutil import COWUtil
from xapi.storage.libs.util import call
from xapi.storage import log
import xapi.storage.libs.qemudisk as qemudisk

MEBIBYTE = 2**20
MSIZE_MIB = 2 * MEBIBYTE
OPT_LOG_ERR = '--debug'
MAX_CHAIN_HEIGHT = 30

QEMU_IMG = '/usr/lib64/qemu-dp/bin/qemu-img'
QEMU_IO = '/usr/lib64/qemu-dp/bin/qemu-io'

QCOW2_CLUSTER_SIZE = '2048k'


def __num_bits(val):
    count = 0
    while val:
        count += val & 1
        val = val >> 1
    return count


def __count_bits(bitmap):
    count = 0
    for i in range(len(bitmap)):
        count += __num_bits(ord(bitmap[i]))
    return count


class QCOW2Util(COWUtil):
    @staticmethod
    def get_max_chain_height():
        return MAX_CHAIN_HEIGHT

    @staticmethod
    def is_empty(dbg, vol_path):
        cmd = [
            QEMU_IO,
            '--cmd', 'open ' + vol_path,
            '--cmd', 'map'
        ]
        ret = call(dbg, cmd)
        lines = ret.splitlines()
        if len(lines) == 1:
            log.debug("Empty, {}".format(lines[0]))
            return "not allocated" in lines[0]
        else:
            log.debug("Not empty {}".format(lines))
            return False

    @staticmethod
    def create(dbg, vol_path, size_mib):
        cmd = [
            QEMU_IMG, 'create',
            '-f', 'qcow2',
            '-o', 'size={}M,cluster_size={}'
            .format(size_mib, QCOW2_CLUSTER_SIZE),
            vol_path
        ]
        return call(dbg, cmd)

    @staticmethod
    def create_snapshot(dbg, backing_file, vol_path):
        cmd = [
            QEMU_IMG, 'create',
            '-f', 'qcow2',
            '-o', 'backing_file={},cluster_size={}'
            .format(backing_file, QCOW2_CLUSTER_SIZE),
            vol_path
        ]
        return call(dbg, cmd)

    @staticmethod
    def resize(dbg, vol_path, size_mib):
        cmd = [
            QEMU_IMG, 'resize',
            vol_path,
            str(size_mib) + 'M'
        ]
        return call(dbg, cmd)

    @staticmethod
    def reset(dbg, vol_path):
        return

    @staticmethod
    def online_snapshot(dbg, new_cow_path, parent_cow_path, force_parent_link):
        """Perform QCOW2 snapshot.

        Args:
            new_cow_path: (str) Absolute path to the COW that will
                be created
            parent_cow_path: (str) Absolute path to the existing COW
                we wish to snapshot
            force_parent_link: (bool) If 'True', link new COW to
                the parent COW, even if the parent is empty
                (current only used for intellicache)
        """

        backing_file = parent_cow_path

        return QCOW2Util.create_snapshot(dbg, backing_file, new_cow_path)

    @staticmethod
    def offline_snapshot(
            dbg, new_cow_path, parent_cow_path, force_parent_link):
        """Perform QCOW2 snapshot.

        Args:
            new_cow_path: (str) Absolute path to the COW that will
                be created
            parent_cow_path: (str) Absolute path to the existing COW
                we wish to snapshot
            force_parent_link: (bool) If 'True', link new COW to
                the parent COW, even if the parent is empty
                (current only used for intellicache)
        """

        backing_file = parent_cow_path

        # Look if parent is empty
        if QCOW2Util.is_empty(dbg, parent_cow_path):
            parent_parent = QCOW2Util.get_parent(dbg, parent_cow_path)
            if parent_parent != "None":
                backing_file = parent_parent
        return QCOW2Util.create_snapshot(dbg, backing_file, new_cow_path)

    @staticmethod
    def coalesce(dbg, vol_path, parent_path):
        cmd = [QEMU_IMG, 'commit', '-q', '-t', 'none', vol_path,
               '-b', parent_path, '-d']
        return call(dbg, cmd)

    @staticmethod
    def get_parent(dbg, vol_path):
        cmd = [
            QEMU_IMG, 'info',
            '--output=json',
            vol_path
        ]
        ret = call(dbg, cmd)
        d = json.loads(ret)
        if "backing-filename" in d.keys():
            return d["backing-filename"]
        else:
            return "None"

    @staticmethod
    def get_vsize(dbg, vol_path):
        # vsize is returned in MB but we want to return bytes
        cmd = [QEMU_IMG, 'query', '-n', vol_path, '-v']
        out = call(dbg, cmd).rstrip()
        return int(out) * MEBIBYTE

    @staticmethod
    def set_parent(dbg, vol_path, parent_path):
        cmd = [QEMU_IMG, 'rebase', '-t', 'none', '-T', 'none',
               vol_path, '-b', parent_path, '-u']
        return call(dbg, cmd)

    @staticmethod
    def is_parent_pointing_to_path(dbg, vol_path, parent_path):
        path = QCOW2Util.get_parent(dbg, vol_path)
        log.debug("is_parent_pointing_to_path {} {}".format(parent_path, path))
        log.debug("p=%s pp=%s" % (vol_path, parent_path))
        return parent_path[-12:] == path[-12:]

    @staticmethod
    def getImgFormat(dbg):
        return 'qcow2'

    @staticmethod
    def refresh_datapath_clone(dbg, meta_path, new_path):
        qemu_be = qemudisk.load_qemudisk_metadata(dbg, meta_path)
        qemu_be.snap(dbg, new_path)
        qemudisk.save_qemudisk_metadata(dbg, meta_path, qemu_be)

    @staticmethod
    def refresh_datapath_coalesce(
            dbg, meta_path, coalesced_node, parent_node):
        qemu_be = qemudisk.load_qemudisk_metadata(dbg, meta_path)
        qemu_be.relink(dbg, coalesced_node, parent_node)
