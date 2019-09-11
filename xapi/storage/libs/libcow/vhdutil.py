from xapi.storage.libs import image, tapdisk
from xapi.storage.libs.libcow.cowutil import COWUtil
from xapi.storage.libs.util import call
from xapi.storage import log

MEBIBYTE = 2**20
MSIZE_MIB = 2 * MEBIBYTE
OPT_LOG_ERR = '--debug'
MAX_CHAIN_HEIGHT = 30

VHD_UTIL_BIN = '/usr/bin/vhd-util'


class VHDUtil(COWUtil):

    @staticmethod
    def get_max_chain_height():
        return MAX_CHAIN_HEIGHT

    @staticmethod
    def __num_bits(val):
        count = 0
        while val:
            count += val & 1
            val = val >> 1
        return count

    @staticmethod
    def __count_bits(bitmap):
        count = 0
        for i in range(len(bitmap)):
            count += VHDUtil.__num_bits(ord(bitmap[i]))
        return count

    @staticmethod
    def is_empty(dbg, vol_path):
        cmd = [VHD_UTIL_BIN, 'read', OPT_LOG_ERR, '-B', '-n', vol_path]
        ret = call(dbg, cmd)
        return VHDUtil.__count_bits(ret) == 0

    @staticmethod
    def create(dbg, vol_path, size_mib):
        cmd = [
            VHD_UTIL_BIN, 'create',
            '-n', vol_path,
            '-s', str(size_mib),
            '-S', str(MSIZE_MIB)
        ]
        return call(dbg, cmd)

    @staticmethod
    def resize(dbg, vol_path, size_mib):
        cmd = [VHD_UTIL_BIN, 'resize', '-n', vol_path,
               '-s', str(size_mib), '-f']
        return call(dbg, cmd)

    @staticmethod
    def reset(dbg, vol_path):
        """Zeroes out the disk."""
        cmd = [VHD_UTIL_BIN, 'modify', OPT_LOG_ERR, '-z', '-n', vol_path]
        return call(dbg, cmd)

    @staticmethod
    def snapshot(dbg, new_cow_path, parent_cow_path, force_parent_link):
        """Perform COW snapshot.

        Args:
            new_cow_path: (str) Absolute path to the COW that will
                be created
            parent_cow_path: (str) Absolute path to the existing COW
                we wish to snapshot
            force_parent_link: (bool) If 'True', link new COW to
                the parent COW, even if the parent is empty
        """
        cmd = [
            VHD_UTIL_BIN, 'snapshot',
            '-n', new_cow_path,
            '-p', parent_cow_path
        ]

        if force_parent_link:
            cmd.append('-e')

        return call(dbg, cmd)

    @staticmethod
    def online_snapshot(dbg, new_cow_path, parent_cow_path, force_parent_link):
        return VHDUtil.snapshot(
            dbg, new_cow_path, parent_cow_path, force_parent_link)

    @staticmethod
    def offline_snapshot(
            dbg, new_cow_path, parent_cow_path, force_parent_link):
        return VHDUtil.snapshot(
            dbg, new_cow_path, parent_cow_path, force_parent_link)

    @staticmethod
    def coalesce(dbg, vol_path, parent_path):
        cmd = [VHD_UTIL_BIN, 'coalesce', '-n', vol_path]
        return call(dbg, cmd)

    @staticmethod
    def get_parent(dbg, vol_path):
        cmd = [VHD_UTIL_BIN, 'query', '-n', vol_path, '-p']
        return call(dbg, cmd).rstrip()

    @staticmethod
    def get_vsize(dbg, vol_path):
        # vsize is returned in MB but we want to return bytes
        cmd = [VHD_UTIL_BIN, 'query', '-n', vol_path, '-v']
        out = call(dbg, cmd).rstrip()
        return int(out) * MEBIBYTE

    @staticmethod
    def set_parent(dbg, vol_path, parent_path):
        cmd = [VHD_UTIL_BIN, 'modify', '-n', vol_path, '-p', parent_path]
        return call(dbg, cmd)

    @staticmethod
    def is_parent_pointing_to_path(dbg, vol_path, parent_path):
        stdout = VHDUtil.get_parent(dbg, vol_path)
        path = stdout.rstrip()
        log.debug("is_parent_pointing_to_path {} {}".format(parent_path, path))
        return parent_path[-12:] == path[-12:]

    @staticmethod
    def getImgFormat(dbg):
        return 'vhd'

    @staticmethod
    def refresh_datapath_clone(dbg, meta_path, new_path):
        tap = tapdisk.find_by_file(dbg, meta_path)
        tap.pause(dbg)
        tap.unpause(dbg, image.Cow(new_path))

    @staticmethod
    def refresh_datapath_coalesce(
            dbg, meta_path, coalesced_node, parent_node):
        tap = tapdisk.find_by_file(dbg, meta_path)
        tap.pause(dbg)
        tap.unpause(dbg)

    @staticmethod
    def pause_datapath(dbg, meta_path):
        tap = tapdisk.find_by_file(dbg, meta_path)
        tap.pause(dbg)

    @staticmethod
    def unpause_datapath(dbg, meta_path, new_path):
        tap = tapdisk.find_by_file(dbg, meta_path)
        tap.unpause(dbg, image.Cow(new_path))
