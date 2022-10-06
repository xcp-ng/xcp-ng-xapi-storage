from xapi.storage.libs.util import call
from xapi.storage.libs.libcow.cowutil import COWUtil

ZFS_UTIL_BIN = 'zfs'

class ZFSUtil(COWUtil):

    @staticmethod
    def create(dbg, vol_path, size_mib):
        cmd = [
            ZFS_UTIL_BIN, 'create',
            '-V', str(size_mib),
            vol_path
        ]
        return call(dbg, cmd)

    @staticmethod
    def get_vsize(dbg, vol_path):
        cmd = [
            ZFS_UTIL_BIN, 'get',
            '-o', 'value', '-Hp', 'used,avail',
            vol_path
        ]
        return call(dbg, cmd).splitlines()

    @staticmethod
    def snapshot(dbg, snap_name, parent_path, force_parent_link):
        path = parent_path + '@' + snap_name
        cmd = [
            ZFS_UTIL_BIN, 'snapshot',
            path
        ]
        return call(dbg, cmd)

    @staticmethod
    def clone(dbg, snap_path, clone_path):
        cmd = [
            ZFS_UTIL_BIN, 'clone',
            snap_path, clone_path
        ]
        return call(dbg, cmd)

    @staticmethod
    def promote(dbg, clone_path):
        cmd = [
            ZFS_UTIL_BIN, 'promote',
            clone_path
        ]
        return call(dbg, cmd)

    @staticmethod
    def destroy(dbg, path):
        cmd = [
            ZFS_UTIL_BIN, 'destroy',
            path
        ]
        return call(dbg, cmd)
